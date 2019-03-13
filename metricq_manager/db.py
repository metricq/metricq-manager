from abc import ABC, abstractmethod
import os
import json
import time
import datetime
from typing import List, Dict, Sequence

import cloudant

from metricq.logging import get_logger

logger = get_logger()


class Db(ABC):

    @abstractmethod
    async def fetch_config(self, token: str) -> dict:
        pass

    @abstractmethod
    async def fetch_metadata(self, metric_ids: Sequence[str]) -> Dict[str, dict]:
        pass

    @abstractmethod
    async def update_metadata(self, metrics: Dict[str, dict]) -> None:
        pass


class Couchdb(Db):
    def __init__(self, couchdb_url, couchdb_user, couchdb_password):
        self.couchdb_client = cloudant.client.CouchDB(couchdb_user, couchdb_password,
                                                      url=couchdb_url, connect=True)
        self.couchdb_session = self.couchdb_client.session()
        self.couchdb_db_config = self.couchdb_client.create_database("config")
        self.couchdb_db_metadata = self.couchdb_client.create_database("metadata")
        logger.info('connected to couchdb at {} for config and metadata',
                    couchdb_url)

    async def fetch_config(self, token: str) -> dict:
        config_document = self.couchdb_db_config[token]
        config_document.fetch()
        return dict(config_document)

    async def fetch_metadata(self, metric_ids: Sequence[str]) -> Dict[str, dict]:
        """ This is async in case we ever make asynchronous couchdb requests """
        metadata = dict()
        for metric in metric_ids:
            try:
                metadata[metric] = self.couchdb_db_metadata[metric]
            except KeyError:
                metadata[metric] = {'error': 'no metadata provided for {}'.format(metric)}
        return metadata

    async def update_metadata(self, metrics: Dict[str, dict]) -> None:
        metrics_new = 0
        metrics_updated = 0

        def update_doc(row):
            nonlocal metrics_new, metrics_updated, metrics
            metric = row['key']
            document = metrics[metric]

            for key in list(document.keys()):
                if key.startswith('_'):
                    del document[key]

            document['_id'] = metric
            if 'date' not in document:
                document['date'] = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
            if 'id' in row:
                try:
                    metrics_updated += 1
                    if metric != row['id']:
                        logger.error('inconsistent key/id while updating metadata {} != {}', metric, row['id'])
                    if 'deleted' not in row['value']:
                        document['_rev'] = row['value']['rev']
                except KeyError:
                    logger.error('something went wrong trying to update existing metadata document {}', metric)
                    raise
            else:
                metrics_new += 1
            return document

        start = time.time()
        docs = self.couchdb_db_metadata.all_docs(keys=list(metrics.keys()))
        new_docs = [update_doc(row) for row in docs['rows']]
        status = self.couchdb_db_metadata.bulk_docs(new_docs)
        end = time.time()
        if len(status) != len(metrics):
            logger.error('metadata update mismatch in metrics count expected {}, actual {}', len(metrics), len(status))
        error = False
        for s in status:
            if 'error' in s:
                error = True
                logger.error('error updating metadata {}', s)
        logger.info('metadata update took {:.3f} s for {} new and {} existing metrics',
                    end-start, metrics_new, metrics_updated)
        if error:
            raise RuntimeError('metadata update failed')


class Filedb(Db):
    def __init__(self, config_path):
        self.config_path = config_path

    async def fetch_config(self, token: str) -> dict:
        with open(os.path.join(self.config_path, token + ".json"), 'r') as f:
            return json.load(f)

    async def fetch_metadata(self, metric_ids: Sequence[str]) -> Dict[str, dict]:
        return {}

    async def update_metadata(self, metrics: Dict[str, dict]) -> None:
        pass


def make_db(config_path, couchdb_url, couchdb_user, couchdb_password):
    if couchdb_user and couchdb_url:
        if config_path:
            logger.warning('ignoring config-path in favor of couchdb')
        return Couchdb(couchdb_url, couchdb_user, couchdb_password)
    elif config_path:
        logger.warning('no couchdb url or user set, falling back to file-only interface from {}',
                       config_path)
        return Filedb(config_path)
    else:
        raise RuntimeError("must specify either --couchdb settings or --config-path")
