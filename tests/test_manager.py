from typing import List, Union
import pytest

from metricq_manager.manager import Manager, Metric, MetricInputAlias


@pytest.mark.parametrize(
    ("metrics", "data_bindings", "history_bindings"),
    [
        # An empty list if bindings does not yield either data or history bindings
        ([], [], []),
        # A list of strings translates into the same data/history bindings
        (["foo", "bar"], ["foo", "bar"], ["foo", "bar"]),
        # Data bindings can assign input aliases in object form
        ([{"input": "foo", "name": "bar"}], ["foo"], ["bar"]),
        # Fall back to "name" for data bindings
        ([{"name": "bar"}], ["bar"], ["bar"]),
        # Mixed bindings of strings and objects are allowed
        (["foo", {"name": "bar"}], ["foo", "bar"], ["foo", "bar"]),
    ],
)
def test_parse_db_bindings(
    metrics: List[Union[Metric, MetricInputAlias]],
    data_bindings: List[Metric],
    history_bindings: List[Metric],
):
    assert Manager.parse_db_bindings(bindings=metrics) == (
        data_bindings,
        history_bindings,
    )
