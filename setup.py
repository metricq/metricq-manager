# metricq
# Copyright (C) 2018 ZIH, Technische Universitaet Dresden, Federal Republic of Germany
#
# All rights reserved.
#
# This file is part of metricq.
#
# metricq is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# metricq is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with metricq.  If not, see <http://www.gnu.org/licenses/>.
from setuptools import setup

setup(name='metricq_manager',
      version='0.1',
      author='TU Dresden',
      python_requires=">=3.5",
      packages=['metricq_manager'],
      scripts=[],
      entry_points='''
      [console_scripts]
      metricq-manager=metricq_manager:manager_cmd
      ''',
      install_requires=['aio-pika', 'aiomonitor', 'click', 'click-completion', 'click_log', 'colorama', 'metricq', 'cloudant', 'yarl'])
