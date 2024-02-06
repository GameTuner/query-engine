# Copyright (c) 2024 AlgebraAI All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

from queryengine.api.chart.internal.warehouse.warehouse import Warehouse
from tests.api.chart.internal.warehouse.warehouse_mock import TestWarehouse, MissingDataTestWarehouse, \
    QueuedDataTestWarehouse


@pytest.fixture(scope='session')
def warehouse() -> Warehouse:
    return TestWarehouse()


@pytest.fixture(scope='session')
def missing_data_warehouse() -> Warehouse:
    return MissingDataTestWarehouse()


@pytest.fixture(scope='session')
def queued_data_warehouse() -> QueuedDataTestWarehouse:
    return QueuedDataTestWarehouse()
