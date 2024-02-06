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

from queryengine.core.datasource.repository import DataSourceRepository
from queryengine.core.kpi.repository import KpiRepository
from queryengine.core.user_history_definition.repository import UserHistoryDefinitionRepository
from tests.core.app.app_config_provider_mock import TestAppRepository
from tests.core.datasource.repository_mock import TestDataSourceRepository
from tests.core.kpi.repository_mock import TestKpiRepository
from tests.core.user_history_definition.repository_mock import TestUserHistoryDefinitionRepository


@pytest.fixture(scope='session')
def user_history_definition_repository() -> UserHistoryDefinitionRepository:
    return TestUserHistoryDefinitionRepository()


@pytest.fixture(scope='session')
def datasource_repository(user_history_definition_repository) -> DataSourceRepository:
    return TestDataSourceRepository(user_history_definition_repository)


@pytest.fixture(scope='session')
def kpi_repository(datasource_repository) -> KpiRepository:
    return TestKpiRepository(datasource_repository)


@pytest.fixture(scope='session')
def app_config_repository() -> TestAppRepository:
    return TestAppRepository()
