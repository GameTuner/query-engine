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

import os
from typing import Generator

from queryengine.core.app.datasource import CachedMetadataAppRepository, MetadataAppRepository
from queryengine.core.bigquery.queryexecutor import SimpleBigQueryExecutor, CancellableBigQueryExecutor
from queryengine.core.datasource.repository import InMemoryDataSourceRepository
from queryengine.core.kpi.repository import InMemoryKpiRepository
from queryengine.core import constants
from queryengine.core.user_history_definition.repository import InMemoryUserHistoryDefinitionRepository, CachedUserHistoryDefinitionRepository

_app_config_repository = CachedMetadataAppRepository(MetadataAppRepository(
    metadata_ip_address=os.environ["METADATA_IP_ADDRESS"],
    metadata_port=os.environ.get("METADATA_PORT", "80")
))

_bigquery_executor = CancellableBigQueryExecutor(SimpleBigQueryExecutor(
    project=os.environ.get('GCP_PROJECT_ID'),
    threads=100,
    max_rows=constants.BIGQUERY_MAX_ROWS
))


def bigquery_executor() -> Generator:
    yield _bigquery_executor


def app_config_repository() -> Generator:
    yield _app_config_repository


def user_history_definition_repository() -> Generator:
    yield CachedUserHistoryDefinitionRepository(next(app_config_repository()))


def datasource_repository() -> Generator:
    yield InMemoryDataSourceRepository(next(user_history_definition_repository()))


def kpi_repository() -> Generator:
    yield InMemoryKpiRepository(next(datasource_repository()))
