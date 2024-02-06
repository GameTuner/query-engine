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

from abc import ABC, abstractmethod
from collections import ChainMap
from typing import Dict, List

from queryengine.core.app.app import App
from queryengine.core.datasource.datasource import DailyDataSource, DataSource, ColumnReference, Column
from queryengine.core.datasource.datasource_definitions import common
from queryengine.core.datasource.datasources import EventDataSource, UserHistoryDataSource, AppsflyerDataSource
from queryengine.core.user_history_definition.repository import UserHistoryDefinitionRepository


class DataSourceRepository(ABC):
    def all_event_data_sources(self, app: App) -> Dict[str, EventDataSource]:

        datasources = [EventDataSource.from_event_schema(app, schema)
                       for schema in app.app_config.event_schemas.values()]
        if app.app_config.use_context_schemas:
            datasources.extend([EventDataSource.from_event_schema(app, schema)
                                for schema in app.common_configs.non_embedded_context_schemas.values()])
            
        return {datasource.id: datasource for datasource in datasources}

    @abstractmethod
    def all_daily_data_sources(self, app: App) -> Dict[str, DailyDataSource]:
        pass

    def load_datasource_by_id(self, app: App, datasource_id: str) -> DataSource:
        return dict(ChainMap(
            self.all_event_data_sources(app),
            self.all_daily_data_sources(app))
        )[datasource_id]

    def load_column_by_full_id(self, app: App, column_full_id: str) -> ColumnReference:
        datasource_id, column_id = column_full_id.split(".", 1)
        datasource = self.load_datasource_by_id(app, datasource_id)
        return ColumnReference(datasource, column_id)


class InMemoryDataSourceRepository(DataSourceRepository):
    def __init__(self, user_history_definition_repository: UserHistoryDefinitionRepository):
        self._user_history_definition_repository = user_history_definition_repository

    def all_daily_data_sources(self, app: App) -> Dict[str, DailyDataSource]:
        datasources = [
            UserHistoryDataSource(
                app=app,
                user_history_definition=self._user_history_definition_repository.load_by_app(app))
        ]
        # TODO temporarly disable appsflyer
        if 'appsflyer' in app.app_config.datasources:
            datasources.append(AppsflyerDataSource(app=app, columns=common.APPSFLYER_COLUMNS))
        return {datasource.id: datasource for datasource in datasources}
