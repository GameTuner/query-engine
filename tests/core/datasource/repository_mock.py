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

from typing import Dict

from queryengine.core.app.app import App
from queryengine.core.datasource.datasource import DailyDataSource
from queryengine.core.datasource.datasources import UserHistoryDataSource
from queryengine.core.datasource.repository import DataSourceRepository
from queryengine.core.user_history_definition.repository import UserHistoryDefinitionRepository


class TestDataSourceRepository(DataSourceRepository):
    def __init__(self, user_history_definition_repository: UserHistoryDefinitionRepository):
        self._user_history_definition_repository = user_history_definition_repository

    def all_daily_data_sources(self, app: App) -> Dict[str, DailyDataSource]:
        datasources = [
            UserHistoryDataSource(
                app=app,
                user_history_definition=self._user_history_definition_repository.load_by_app(app))
        ]
        return {datasource.id: datasource for datasource in datasources}
