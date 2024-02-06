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
from typing import List

from queryengine.core.app.app import App, MaterializedColumns
from queryengine.core.app.datasource import AppRepository
from queryengine.core.datasource.datasource import Column
from queryengine.core.user_history_definition.user_history_definition import RegistrationColumn, UserHistoryDefinition
from queryengine.core.user_history_definition.user_history_definitions import demoapp, common
from queryengine.core.datasource.datasource import DataType


class UserHistoryDefinitionRepository(ABC):
    @abstractmethod
    def load_by_app(self, app: App) -> UserHistoryDefinition:
        pass

    def build_definition_from_materialized_columns(self, materialized_columns: List[MaterializedColumns]) -> UserHistoryDefinition:
        # TODO - define materialized columns as external table columns, not registration columns
        non_total_columns = {column.column_name: RegistrationColumn.from_materialized_column(column) for column in materialized_columns}
        total_columns = {f"{column.column_name}_total": RegistrationColumn.from_materialized_column(column, True) for column in materialized_columns if column.totals}
        return UserHistoryDefinition(
            registration_columns=non_total_columns | total_columns,
            external_table_columns={},
            total_columns={},
            computed_columns={},
        )

class InMemoryUserHistoryDefinitionRepository(UserHistoryDefinitionRepository):
    def _game_specific_user_history_definition(self, app_id: str) -> UserHistoryDefinition:
        return {
            'demoapp': demoapp.USER_HISTORY_DEFINITION,
        }.get(app_id, UserHistoryDefinition())

    def load_by_app(self, app: App) -> UserHistoryDefinition:
        return common.USER_HISTORY_DEFINITION.merge(self._game_specific_user_history_definition(app.app_id()))
    
class CachedUserHistoryDefinitionRepository(UserHistoryDefinitionRepository):
    def __init__(self, app_repository: AppRepository):
        self.app_repository = app_repository
        self._user_history_definitions = {}
        super().__init__()

    def load_by_app(self, app: App) -> UserHistoryDefinition:
        app = self.app_repository.from_app_id(app.app_id())
        materialized_columns = app.app_config.datasources['user_history'].materialized_columns
        materialized_columns_definition = super().build_definition_from_materialized_columns(materialized_columns)
        return common.USER_HISTORY_DEFINITION.merge(materialized_columns_definition)
