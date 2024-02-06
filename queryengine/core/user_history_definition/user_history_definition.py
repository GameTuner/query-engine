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

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List

from queryengine.core.app.app import MaterializedColumns
from queryengine.core.datasource.datasource import Column, DataType


@dataclass
class RegistrationColumn:
    column_definition: Column
    registration_table_column: str

    @classmethod
    def from_column(cls, column_definition: Column, registration_table_column: str | None = None) -> 'RegistrationColumn':
        return RegistrationColumn(
            column_definition=column_definition,
            registration_table_column=registration_table_column or column_definition.id)
    
    @classmethod
    def from_materialized_column(cls, column_definition: MaterializedColumns, is_total: bool = False) -> 'RegistrationColumn':
        column_name = column_definition.column_name if not is_total else f'{column_definition.column_name}_total'
        return RegistrationColumn(
            column_definition=
                Column(
                    id=column_name,
                    data_type=DataType.parse(column_definition.data_type),
                    can_filter=column_definition.can_filter,
                    can_group_by=column_definition.can_group_by,
                    hidden=column_definition.hidden
                ),
            registration_table_column=column_name)
    

    @property
    def name(self):
        return self.column_definition.id

    def registration_table_dataset_name(self, app_id: str) -> str:
        return f'{app_id}_main'


@dataclass
class ExternalTableColumn:
    dataset_name: str
    table_name: str
    table_filter_formula: str | None
    table_aggregation_formula: str
    user_history_formula: str | None
    materialized_from: date | None
    column_definition: Column

    @property
    def name(self):
        return self.column_definition.id

    def __post_init__(self):
        if not self.user_history_formula:
            self.user_history_formula = f"{{{self.name}}}"


@dataclass
class TotalColumn:
    source_column: str
    time_window: str
    formula: str
    column_definition: Column

    @property
    def name(self):
        return self.column_definition.id


@dataclass
class ComputedColumn:
    formula: str
    column_definition: Column

    @property
    def name(self):
        return self.column_definition.id


@dataclass
class UserHistoryDefinition:
    registration_columns: Dict[str, RegistrationColumn] = field(default_factory=dict)
    external_table_columns: Dict[str, ExternalTableColumn] = field(default_factory=dict)
    total_columns: Dict[str, TotalColumn] = field(default_factory=dict)
    computed_columns: Dict[str, ComputedColumn] = field(default_factory=dict)

    def get_columns(self) -> List[Column]:
        columns = []
        columns.extend(c.column_definition for c in self.registration_columns.values())
        columns.extend(c.column_definition for c in self.external_table_columns.values())
        columns.extend(c.column_definition for c in self.total_columns.values())
        columns.extend(c.column_definition for c in self.computed_columns.values())
        return columns

    def merge(self, user_history_definition: 'UserHistoryDefinition') -> 'UserHistoryDefinition':
        return UserHistoryDefinition(
            registration_columns=self.registration_columns | user_history_definition.registration_columns,
            external_table_columns=self.external_table_columns | user_history_definition.external_table_columns,
            total_columns=self.total_columns | user_history_definition.total_columns,
            computed_columns=self.computed_columns | user_history_definition.computed_columns,
        )

