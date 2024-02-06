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

from queryengine.core.datasource.datasource import Column, DataType
from queryengine.core.user_history_definition.user_history_definition import RegistrationColumn, UserHistoryDefinition

# Define registrations columns here
USER_HISTORY_REGISTRATION_COLUMNS = [
    # RegistrationColumn.from_column(Column('column_name', DataType.number, can_group_by=False)),
]
USER_HISTORY_DEFINITION = UserHistoryDefinition(
    registration_columns={c.registration_table_column: c for c in USER_HISTORY_REGISTRATION_COLUMNS},
    external_table_columns={},
    total_columns={},
    computed_columns={},
)