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

import datetime

from queryengine.core.app.app import App
from queryengine.core.datasource.datasource import Column, DataType
from queryengine.core.user_history_definition.repository import UserHistoryDefinitionRepository
from queryengine.core.user_history_definition.user_history_definition import UserHistoryDefinition, RegistrationColumn, \
    ExternalTableColumn, ComputedColumn


class TestUserHistoryDefinitionRepository(UserHistoryDefinitionRepository):
    def load_by_app(self, app: App) -> UserHistoryDefinition:
        return UserHistoryDefinition(
            registration_columns={
                'date_': RegistrationColumn.from_column(Column('date_', DataType.date)),
                'unique_id': RegistrationColumn.from_column(Column('unique_id', DataType.string)),
                'up_int': RegistrationColumn.from_column(Column('up_int', DataType.integer)),
                'up_string': RegistrationColumn.from_column(Column('up_string', DataType.string)),
                'up_date': RegistrationColumn.from_column(Column('up_date', DataType.date)),
                'cohort_day': RegistrationColumn.from_column(Column('cohort_day', DataType.integer)),
                'registration_date': RegistrationColumn.from_column(Column('registration_date', DataType.date)),
            },
            external_table_columns={
                'ext_int': ExternalTableColumn(
                    dataset_name=f'{app.app_id()}_raw',
                    table_name='purchase',
                    table_filter_formula=None,
                    table_aggregation_formula='SUM({purchase})',
                    user_history_formula=None,
                    materialized_from=None,
                    column_definition=Column('ext_int', DataType.integer),
                ),
                'ext_int_materialized': ExternalTableColumn(
                    dataset_name=f'{app.app_id()}_raw',
                    table_name='purchase',
                    table_filter_formula="{status} = 'SUCCESS'",
                    table_aggregation_formula='SUM({purchase})',
                    user_history_formula='COALESCE({ext_int_materialized}, 0)',
                    materialized_from=datetime.datetime(2022, 6, 1).date(),
                    column_definition=Column('ext_int_materialized', DataType.integer),
                ),
                'ext_int_materialized_same_filter_different_formula': ExternalTableColumn(
                    dataset_name=f'{app.app_id()}_raw',
                    table_name='purchase',
                    table_filter_formula="{status} = 'SUCCESS'",
                    table_aggregation_formula='SUM({purchase_usd})',
                    user_history_formula='COALESCE({ext_int_materialized_same_filter_different_formula}, 0.0)',
                    materialized_from=datetime.datetime(2022, 6, 1).date(),
                    column_definition=Column('ext_int_materialized_same_filter_different_formula', DataType.integer),
                )
            },
            computed_columns={
                'comp_registration': ComputedColumn(
                    formula='{date_}',
                    column_definition=Column('comp_registration', DataType.date),
                ),
                'comp_two_externals': ComputedColumn(
                    formula='{ext_int_materialized} + {ext_int}',
                    column_definition=Column('comp_registration', DataType.date),
                ),
                'comp_computed': ComputedColumn(
                    formula='{comp_two_externals} + 1',
                    column_definition=Column('comp_registration', DataType.date),
                ),
            }
        )