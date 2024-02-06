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

from queryengine.core import constants
from queryengine.core.bigquery.sql.boolean_expression import BooleanExpression
from queryengine.core.bigquery.sql.expression import Expression
from queryengine.core.bigquery.sql.join import JoinType, Join
from queryengine.core.bigquery.sql.sql_builder import QueryBuilder, SelectStatement
from queryengine.core.bigquery.sql.table_like import Table, Cte, TableLike
from queryengine.core.user_history_definition.user_history_definition import RegistrationColumn


def get_column_for_insert_query(app_id: str, registration_column: RegistrationColumn, sql_builder: QueryBuilder,
                                select_statement: SelectStatement) -> Expression:

    formula_column = _formula_column(registration_column)
    user_history_table = select_statement.get_table()

    cte = _build_cte(app_id, registration_column)
    if cte.cte_name in sql_builder.cte_map:
        cte = sql_builder.cte_map[cte.cte_name]
        if not isinstance(cte.select, SelectStatement):
            raise Exception(f'Cte {cte.cte_name} not compatible!')
        cte.select.get_select().expressions.append(formula_column)
    else:
        _register_and_join_cte(cte, select_statement, sql_builder, user_history_table)

    return cte.column(registration_column.name)


def _formula_column(app_id: str, registration_column: RegistrationColumn):
    external_table = Table(
        registration_column.registration_table_dataset_name(app_id),
        registration_column.registration_table_column)
    return external_table.column(registration_column.registration_table_column).as_alias(registration_column.name)


def _build_cte(app_id: str, registration_column: RegistrationColumn):
    cte_name = '_base'

    registration_table = Table(
        registration_column.registration_table_dataset_name(app_id),
        registration_column.registration_table_column)

    select = SelectStatement()\
        .select([
            Expression(registration_table.column(constants.DATE_PARTITION_COLUMN_NAME).to_sql()),
            Expression(registration_table.column(constants.UNIQUE_ID_COLUMN_NAME).to_sql()),
            _formula_column(app_id, registration_column),
        ])\
        .from_(registration_table) \
        .group_by([
            Expression(registration_table.column(constants.DATE_PARTITION_COLUMN_NAME).to_sql()),
            Expression(registration_table.column(constants.UNIQUE_ID_COLUMN_NAME).to_sql()),
        ])

    return Cte(cte_name, select)


def _register_and_join_cte(cte: Cte, select_statement: SelectStatement, sql_builder: QueryBuilder, user_history_table: TableLike):
    sql_builder.with_cte(cte)
    select_statement.join(Join(join_type=JoinType.LEFT, table=cte).on(BooleanExpression.all_and_([
        BooleanExpression.as_(
            f'{user_history_table.column(constants.DATE_PARTITION_COLUMN_NAME).to_sql()} = {cte.column(constants.DATE_PARTITION_COLUMN_NAME).to_sql()}'),
        BooleanExpression.as_(
            f'{user_history_table.column(constants.UNIQUE_ID_COLUMN_NAME).to_sql()} = {cte.column(constants.UNIQUE_ID_COLUMN_NAME).to_sql()}'),
    ])))




