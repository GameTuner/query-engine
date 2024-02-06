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

from typing import List

from queryengine.api.chart.internal.warehouse.bigquery.query_builder import column_source_builder, common
from queryengine.core import constants
from queryengine.core.bigquery.sql.boolean_expression import BooleanExpression
from queryengine.core.bigquery.sql.join import Join, JoinType
from queryengine.core.bigquery.sql.sql_builder import QueryBuilder, SelectStatement
from queryengine.core.bigquery.sql.table_like import Table
from queryengine.core.datasource.datasource import DataSource, Cardinality
from queryengine.core.datasource.datasources import UserHistoryDataSource
from queryengine.core.dateinterval import DatetimeInterval


def prepare_many_rows_per_user_to_one_row_per_user(
        app_id: str, date_intervals: List[DatetimeInterval], datasource: DataSource, select_statement: SelectStatement,
        join_datasource: DataSource, sql_builder: QueryBuilder) -> Table:
    if datasource.rows_per_user != Cardinality.many:
        raise Exception(f"Datasource {datasource.id} cannot use join from different data sources")

    if join_datasource.rows_per_user != Cardinality.one:
        raise Exception(f"Can only join by datasources with one row per user!")

    user_history_definition = join_datasource.user_history_definition \
        if isinstance(join_datasource, UserHistoryDataSource) else None

    column_source = column_source_builder.build(
        app_id, join_datasource.user_enrich_table_name(), join_datasource, user_history_definition, sql_builder, select_statement)

    join_date_column = column_source.get_and_load_column(constants.DATE_PARTITION_COLUMN_NAME, date_intervals)
    join_unique_id_column = column_source.get_and_load_column(constants.UNIQUE_ID_COLUMN_NAME, date_intervals)

    join = Join(join_type=JoinType.INNER, table=column_source.table).on(BooleanExpression.all_and_([
        BooleanExpression.as_(
            f'{select_statement.get_table().column(constants.DATE_PARTITION_COLUMN_NAME).to_sql()} = {join_date_column.to_sql()}'),
        BooleanExpression.as_(
            f'{select_statement.get_table().column(constants.UNIQUE_ID_COLUMN_NAME).to_sql()} = {join_unique_id_column.to_sql()}'),
    ]
    ))

    if join.to_sql() not in [j.to_sql() for j in select_statement.get_joins()]:
        select_statement.join(Join(join_type=JoinType.INNER, table=column_source.table).on(BooleanExpression.all_and_([
            BooleanExpression.as_(
                f'{select_statement.get_table().column(constants.DATE_PARTITION_COLUMN_NAME).to_sql()} = {join_date_column.to_sql()}'),
            BooleanExpression.as_(
                f'{select_statement.get_table().column(constants.UNIQUE_ID_COLUMN_NAME).to_sql()} = {join_unique_id_column.to_sql()}'),
        ]
        )))
    return common.build_table(app_id, join_datasource.schema, join_datasource.user_enrich_table_name())
