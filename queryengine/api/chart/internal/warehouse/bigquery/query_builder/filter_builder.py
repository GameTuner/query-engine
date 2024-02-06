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

from queryengine.api.chart.internal.domain import ColumnFilter
from queryengine.api.chart.internal.warehouse.bigquery.query_builder import join_builder, column_source_builder
from queryengine.core.user_history_definition.column_sources.column_sources import ColumnSource
from queryengine.core.bigquery.sql.boolean_expression import BooleanExpression
from queryengine.core.bigquery.sql.sql_builder import QueryBuilder, SelectStatement
from queryengine.core.datasource.datasource import DataSource
from queryengine.core.datasource.datasources import UserHistoryDataSource
from queryengine.core.dateinterval import DatetimeInterval


def build_from_filter(app_id: str, column_source: ColumnSource, date_intervals: List[DatetimeInterval],
                      datasource: DataSource, filter: ColumnFilter,
                      select_statement: SelectStatement, sql_builder: QueryBuilder):
    if filter.column_ref.datasource == datasource:
        _build_from_local_filter(select_statement, filter, column_source,date_intervals)
    else:
        _build_from_external_filter(app_id, date_intervals, datasource, select_statement, filter, sql_builder)


def _build_from_local_filter(select_statement: SelectStatement, filter: ColumnFilter,
                             column_source: ColumnSource, date_intervals: List[DatetimeInterval]):
    column = column_source.get_and_load_column(filter.column_ref.column_id, date_intervals)
    select_statement.and_where(BooleanExpression.from_filter(column, filter.operation, filter.value_list,
                                                             filter.column_ref.column().data_type))


def _build_from_external_filter(app_id: str, date_intervals: List[DatetimeInterval],
                                datasource: DataSource, select_statement: SelectStatement,
                                filter: ColumnFilter, sql_builder: QueryBuilder):
    joined_table = join_builder.prepare_many_rows_per_user_to_one_row_per_user(
        app_id=app_id, date_intervals=date_intervals, datasource=datasource, select_statement=select_statement,
        join_datasource=filter.column_ref.datasource, sql_builder=sql_builder
    )
    user_history_definition = filter.column_ref.datasource.user_history_definition \
        if isinstance(filter.column_ref.datasource, UserHistoryDataSource) else None
    column_source = column_source_builder.build(
        app_id, joined_table.table_name, filter.column_ref.datasource, user_history_definition, sql_builder, select_statement)
    column = column_source.get_and_load_column(filter.column_ref.column_id, date_intervals)
    select_statement.and_where(BooleanExpression.from_filter(column, filter.operation, filter.value_list,
                                                             filter.column_ref.column().data_type))
