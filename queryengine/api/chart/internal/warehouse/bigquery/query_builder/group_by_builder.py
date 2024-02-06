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

from queryengine.api.chart.internal.warehouse.bigquery.query_builder import join_builder, column_source_builder
from queryengine.core.user_history_definition.column_sources.column_sources import ColumnSource
from queryengine.core.bigquery.sql.expression import Expression
from queryengine.core.bigquery.sql.sql_builder import QueryBuilder, SelectStatement
from queryengine.core.datasource.datasource import ColumnReference, DataSource
from queryengine.core.datasource.datasources import UserHistoryDataSource
from queryengine.core.dateinterval import DatetimeInterval


def build(app_id: str, column_source: ColumnSource, date_intervals: List[DatetimeInterval],
          datasource: DataSource, group_by: ColumnReference, alias: str,
          select_statement: SelectStatement, sql_builder: QueryBuilder) -> Expression:
    if group_by.datasource == datasource:
        return column_source.get_and_load_column(group_by.column_id, date_intervals).as_alias(alias)
    else:
        joined_table = join_builder.prepare_many_rows_per_user_to_one_row_per_user(
            app_id=app_id, date_intervals=date_intervals, datasource=datasource, select_statement=select_statement,
            join_datasource=group_by.datasource, sql_builder=sql_builder
        )
        user_history_definition = group_by.datasource.user_history_definition \
            if isinstance(group_by.datasource, UserHistoryDataSource) else None
        column_source = column_source_builder.build(
            app_id, joined_table.table_name, group_by.datasource, user_history_definition, sql_builder, select_statement)
        return column_source.get_and_load_column(group_by.column_id, date_intervals).as_alias(alias)
