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

from queryengine.api.chart.internal.domain import WarehouseChartQuery
from queryengine.api.chart.internal.warehouse.bigquery.query_builder import column_source_builder, metric_builder
from queryengine.api.chart.internal.warehouse.bigquery.query_builder import x_axis_expression_builder, filter_builder, \
    group_by_builder
from queryengine.core import constants
from queryengine.core.bigquery.queryexecutor import BigQueryQuery
from queryengine.core.bigquery.sql.boolean_expression import BooleanExpression, BooleanExpressionParenthesis
from queryengine.core.bigquery.sql.sql_builder import QueryBuilder, SelectStatement
from queryengine.core.datasource.datasources import UserHistoryDataSource


def build(query: WarehouseChartQuery) -> Dict[str, BigQueryQuery]:
    symbol_to_query_map = {}

    user_history_definition = query.datasource.user_history_definition \
        if isinstance(query.datasource, UserHistoryDataSource) else None

    for metric_id, metric in query.metrics.items():
        sql_builder = QueryBuilder()

        select_statement = SelectStatement()
        column_source = column_source_builder.build(query.app_id, metric.data_source_table, query.datasource,
                                                    user_history_definition, sql_builder, select_statement)

        select_statement = select_statement.from_(column_source.table)

        boolean_expression = None
        for date_interval in query.date_intervals:
            date_column = column_source.get_and_load_column(constants.DATE_PARTITION_COLUMN_NAME, [date_interval])
            date_condition = BooleanExpression.from_date(date_column, date_interval)
            if not boolean_expression:
                boolean_expression = date_condition
            else:
                boolean_expression = boolean_expression.or_(date_condition)
        select_statement.where(BooleanExpressionParenthesis(boolean_expression))
        kpi_expression = metric_builder.build_select_expression(
            metric, column_source, query.date_intervals, query.datasource)

        for filter in query.column_filters:
            filter_builder.build_from_filter(
                app_id=query.app_id, column_source=column_source, date_intervals=query.date_intervals,
                datasource=query.datasource, filter=filter,
                select_statement=select_statement, sql_builder=sql_builder)

        if metric.where_expression:
            select_statement.and_where(metric_builder.build_boolean_expression(
                metric, column_source, query.date_intervals, query.datasource))

        x_axis_expression = x_axis_expression_builder.build(query, column_source, query.date_intervals)

        group_by_expressions = []
        for idx, group_by_column in enumerate(query.column_group_bys):
            group_by_expressions.append(group_by_builder.build(
                app_id=query.app_id, column_source=column_source, date_intervals=query.date_intervals,
                datasource=query.datasource, group_by=group_by_column,
                alias=f'group_by_{idx + 1}', select_statement=select_statement, sql_builder=sql_builder
            ))

        select_statement \
            .select([x_axis_expression] + group_by_expressions + [kpi_expression]) \
            .group_by([x_axis_expression] + group_by_expressions) \
            .order_by([x_axis_expression])
        sql_builder.select(select_statement)

        symbol_to_query_map[metric_id] = BigQueryQuery(query.page_id, query.request_id,
                                                       f"{query.datasource.id}.{metric.data_source_table}.{metric_id}",
                                                       sql_builder.to_sql(),
                                                       query.app_id)
    return symbol_to_query_map
