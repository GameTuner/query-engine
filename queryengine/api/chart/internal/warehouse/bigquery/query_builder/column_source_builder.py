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

from queryengine.api.chart.internal.warehouse.bigquery.query_builder import common
from queryengine.core.bigquery.column_source import TableColumnSource
from queryengine.core.user_history_definition.column_sources.column_sources import ColumnSource, QueryUserHistoryColumnSource
from queryengine.core import constants
from queryengine.core.bigquery.sql.boolean_expression import BooleanExpression
from queryengine.core.bigquery.sql.sql_builder import SelectStatement, UnionStatement, QueryBuilder
from queryengine.core.bigquery.sql.table_like import Cte
from queryengine.core.datasource.datasource import DataType, DataSource
from queryengine.core.datasource.datasources import EventDataSource, UserHistoryDataSource


def build(app_id: str, table_name: str, datasource: DataSource,
          user_history_definition, sql_builder: QueryBuilder, select_statement: SelectStatement) -> ColumnSource:
    if isinstance(datasource, EventDataSource):
        table = _build_events_table(app_id, datasource, sql_builder)
        return TableColumnSource(table)
    elif isinstance(datasource, UserHistoryDataSource):
        table = common.build_table(app_id, datasource.schema, table_name)
        return QueryUserHistoryColumnSource(app_id, table, user_history_definition, sql_builder, select_statement)
    else:
        table = common.build_table(app_id, datasource.schema, table_name)
        return TableColumnSource(table)


def _build_events_table(app_id, datasource, sql_builder) -> Cte:
    raw_table = common.build_table(app_id, datasource.schema, datasource.table_name)
    load_table = common.build_table(app_id, datasource.realtime_schema, datasource.table_name)

    after_raw_filter = BooleanExpression.from_filter(
        load_table.column(constants.DATE_PARTITION_COLUMN_NAME),
        '>',
        [str(datasource.raw_data_availability.date_to.date())], DataType.date
    ) if datasource.raw_data_availability else BooleanExpression.as_('TRUE')
    cte = Cte(
        cte_name='base',
        select=UnionStatement([
            SelectStatement() \
                .select_star() \
                .from_(raw_table) \
                .where(
                BooleanExpression.from_date(
                    raw_table.column(constants.DATE_PARTITION_COLUMN_NAME),
                    datasource.raw_data_availability
                ) if datasource.raw_data_availability else BooleanExpression.as_('FALSE')
            ),
            #  TODO hash gdpr columns
            SelectStatement() \
                .select_star() \
                .from_(load_table) \
                .where(
                after_raw_filter.and_(BooleanExpression.from_filter(
                    load_table.column(constants.EVENT_SANDBOX_COLUMN_NAME),
                    'boolean_is_not',
                    ['TRUE'], DataType.boolean
                ))

            )
        ]))
    sql_builder.with_cte(cte)
    return cte
