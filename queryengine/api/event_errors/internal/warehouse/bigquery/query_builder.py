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

import os
from typing import Dict

from queryengine.api.event_errors.internal.domain import EventErrorsQuery
from queryengine.core import constants
from queryengine.core.bigquery.queryexecutor import BigQueryQuery
from queryengine.core.bigquery.sql.boolean_expression import BooleanExpression
from queryengine.core.bigquery.sql.sql_builder import QueryBuilder, SelectStatement
from queryengine.core.bigquery.sql.table_like import Table
from queryengine.core.datasource.datasource import DataType
from queryengine.core.bigquery.sql.expression import Expression


def build(query: EventErrorsQuery) -> Dict[str, BigQueryQuery]:
    sql_builder = QueryBuilder()
    #TODO - extract dataset and table name outside of query builder
    table = Table(dataset_name='gametuner_monitoring',
                  table_name='v_enrich_bad_events')
    
    select_statement = SelectStatement() \
        .select([Expression('event_name'), Expression('COUNT(1)').as_alias(constants.X_AXIS_COLUMN_ALIAS)]) \
        .from_(table) \
        .where(BooleanExpression.from_timestamp(table.column(column_name='load_tstamp'), query.date_interval)) \
        .and_where(BooleanExpression.from_filter(table.column(column_name='app_id'), "=", [query.app.app_id()], DataType.string)) \
        .and_where(BooleanExpression.from_filter(table.column(column_name='event_name'), "is_not_null", [], DataType.boolean)) \
        .group_by([Expression('event_name')])
    
    if query.event_name:
        select_statement = select_statement.and_where(BooleanExpression.from_filter(table.column(column_name='event_name'), "=", [query.event_name], DataType.string))
    sql_builder.select(select_statement)

    return {"event_errors": BigQueryQuery(
        query.page_id, query.request_id,
        "event_errors_query",
        sql_builder.to_sql(),
        query.app.app_id())}
