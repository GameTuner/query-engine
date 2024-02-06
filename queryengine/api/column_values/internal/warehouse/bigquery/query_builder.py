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

from queryengine.api.column_values.internal.domain import ColumnValuesQuery
from queryengine.core import constants
from queryengine.core.bigquery.queryexecutor import BigQueryQuery
from queryengine.core.bigquery.sql.boolean_expression import BooleanExpression
from queryengine.core.bigquery.sql.sql_builder import QueryBuilder, SelectStatement
from queryengine.core.bigquery.sql.table_like import Table


def build(query: ColumnValuesQuery) -> Dict[str, BigQueryQuery]:
    sql_builder = QueryBuilder()
    table = Table(dataset_name=f'{query.app.app_id()}_{query.column.datasource.schema}',
                  table_name=query.column.datasource.table_name)
    column = table.column(column_name=query.column.column_id, alias=constants.X_AXIS_COLUMN_ALIAS)
    select_statement = SelectStatement() \
        .select([column]) \
        .from_(table) \
        .where(BooleanExpression.from_date(table.column(column_name=constants.DATE_PARTITION_COLUMN_NAME),
                                           query.date_interval)) \
        .group_by([column]) \
        .limit(500)
    sql_builder.select(select_statement)

    return {query.column.column_id: BigQueryQuery(
        query.page_id, query.request_id,
        f"{query.column.datasource.id}.{query.column.datasource.table_name}.{query.column.column_id}",
        sql_builder.to_sql(),
        query.app.app_id())}
