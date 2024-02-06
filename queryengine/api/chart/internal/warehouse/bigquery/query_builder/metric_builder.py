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

from queryengine.core.user_history_definition.column_sources.column_sources import ColumnSource
from queryengine.core import constants
from queryengine.core.bigquery.sql.boolean_expression import BooleanExpression
from queryengine.core.bigquery.sql.expression import Expression, TemplateDict, DotsFormatter
from queryengine.core.datasource.datasource import DataSource
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.kpi.kpi import WarehouseMetric


def build_select_expression(metric: WarehouseMetric, column_source: ColumnSource,
                            date_intervals: List[DatetimeInterval], datasource: DataSource) -> Expression:
    column_names = DotsFormatter().get_format_strings(metric.select_expression)
    columns = {c.id: column_source.get_and_load_column(c.id, date_intervals).to_sql()
               for c in datasource.columns_by_id.values() if c.id in column_names}
    return Expression(
        metric.select_expression,
        template_dict=TemplateDict(columns)
    ).as_alias(constants.DATA_COLUMN_ALIAS)


def build_boolean_expression(metric: WarehouseMetric, column_source: ColumnSource,
                             date_intervals: List[DatetimeInterval], datasource: DataSource) -> BooleanExpression:
    column_names = DotsFormatter().get_format_strings(metric.where_expression)
    columns = {c.id: column_source.get_and_load_column(c.id, date_intervals).to_sql()
               for c in datasource.columns_by_id.values() if c.id in column_names}

    return BooleanExpression(Expression(
        expression=metric.where_expression,
        template_dict=TemplateDict(columns),
    ))
