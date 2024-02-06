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

from queryengine.api.chart.internal.domain import WarehouseChartQuery
from queryengine.core.user_history_definition.column_sources.column_sources import ColumnSource
from queryengine.core import constants
from queryengine.core.bigquery.sql.expression import Expression, TemplateDict
from queryengine.core.datasource.datasource import Cardinality
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.timegrain import TimeGrain


def _date_trunc_from_time_grain(time_grain: TimeGrain):
    if time_grain == TimeGrain.min15:
        return 'TIMESTAMP_ADD(TIMESTAMP_TRUNC({ts}, HOUR), INTERVAL CAST(EXTRACT(MINUTE FROM {ts}) / 15 AS INT64)*15 MINUTE)'
    elif time_grain == TimeGrain.hour:
        return 'DATE_TRUNC({ts}, HOUR)'
    else:
        return 'DATE_TRUNC({ts}, DAY)'


def build(query: WarehouseChartQuery, column_source: ColumnSource, date_intervals: List[DatetimeInterval]):
    if query.x_axis_column.column_id != constants.DATE_PARTITION_COLUMN_NAME:
        return column_source.get_and_load_column(query.x_axis_column.column_id, date_intervals)\
            .as_alias(alias=constants.X_AXIS_COLUMN_ALIAS)

    if query.datasource.rows_per_user == Cardinality.many:
        timestamp_column = column_source.get_and_load_column(constants.EVENT_TIMESTAMP_COLUMN_NAME, date_intervals)
        return Expression(
            _date_trunc_from_time_grain(query.time_grain),
            template_dict=TemplateDict({'ts': timestamp_column}),
        ).as_alias(constants.X_AXIS_COLUMN_ALIAS)
    else:
        date_column = column_source.get_and_load_column(constants.DATE_PARTITION_COLUMN_NAME, date_intervals)
        return Expression(
            'TIMESTAMP({x_axis})',
            template_dict=TemplateDict({'x_axis': date_column}),
        ).as_alias(constants.X_AXIS_COLUMN_ALIAS)
