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

from dataclasses import dataclass
from typing import List, Dict

from queryengine.core.app.app import App
from queryengine.core.datasource.datasource import DataSource, ColumnReference
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.kpi.kpi import Kpi, Unit, WarehouseMetric
from queryengine.core.tabular_data_result import TabularDataResult
from queryengine.core.timegrain import TimeGrain


@dataclass(frozen=True)
class ColumnFilter:
    column_ref: ColumnReference
    operation: str
    value_list: List[str]


@dataclass
class WarehouseChartQuery:
    app_id: str
    page_id: str
    request_id: str
    datasource: DataSource
    metrics: Dict[str, WarehouseMetric]
    date_intervals: List[DatetimeInterval]
    time_grain: TimeGrain
    column_filters: List[ColumnFilter]
    column_group_bys: List[ColumnReference]
    x_axis_column: ColumnReference

class ChartQuery:
    def __init__(self, app: App, page_id: str, request_id: str,
                 datasource: DataSource, kpi: Kpi, date_interval: DatetimeInterval,
                 time_grain: TimeGrain | None, x_axis_column: ColumnReference,
                 compare_interval: DatetimeInterval | None,
                 column_filters: List[ColumnFilter] = None, column_group_bys: List[ColumnReference] = None,
                 sort_by_datasource: DataSource | None = None,
                 sort_by_kpi: Kpi | None = None,
                 group_by_limit=None
                 ):
        self.app = app
        self.page_id = page_id
        self.request_id = request_id
        self.datasource = datasource
        self.kpi = kpi
        self.time_grain = time_grain
        self.x_axis_column = x_axis_column
        self.column_filters = column_filters or []
        self.column_group_bys = column_group_bys or []
        self.requested_date_interval = date_interval
        self.requested_compare_interval = compare_interval
        self.sort_by_datasource = sort_by_datasource
        self.sort_by_kpi = sort_by_kpi
        self.group_by_limit = group_by_limit

        self.date_interval = self.datasource.clamp_date_interval(self.requested_date_interval)
        self.compare_interval = self.datasource.clamp_date_interval(
            self.requested_compare_interval) if self.requested_compare_interval else None

    def compare_align_offset(self) -> int | None:
        if not self.requested_compare_interval:
            return None
        offset = (self.date_interval.date_to.date() - self.compare_interval.date_to.date()).days
        date_clamped_right = (self.requested_date_interval.date_to.date() - self.date_interval.date_to.date()).days
        compare_date_clamped_right = (
                    self.requested_compare_interval.date_to.date() - self.compare_interval.date_to.date()).days
        return offset + date_clamped_right - compare_date_clamped_right

    def to_warehouse_query(self) -> WarehouseChartQuery:
        return WarehouseChartQuery(
            app_id=self.app.app_id(),
            page_id=self.page_id,
            request_id=self.request_id,
            datasource=self.datasource,
            metrics=self.kpi.metrics,
            date_intervals=[self.date_interval],
            time_grain=self.time_grain,
            column_filters=list(self.column_filters),
            column_group_bys=list(self.column_group_bys),
            x_axis_column=self.x_axis_column
        )

    def to_compare_warehouse_query(self):
        if not self.compare_interval:
            return None
        query = self.to_warehouse_query()
        query.date_intervals = [self.compare_interval]
        return query

    def to_sort_by_warehouse_query(self):
        if not self.sort_by_datasource or not self.sort_by_kpi:
            return None
        if self.sort_by_datasource != self.datasource:
            return None
        query = self.to_warehouse_query()
        query.metrics = self.sort_by_kpi.metrics
        return query


@dataclass(frozen=True)
class ChartQueryResult:
    chart_data: TabularDataResult | None
    chart_total: TabularDataResult | None
    chart_total_overall: TabularDataResult | None

    compare_period_chart_data: TabularDataResult | None
    compare_period_chart_total: TabularDataResult | None
    compare_period_chart_total_overall: TabularDataResult | None

    unit: Unit | None
