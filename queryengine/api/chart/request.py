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

from dataclasses import dataclass, field
from typing import List

from queryengine.api.chart.internal.domain import ChartQuery, ColumnFilter
from queryengine.core.app.app import App
from queryengine.core.app.datasource import AppRepository
from queryengine.core.datasource.repository import DataSourceRepository
from queryengine.core.dateinterval import DateInterval
from queryengine.core.kpi.repository import KpiRepository
from queryengine.core.timegrain import TimeGrain


@dataclass(frozen=True)
class ColumnFilterDTO:
    column_id: str
    operation: str
    value_list: List[str]

    def to_domain_model(self, app: App, datasource_repository: DataSourceRepository):
        return ColumnFilter(
            column_ref=datasource_repository.load_column_by_full_id(app=app, column_full_id=self.column_id),
            operation=self.operation,
            value_list=self.value_list
        )


@dataclass(frozen=True)
class ChartQueryDTO:
    page_id: str
    request_id: str
    kpi_id: str
    date_interval: DateInterval
    x_axis_column_id: str
    column_filters: List[ColumnFilterDTO] = field(default_factory=list)
    column_group_bys: List[str] = field(default_factory=list)
    time_grain: TimeGrain | None = TimeGrain.day
    compare_date_interval: DateInterval | None = None
    sort_by_kpi_id: str | None = None
    group_by_limit: int | None = None

    def to_domain_model(self, app_id: str, app_repository: AppRepository, datasource_repository: DataSourceRepository,
                        kpi_repository: KpiRepository) -> ChartQuery:
        app = app_repository.from_app_id(app_id)
        kpi_ref = kpi_repository.load_by_full_kpi_id(app=app, full_kpi_id=self.kpi_id)
        sort_by_kpi_ref = kpi_repository.load_by_full_kpi_id(app=app,
                                                             full_kpi_id=self.sort_by_kpi_id) if self.sort_by_kpi_id else None
        datasource = datasource_repository.load_datasource_by_id(app, kpi_ref.datasource_id)
        return ChartQuery(
            app=app,
            page_id=self.page_id,
            request_id=self.request_id,
            datasource=datasource,
            kpi=kpi_ref.kpi,
            time_grain=self.time_grain,
            date_interval=self.date_interval.to_datetime_interval(self.time_grain),
            compare_interval=self.compare_date_interval.to_datetime_interval(
                self.time_grain) if self.compare_date_interval else None,
            x_axis_column=datasource_repository.load_column_by_full_id(app, self.x_axis_column_id),
            column_filters=[u.to_domain_model(app, datasource_repository) for u in self.column_filters],
            column_group_bys=[datasource_repository.load_column_by_full_id(app=app, column_full_id=c)
                              for c in self.column_group_bys],
            sort_by_datasource=datasource_repository.load_datasource_by_id(app,
                                                                           sort_by_kpi_ref.datasource_id) if sort_by_kpi_ref else None,
            sort_by_kpi=sort_by_kpi_ref.kpi if sort_by_kpi_ref else None,
            group_by_limit=self.group_by_limit
        )
