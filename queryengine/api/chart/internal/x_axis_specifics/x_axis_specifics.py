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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Set, List, Tuple

from queryengine.api.chart.internal.domain import ChartQuery
from queryengine.api.chart.internal.semantic_layer.rollup_data_result import RollupDataResult, RollupDataResults
from queryengine.api.chart.internal.warehouse.warehouse import Warehouse
from queryengine.core import constants
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.kpi.kpi import Kpi
from queryengine.core.tabular_data_result import TabularDataResults
from queryengine.core.timegrain import TimeGrain


@dataclass
class WarehouseComparedResults:
    results: TabularDataResults
    compare_results: TabularDataResults | None
    sort_by_results: TabularDataResults | None


class XAxisSpecifics(ABC):
    @abstractmethod
    def get_warehouse_compared_results(self, query: ChartQuery, warehouse: Warehouse) -> WarehouseComparedResults:
        pass

    @abstractmethod
    def get_identity_result(self, date_interval: DatetimeInterval, time_grain: TimeGrain | None,
                            group_by_columns: List[str], group_by_values: Set[Tuple]) -> RollupDataResult:
        pass

    @abstractmethod
    def get_compare_identity_date_interval(self, query: ChartQuery) -> DatetimeInterval:
        pass

    @abstractmethod
    def get_semantic_layer_result(self, query: ChartQuery, kpi: Kpi, identity_table_result: RollupDataResult,
                                  rollup_table_results: RollupDataResults):
        pass

    @abstractmethod
    def get_total(self, query: ChartQuery, identity_table_result: RollupDataResult,
                  rollup_table_results: RollupDataResults):
        pass

    @abstractmethod
    def get_single_total(self, query: ChartQuery, identity_table_result: RollupDataResult,
                         rollup_table_results: RollupDataResults):
        pass


def get_x_axis_specifics(x_axis_column_id: str):
    if x_axis_column_id == constants.DATE_PARTITION_COLUMN_NAME:
        from queryengine.api.chart.internal.x_axis_specifics.date.specifics import DateSpecifics
        return DateSpecifics()
    elif x_axis_column_id == constants.COHORT_DAY_COLUMN_NAME:
        from queryengine.api.chart.internal.x_axis_specifics.cohort_day.specifics import CohortDaySpecifics
        return CohortDaySpecifics()
    else:
        raise Exception(f"Unsupported x axis column: {x_axis_column_id}")
