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

from datetime import datetime

import pytz
from pandas import DataFrame

from queryengine.api.chart.internal.domain import WarehouseChartQuery
from queryengine.api.chart.internal.warehouse.warehouse import Warehouse
from queryengine.api.column_values.internal.domain import ColumnValuesQuery
from queryengine.core import constants
from queryengine.core.tabular_data_result import TabularDataResults, TabularDataResult
from queryengine.core.warehouse import FutureResult


class TestFutureResult(FutureResult):
    def __init__(self, result):
        self._result = result

    def get(self) -> TabularDataResults:
        return self._result


class MissingDataTestWarehouse(Warehouse):
    def submit_query(self, query: WarehouseChartQuery) -> FutureResult:
        results = TabularDataResults()
        for metric_id, metric in query.metrics.items():
            results.add(metric_id, TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [],
                constants.DATA_COLUMN_ALIAS: []
            })))
        return TestFutureResult(results)


class QueuedDataTestWarehouse(Warehouse):
    def __init__(self):
        self._data_queue = []

    def enqueue_data(self, results: TabularDataResults):
        self._data_queue.append(results)

    def submit_query(self, query: WarehouseChartQuery) -> FutureResult:
        return TestFutureResult(self._data_queue.pop(0))


class TestWarehouse(Warehouse):
    def submit_query(self, query: WarehouseChartQuery) -> FutureResult:
        reference_date = datetime(2022, 1, 1).replace(tzinfo=pytz.UTC)

        results = TabularDataResults()
        for metric_id, metric in query.metrics.items():
            if query.x_axis_column.column_id == constants.DATE_PARTITION_COLUMN_NAME:
                results.add(metric_id, TabularDataResult.from_date_interval(
                    query.date_intervals[0], time_grain=query.time_grain, group_by_values=set(), group_by_columns=[]))
            elif query.x_axis_column.column_id == constants.COHORT_DAY_COLUMN_NAME:
                results.add(metric_id, TabularDataResult.from_cohort_days(
                    query.date_intervals[0].days() // 2, group_by_values=set(), group_by_columns=[]))

            # add auto increment values based on reference date, so we have predictable values
            results.results_map[metric_id].df[constants.DATA_COLUMN_ALIAS] = [
                (query.date_intervals[0].date_from.date() - reference_date.date()).days + i * 1.0
                for i in range(1, len(results.results_map[metric_id].df) + 1)]

        return TestFutureResult(results)

    def submit_column_values_query(self, query: ColumnValuesQuery) -> FutureResult:
        pass
