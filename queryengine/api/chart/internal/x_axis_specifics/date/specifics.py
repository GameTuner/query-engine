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

from datetime import timedelta
from typing import Set, Tuple, List

from queryengine.api.chart.internal.domain import ChartQuery
from queryengine.api.chart.internal.semantic_layer import formula_interpreter
from queryengine.api.chart.internal.semantic_layer.rollup_data_result import RollupDataResult, RollupDataResults
from queryengine.api.chart.internal.warehouse.warehouse import Warehouse
from queryengine.api.chart.internal.x_axis_specifics.x_axis_specifics import XAxisSpecifics, WarehouseComparedResults
from queryengine.core import constants
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.kpi.kpi import Kpi
from queryengine.core.tabular_data_result import TabularDataResult
from queryengine.core.timegrain import TimeGrain


class DateSpecifics(XAxisSpecifics):
    def get_warehouse_compared_results(self, query: ChartQuery, warehouse: Warehouse) -> WarehouseComparedResults:
        warehouse_query = query.to_warehouse_query()
        result_future = warehouse.submit_query(warehouse_query)

        sort_by_warehouse_query = query.to_sort_by_warehouse_query()
        sort_by_future = None
        if sort_by_warehouse_query:
            sort_by_future = warehouse.submit_query(sort_by_warehouse_query)

        compared_warehouse_query = query.to_compare_warehouse_query()
        if compared_warehouse_query:
            compare_future = warehouse.submit_query(compared_warehouse_query)
            results = result_future.get()

            compare_results = compare_future.get() \
                .map_x_axis(mapper=lambda dt: dt + timedelta(days=query.compare_align_offset())) \
                .filter(lambda dt: query.requested_date_interval.contains_date(dt.date()))

            return WarehouseComparedResults(
                results=results,
                compare_results=compare_results,
                sort_by_results=sort_by_future.get() if sort_by_future else None
            )
        else:
            return WarehouseComparedResults(
                results=result_future.get(),
                compare_results=None,
                sort_by_results=sort_by_future.get() if sort_by_future else None
            )

    def get_identity_result(self, date_interval: DatetimeInterval, time_grain: TimeGrain | None,
                            group_by_columns: List[str], group_by_values: Set[Tuple]) -> RollupDataResult:
        return RollupDataResult(
            warehouse_result=TabularDataResult.from_date_interval(
                date_interval,
                time_grain,
                group_by_columns=group_by_columns,
                group_by_values=group_by_values,
            ),
            x_axis_rollup_function_name='sum',
            y_axis_rollup_function_name='sum'
        )

    def get_compare_identity_date_interval(self, query: ChartQuery) -> DatetimeInterval:
        return DatetimeInterval(
            max(query.date_interval.date_from,
                query.compare_interval.date_from + timedelta(days=query.compare_align_offset())),
            query.compare_interval.date_to + timedelta(days=query.compare_align_offset())
        )

    def get_semantic_layer_result(self, query: ChartQuery, kpi: Kpi,
                                  identity_table_result: RollupDataResult,
                                  rollup_table_results: RollupDataResults) -> TabularDataResult:
        result = formula_interpreter.evaluate(
            identity=identity_table_result
            .rollup(x_axis_mapper=lambda s: s.apply(query.time_grain.truncate_datetime)),
            formula=kpi.formula,
            values=rollup_table_results
            .rollup(x_axis_mapper=lambda s: s.apply(query.time_grain.truncate_datetime)),
        )
        if query.datasource.time_grain.to_minutes() >= TimeGrain.day.to_minutes():
            result.df[constants.X_AXIS_COLUMN_ALIAS] = result.df[
                constants.X_AXIS_COLUMN_ALIAS].apply(lambda dt: dt.date())
        return result

    def get_total(self, query: ChartQuery,
                  identity_table_result: RollupDataResult,
                  rollup_table_results: RollupDataResults) -> TabularDataResult | None:
        total = formula_interpreter.evaluate(
            identity=identity_table_result.rollup(x_axis_mapper=lambda dt: 0),
            formula=query.kpi.formula,
            values=rollup_table_results.rollup(x_axis_mapper=lambda dt: 0),
        )
        return total

    def get_single_total(self, query: ChartQuery,
                         identity_table_result: RollupDataResult,
                         rollup_table_results: RollupDataResults) -> TabularDataResult | None:
        return formula_interpreter.evaluate(
            identity=identity_table_result
            .rollup(x_axis_mapper=lambda dt: 0, group_by_columns_mapper=lambda x: 0),
            formula=query.kpi.formula,
            values=rollup_table_results
            .rollup(x_axis_mapper=lambda dt: 0, group_by_columns_mapper=lambda x: 0)
        )
