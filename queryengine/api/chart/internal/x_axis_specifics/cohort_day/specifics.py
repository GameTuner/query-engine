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

from queryengine.api.chart.internal.domain import ChartQuery, WarehouseChartQuery, ColumnFilter, TimeGrain
from queryengine.api.chart.internal.semantic_layer import formula_interpreter
from queryengine.api.chart.internal.semantic_layer.rollup_data_result import RollupDataResult, RollupDataResults
from queryengine.api.chart.internal.warehouse.warehouse import Warehouse
from queryengine.api.chart.internal.x_axis_specifics.x_axis_specifics import XAxisSpecifics, WarehouseComparedResults
from queryengine.core import constants
from queryengine.core.datasource.datasource import ColumnReference
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.kpi.kpi import Kpi
from queryengine.core.tabular_data_result import TabularDataResult


class CohortDaySpecifics(XAxisSpecifics):
    def _preprocess_warehouse_query(self, warehouse_query: WarehouseChartQuery):
        warehouse_query.column_filters.append(ColumnFilter(
            column_ref=ColumnReference(warehouse_query.datasource, constants.REGISTRATION_DATE_COLUMN_NAME),
            operation='between',
            value_list=[str(warehouse_query.date_intervals[0].date_from.date()),
                        str(warehouse_query.date_intervals[0].date_to.date())]
        ))
        warehouse_query.date_intervals[0].date_to += timedelta(days=warehouse_query.date_intervals[0].days())
        return warehouse_query

    def get_warehouse_compared_results(self, query: ChartQuery, warehouse: Warehouse) -> WarehouseComparedResults:
        result_future = warehouse.submit_query(self._preprocess_warehouse_query(query.to_warehouse_query()))

        sort_by_warehouse_query = query.to_sort_by_warehouse_query()
        sort_by_future = None
        if sort_by_warehouse_query:
            sort_by_future = warehouse.submit_query(sort_by_warehouse_query)

        compare_query = query.to_compare_warehouse_query()
        if compare_query:
            compared_warehouse_query = self._preprocess_warehouse_query(compare_query)
            results = result_future.get()
            compare_results = warehouse.submit_query(compared_warehouse_query).get()

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
            warehouse_result=TabularDataResult.from_cohort_days(
                days=date_interval.days() // 2,
                group_by_columns=group_by_columns,
                group_by_values=group_by_values,
            ),
            x_axis_rollup_function_name='sum',
            y_axis_rollup_function_name='sum'
        )

    def get_compare_identity_date_interval(self, query: ChartQuery) -> DatetimeInterval:
        return query.compare_interval

    def get_semantic_layer_result(self, query: ChartQuery, kpi: Kpi,
                                  identity_table_result: RollupDataResult,
                                  rollup_table_results: RollupDataResults) -> TabularDataResult:
        result = formula_interpreter.evaluate(
            identity=identity_table_result
            .rollup(),
            formula=kpi.formula,
            values=rollup_table_results
            .rollup(),
        )
        result.df[constants.X_AXIS_COLUMN_ALIAS] = result.df[constants.X_AXIS_COLUMN_ALIAS].apply(lambda x: int(x))

        return result

    def get_total(self, query: ChartQuery,
                  identity_table_result: RollupDataResult,
                  rollup_table_results: RollupDataResults) -> TabularDataResult | None:
        return None

    def get_single_total(self, query: ChartQuery,
                         identity_table_result: RollupDataResult,
                         rollup_table_results: RollupDataResults) -> TabularDataResult | None:
        return None
