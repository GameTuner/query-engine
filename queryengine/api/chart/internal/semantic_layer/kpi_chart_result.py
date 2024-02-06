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
from typing import Tuple, List
from opentelemetry import trace

from queryengine.api.chart.internal.domain import ChartQuery
from queryengine.api.chart.internal.semantic_layer.rollup_data_result import RollupDataResults, RollupDataResult
from queryengine.api.chart.internal.x_axis_specifics.x_axis_specifics import get_x_axis_specifics
from queryengine.core.kpi.kpi import Kpi
from queryengine.core.tabular_data_result import TabularDataResult, TabularDataResults


@dataclass
class KpiChartQueryResult:
    result: TabularDataResult | None
    total: TabularDataResult | None
    single_total: TabularDataResult | None

    @staticmethod
    def build_from_result(query: ChartQuery, query_results: TabularDataResults,
                          sort_by_query_results: TabularDataResults | None):
        x_axis_specifics = get_x_axis_specifics(query.x_axis_column.column_id)
        tracer = trace.get_tracer("query_engine")
        with tracer.start_as_current_span("calculate_results") as span:
            result, rollup_results, identity_table_result = KpiChartQueryResult.limit_group_by_values(query, query_results,
                                                                                                    sort_by_query_results)
        with tracer.start_as_current_span("trim_zeros") as span:
            result = KpiChartQueryResult.trim_zeros_result(result)
            rollup_results = KpiChartQueryResult.trim_zeros_rollup_results(rollup_results)
            identity_table_result = KpiChartQueryResult.trim_zeros_rollup_results(identity_table_result)

        with tracer.start_as_current_span("calculate_totals") as span:
            total = x_axis_specifics.get_total(query, identity_table_result, rollup_results)
            single_total = x_axis_specifics.get_single_total(query, identity_table_result, rollup_results)

        return KpiChartQueryResult(
            result=result,
            total=total,
            single_total=single_total
        )

    @staticmethod
    def build_from_compare_result(query: ChartQuery, compare_query_results: TabularDataResults,
                                  result: TabularDataResult):
        if not compare_query_results:
            return KpiChartQueryResult(None, None, None)
        compare_query_results = compare_query_results.filter_by_group_by_values(result.group_by_values())
        if compare_query_results.group_by_columns() and not compare_query_results.group_by_values():
            # everything was filtered out
            return KpiChartQueryResult(None, None, None)
        compare_identity_date_interval = get_x_axis_specifics(query.x_axis_column.column_id) \
            .get_compare_identity_date_interval(query)

        x_axis_specifics = get_x_axis_specifics(query.x_axis_column.column_id)
        identity_table_result = x_axis_specifics.get_identity_result(
            compare_identity_date_interval, query.time_grain, compare_query_results.group_by_columns(),
            compare_query_results.group_by_values())
        rollup_results = KpiChartQueryResult._get_rollup_results(query, query.kpi, identity_table_result,
                                                                 compare_query_results)
        result = x_axis_specifics.get_semantic_layer_result(query, query.kpi, identity_table_result, rollup_results)

        result = KpiChartQueryResult.trim_zeros_result(result)
        rollup_results = KpiChartQueryResult.trim_zeros_rollup_results(rollup_results)
        identity_table_result = KpiChartQueryResult.trim_zeros_rollup_results(identity_table_result)

        total = x_axis_specifics.get_total(query, identity_table_result, rollup_results)
        single_total = x_axis_specifics.get_single_total(query, identity_table_result, rollup_results)

        return KpiChartQueryResult(
            result=result,
            total=total,
            single_total=single_total
        )

    @staticmethod
    def limit_group_by_values(query: ChartQuery, query_results, sort_by_query_results: TabularDataResults | None):

        x_axis_specifics = get_x_axis_specifics(query.x_axis_column.column_id)

        tracer = trace.get_tracer("query_engine")
        if query.group_by_limit is None or query.group_by_limit == 0:
            with tracer.start_as_current_span("group_by_limit") as span:
                identity_table_result = x_axis_specifics.get_identity_result(
                    query.date_interval, query.time_grain, query_results.group_by_columns(),
                    query_results.group_by_values())
                rollup_results = KpiChartQueryResult._get_rollup_results(query, query.kpi, identity_table_result,
                                                                        query_results)
                result = x_axis_specifics.get_semantic_layer_result(query, query.kpi, identity_table_result, rollup_results)
                return result, rollup_results, identity_table_result

        if sort_by_query_results:
            with tracer.start_as_current_span("sort_by") as span:
                results = sort_by_query_results
                identity_table_result = x_axis_specifics.get_identity_result(
                    query.date_interval, query.time_grain, results.group_by_columns(),
                    results.group_by_values())
                rollup_results = KpiChartQueryResult._get_rollup_results(query, query.sort_by_kpi, identity_table_result,
                                                                        results)
                result = x_axis_specifics.get_semantic_layer_result(query, query.sort_by_kpi, identity_table_result,
                                                                    rollup_results)
        else:
            with tracer.start_as_current_span("rollup_results") as span:
                results = query_results
                identity_table_result = x_axis_specifics.get_identity_result(
                    query.date_interval, query.time_grain, results.group_by_columns(),
                    results.group_by_values())
                rollup_results = KpiChartQueryResult._get_rollup_results(query, query.kpi, identity_table_result, results)
                result = x_axis_specifics.get_semantic_layer_result(query, query.kpi, identity_table_result, rollup_results)

        with tracer.start_as_current_span("get_totals") as span:
            total_values = KpiChartQueryResult.get_total(result)
            total_values = total_values.get_top_n_values(query.group_by_limit)
            group_by_values = total_values.group_by_values()

        with tracer.start_as_current_span("final_rollup") as span:
            final_identity_table_result = x_axis_specifics.get_identity_result(
                query.date_interval, query.time_grain, query_results.group_by_columns(),
                query_results.group_by_values())
            final_rollup_results = KpiChartQueryResult._get_rollup_results(query, query.kpi, final_identity_table_result,
                                                                        query_results)
            final_result = x_axis_specifics.get_semantic_layer_result(query, query.kpi, final_identity_table_result,
                                                                    final_rollup_results)

            final_rollup_results = KpiChartQueryResult.filter_rollup_results(final_rollup_results, group_by_values)
            final_result = KpiChartQueryResult.filter_result(final_result, group_by_values)
            final_identity_table_result = KpiChartQueryResult.filter_rollup_results(final_identity_table_result,
                                                                                    group_by_values)
            return final_result, final_rollup_results, final_identity_table_result

    @staticmethod
    def trim_zeros_result(result: TabularDataResult):
        return result.trim_zeros()

    @staticmethod
    def trim_zeros_rollup_results(rollup_results: RollupDataResults):
        rollup_results.trim_zeros()
        return rollup_results

    @staticmethod
    def filter_rollup_results(rollup_results: RollupDataResults, group_by_values: List[Tuple]):
        return rollup_results.filter_by_group_by_values(group_by_values)

    @staticmethod
    def filter_result(result: TabularDataResult, group_by_values: List[Tuple]):
        return result.filter_by_group_by_values(group_by_values)

    @staticmethod
    def get_total(result: TabularDataResult) -> TabularDataResult:
        rollup = RollupDataResult(result, 'sum', 'sum')
        return rollup.rollup(x_axis_mapper=lambda dt: 0)

    @staticmethod
    def _get_rollup_results(query: ChartQuery, kpi: Kpi, identity_table_result, query_results) -> RollupDataResults:
        rollup_table_results = RollupDataResults()
        for id, warehouse_result in query_results.results_map.items():
            if warehouse_result.df.empty:
                warehouse_result = identity_table_result.warehouse_result
            rollup_table_results.add(id, RollupDataResult(
                warehouse_result=warehouse_result,
                x_axis_rollup_function_name=kpi.x_axis[query.x_axis_column.column_id].rollup_x_axis,
                y_axis_rollup_function_name=kpi.x_axis[query.x_axis_column.column_id].rollup_y_axis,
            ))
        return rollup_table_results
