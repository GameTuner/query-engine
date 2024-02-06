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

import time

from fastapi.logger import logger
from opentelemetry import trace

from queryengine.api.chart.internal.domain import ChartQuery, ChartQueryResult
from queryengine.api.chart.internal.semantic_layer.kpi_chart_result import KpiChartQueryResult
from queryengine.api.chart.internal.warehouse.warehouse import Warehouse
from queryengine.api.chart.internal.x_axis_specifics.x_axis_specifics import get_x_axis_specifics
from queryengine.api.chart.request import ChartQueryDTO
from queryengine.api.chart.response import ChartDataDTO
from queryengine.core.app.datasource import AppRepository
from queryengine.core.datasource.repository import DataSourceRepository
from queryengine.core.kpi.repository import KpiRepository
from queryengine.core.warehouse import TooManyRowsException, TooManyGroupByValuesException
from queryengine.core import constants


class ChartQueryService:
    def __init__(self, app_repository: AppRepository, datasource_repository: DataSourceRepository,
                 kpi_repository: KpiRepository, warehouse: Warehouse):
        self._app_repository = app_repository
        self._datasource_repository = datasource_repository
        self._kpi_repository = kpi_repository
        self._warehouse = warehouse

    def execute(self, app_id: str, query_dto: ChartQueryDTO) -> ChartDataDTO:
        query = query_dto.to_domain_model(
            app_id=app_id,
            app_repository=self._app_repository,
            datasource_repository=self._datasource_repository,
            kpi_repository=self._kpi_repository
        )
        if not query.date_interval:
            return ChartDataDTO.from_chart_results(self._empty_result(query))

        tracer = trace.get_tracer("query_engine")
        with tracer.start_as_current_span("query-engine-chart-request") as span:
            span.set_attribute("request_id", query.request_id)
            span.set_attribute("datasource", query.datasource.id)
            span.set_attribute("x_axis", query.x_axis_column.column_id)
            span.set_attribute("app_id", query.app.app_id())
            span.set_attribute("days", query.date_interval.days())
            

            start_time = time.time()

            try:
                # warehouse data
                query_start_time = time.time()
                warehouse_compared_results = get_x_axis_specifics(query.x_axis_column.column_id) \
                    .get_warehouse_compared_results(query, self._warehouse)
                query_end_time = time.time()

                semantic_start_time = time.time()
                with tracer.start_as_current_span("semantic_layer"):
                    chart_result = self._apply_semantic_layer(query, warehouse_compared_results)
                semantic_end_time = time.time()
                span.set_attribute("status", "OK")
                span.set_status(trace.Status(trace.StatusCode.OK, "OK"))
            except Exception as ex:
                span.set_attribute("exception_type", ex.__class__.__name__)
                span.set_attribute("status", "ERROR")
                span.set_status(trace.Status(trace.StatusCode.ERROR, "ERROR"))
                span.record_exception(ex)
                raise
            

            end_time = time.time()
            logger.info(f'''Chart query {query} finished.
Warehouse query: {query_end_time - query_start_time:.2f}s
Semantic layer: {semantic_end_time - semantic_start_time:.2f}s
-----------------------
Total: {end_time - start_time:.2f}s
''')
            return chart_result

    def _empty_result(self, query: ChartQuery):
        return ChartQueryResult(
            chart_data=None,
            chart_total=None,
            chart_total_overall=None,
            compare_period_chart_data=None,
            compare_period_chart_total=None,
            compare_period_chart_total_overall=None,
            unit=query.kpi.unit
        )

    def _apply_semantic_layer(self, query, warehouse_compared_results) -> ChartDataDTO:
        #check distinct values in group by columns
        tracer = trace.get_tracer("query_engine")
        distinct_group_by_values = warehouse_compared_results.results.group_by_columns_distinct_values_count()
        max_rows = warehouse_compared_results.results.get_max_rows()
        if distinct_group_by_values > constants.BIGQUERY_MAX_DISTINCT_GROUP_BY_VALUES and \
            max_rows > constants.BIGQUERY_MAX_ROWS / 2:
            with tracer.start_as_current_span("group_by_columns_distinct_value") as span:
                span.set_attribute("distinct_group_by_values", distinct_group_by_values)
                span.set_attribute("rows_count", max_rows)
                raise TooManyGroupByValuesException()

        if len(warehouse_compared_results.results.group_by_columns()) > 0 and len(
                warehouse_compared_results.results.group_by_values()) == 0:
            # special case where we use group by but got no data so we can't easily generate identity values with 0
            # we just return empty results
            chart_result = self._empty_result(query)
        else:
            # semantic layer data
            with tracer.start_as_current_span("semantic_layer_build_results") as span:
                kpi_result = KpiChartQueryResult.build_from_result(
                    query, warehouse_compared_results.results,
                    warehouse_compared_results.sort_by_results)
            with tracer.start_as_current_span("semantic_layer_build_compare_results") as span:
                compared_kpi_result = KpiChartQueryResult.build_from_compare_result(
                    query, warehouse_compared_results.compare_results, kpi_result.result)

            chart_result = ChartQueryResult(
                chart_data=kpi_result.result,
                chart_total=kpi_result.total,
                chart_total_overall=kpi_result.single_total,
                compare_period_chart_data=compared_kpi_result.result,
                compare_period_chart_total=compared_kpi_result.total,
                compare_period_chart_total_overall=compared_kpi_result.single_total,
                unit=query.kpi.unit
            )
        return ChartDataDTO.from_chart_results(chart_result)
