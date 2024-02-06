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

from opentelemetry import trace

from queryengine.api.event_errors.internal.warehouse.bigquery import query_builder
from queryengine.api.event_errors.internal.warehouse.warehouse import Warehouse
from queryengine.api.event_errors.internal.domain import EventErrorsQuery
from queryengine.core.bigquery.queryexecutor import BigQueryExecutor, BigQueryFutureResult
from queryengine.core.warehouse import FutureResult


class BigQueryWarehouse(Warehouse):
    def __init__(self, big_query_executor: BigQueryExecutor):
        self.big_query_executor = big_query_executor

    def submit_query(self, query: EventErrorsQuery) -> FutureResult:
        tracer = trace.get_tracer("query_engine")
        with tracer.start_as_current_span("build_sql"):
            bigquery_query_map = query_builder.build(query)
            futures = []
            for symbol_id, query in bigquery_query_map.items():
                futures.append((symbol_id, query, self.big_query_executor.execute(query)))
            return BigQueryFutureResult(futures)
