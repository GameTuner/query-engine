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
from typing import List

from fastapi.logger import logger
from opentelemetry import trace

from queryengine.api.event_errors.internal.warehouse.warehouse import Warehouse
from queryengine.api.event_errors.request import EventErrorsQueryDTO
from queryengine.core import constants
from queryengine.core.app.datasource import AppRepository
from queryengine.core.datasource.repository import DataSourceRepository


class EventErrorsQueryService:
    def __init__(self, app_repository: AppRepository, datasource_repository: DataSourceRepository,
                 warehouse: Warehouse):
        self._app_repository = app_repository
        self._datasource_repository = datasource_repository
        self.warehouse = warehouse

    def execute(self, app_id: str, query_dto: EventErrorsQueryDTO) -> int:
        query = query_dto.to_domain_model(
            app_id=app_id,
            app_repository=self._app_repository
        )
        if not query.date_interval:
            return None

        tracer = trace.get_tracer("query_engine")
        with tracer.start_as_current_span("query-engine-event-errors-request") as span:
            span.set_attribute("request_id", query.request_id)
            span.set_attribute("app_id", query.app.app_id())
            span.set_attribute("event_name", query.event_name)
            span.set_attribute("days", query.date_interval.days())

            start_time = time.time()
            tabular_data_results = self.warehouse.submit_query(query).get()
            end_time = time.time()
            logger.info(f'''Event errors query {query} finished. Total: {end_time - start_time:.2f}s''')

            results = {}
            
            for row in tabular_data_results.results_map["event_errors"].df.to_dict(orient='records'):
                if row["event_name"]:
                    results[row["event_name"]] = row[constants.X_AXIS_COLUMN_ALIAS]
                    
            return results
