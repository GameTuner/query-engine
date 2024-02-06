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

from typing import List

from fastapi import APIRouter, Depends, HTTPException
from fastapi.logger import logger

from queryengine import dependencies
from queryengine.api.column_values.internal.warehouse.bigquery.bigquery_warehouse import BigQueryWarehouse
from queryengine.api.column_values.internal.warehouse.warehouse import Warehouse
from queryengine.api.column_values.request import ColumnValuesQueryDTO
from queryengine.api.column_values.service import ColumnValuesQueryService
from queryengine.core.app.datasource import AppRepository
from queryengine.core.bigquery.queryexecutor import BigQueryExecutor
from queryengine.core.datasource.repository import DataSourceRepository
from queryengine.core.warehouse import TooManyRequestsException
from queryengine.logging.router import LoggingRoute

router = APIRouter(route_class=LoggingRoute)


def bigquery_warehouse(
        bigquery_executor: BigQueryExecutor = Depends(dependencies.bigquery_executor)) -> BigQueryWarehouse:
    yield BigQueryWarehouse(bigquery_executor)


@router.post("/api/v1/{app_id}/column-values/submit")
def submit_colum_values(app_id: str, request: ColumnValuesQueryDTO,
                        warehouse: Warehouse = Depends(bigquery_warehouse),
                        app_config_repository: AppRepository = Depends(dependencies.app_config_repository),
                        datasource_repository: DataSourceRepository = Depends(dependencies.datasource_repository),
                        ) -> List:
    logger.info(f'Got column values request: {request}')
    try:
        return ColumnValuesQueryService(
            app_repository=app_config_repository,
            datasource_repository=datasource_repository,
            warehouse=warehouse
        ).execute(app_id, request)
    except TooManyRequestsException:
        raise HTTPException(status_code=429)
