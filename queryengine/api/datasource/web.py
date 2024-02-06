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

from typing import Dict

from fastapi import APIRouter, Depends

from queryengine import dependencies
from queryengine.api.datasource.response import DataSourceDTO
from queryengine.core.app.datasource import AppRepository
from queryengine.core.datasource.repository import DataSourceRepository
from queryengine.core.kpi.repository import KpiRepository
from queryengine.logging.router import LoggingRoute

router = APIRouter(route_class=LoggingRoute)


@router.get("/api/v1/{app_id}/datasources")
def data_sources(app_id: str,
                 app_config_repository: AppRepository = Depends(dependencies.app_config_repository),
                 datasource_repository: DataSourceRepository = Depends(dependencies.datasource_repository),
                 kpi_repository: KpiRepository = Depends(dependencies.kpi_repository),
                 ) -> Dict[str, DataSourceDTO]:
    app_config = app_config_repository.from_app_id(app_id)
    datasources = datasource_repository.all_daily_data_sources(app_config).values()
    return {datasource.id: DataSourceDTO.from_domain_model(
        datasource, list(kpi_repository.load_by_datasource_id(app_config, datasource.id).values()))
        for datasource in datasources}


@router.get("/api/v1/{app_id}/event-datasources")
def event_data_sources(app_id: str,
                       app_config_repository: AppRepository = Depends(dependencies.app_config_repository),
                       datasource_repository: DataSourceRepository = Depends(dependencies.datasource_repository),
                       kpi_repository: KpiRepository = Depends(dependencies.kpi_repository),
                       ) -> Dict[str, DataSourceDTO]:
    app_config = app_config_repository.from_app_id(app_id)
    datasources = datasource_repository.all_event_data_sources(app_config).values()
    return {datasource.id: DataSourceDTO.from_domain_model(
        datasource, list(kpi_repository.load_by_datasource_id(app_config, datasource.id).values()))
        for datasource in datasources}


@router.get("/api/v1/{app_id}/datasources-freshness")
def get_datasources_freshness(app_id: str,
                              app_config_repository: AppRepository = Depends(dependencies.app_config_repository),
                              datasource_repository: DataSourceRepository = Depends(dependencies.datasource_repository),
                              ):
    app_config = app_config_repository.from_app_id(app_id)
    all_daily_datasources = datasource_repository.all_daily_data_sources(app_config)
    return {
        datasource_name: datasource.data_availability.date_to.date()
        for datasource_name, datasource in all_daily_datasources.items()
        if datasource.data_availability
    }
