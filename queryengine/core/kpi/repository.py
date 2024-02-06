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
from typing import Dict, List

from queryengine.core.app.app import App
from queryengine.core.datasource.datasource import DataType
from queryengine.core.datasource.repository import DataSourceRepository
from queryengine.core.kpi.kpi import Kpi, KpiReference, WarehouseMetric, Rollup, Unit
from queryengine.core.kpi.kpi_definitions import demoapp, common


class KpiRepository(ABC):
    @abstractmethod
    def load_by_datasource_id(self, app: App, datasource_id: str) -> Dict[str, Kpi]:
        pass

    def load_by_full_kpi_id(self, app: App, full_kpi_id: str) -> KpiReference:
        datasource_id, kpi_id = full_kpi_id.split(".", 1)
        return KpiReference(datasource_id, self.load_by_datasource_id(app, datasource_id)[kpi_id])


class InMemoryKpiRepository(KpiRepository):
    def __init__(self, datasource_repository: DataSourceRepository):
        self.datasource_repository = datasource_repository

    def _game_specific_kpis(self, app_id: str) -> List[Kpi]:
        return {
            'demoapp': demoapp.KPIS
        }.get(app_id, [])

    @staticmethod
    def add_kpi_with_num_values(kpis: List[Kpi], datasource: str):
        kpis.extend([Kpi(f'sum_{column.id}', label=f'Sum of {column.label}', formula="x",
                     metrics={'x': WarehouseMetric(select_expression=f'SUM({{{column.id}}})', where_expression=None,
                                                   data_source_table=datasource.table_name)},
                     x_axis={'date_': Rollup('SUM', 'SUM')}),
                 Kpi(f'avg_{column.id}', label=f'Average of {column.label}', formula="x",
                     metrics={'x': WarehouseMetric(select_expression=f'AVG({{{column.id}}})', where_expression=None,
                                                   data_source_table=datasource.table_name)},
                     x_axis={'date_': Rollup('SUM', 'SUM')}),
                 ]
                for column in datasource.columns_by_id.values() if
                column.data_type in [DataType.number, DataType.integer])

    @staticmethod
    def add_kpi_with_string_values(kpis: List[Kpi], datasource: str):
            kpis.extend([
                [Kpi(f'cnt_uniq_{column.id}', label=f'Count Distinct of {column.label}', formula="x",
                     metrics={'x': WarehouseMetric(select_expression=f'COUNT(DISTINCT {{{column.id}}})',
                                                   where_expression=None, 
                                                   data_source_table=datasource.table_name)},
                     x_axis={'date_': Rollup('SUM', 'SUM')})]
                for column in datasource.columns_by_id.values() if column.data_type in [DataType.string]])        

    @staticmethod
    def add_top_level_kpi(kpis: List[Kpi], datasource: str, datasource_id: str):
        kpis.extend([[
                Kpi(f'cnt', label=f'Event Count', formula="x",
                    metrics={'x': WarehouseMetric(select_expression=f'COUNT(*)', where_expression=None,
                                                  data_source_table=datasource.table_name)},
                    x_axis={'date_': Rollup('SUM', 'SUM')})]])
        kpis.extend([[Kpi('pct_offline_events', unit=Unit.percent(), label='Percentage of Offline Events', formula="offline_events * 100 / total_events",
                    metrics={'offline_events': WarehouseMetric(select_expression='COUNT(*)', 
                                                                where_expression='{params.is_online} = TRUE',
                                                                data_source_table=datasource.table_name),
                                'total_events': WarehouseMetric(select_expression='COUNT(*)', 
                                                                where_expression=None, 
                                                                data_source_table=datasource.table_name)},
                     x_axis={'date_': Rollup('SUM', 'SUM')})] 
                for column in datasource.columns_by_id.values() if 
                 datasource_id == "events_ctx_event_context"])   

    def _from_event_data_sources(self, app: App) -> Dict[str, Dict[str, Kpi]]:
        datasources = {}
        for datasource_id, datasource in self.datasource_repository.all_event_data_sources(app).items():
            kpis = []
            InMemoryKpiRepository.add_kpi_with_num_values(kpis, datasource)
            InMemoryKpiRepository.add_kpi_with_string_values(kpis, datasource)
            InMemoryKpiRepository.add_top_level_kpi(kpis, datasource, datasource_id)
            kpi_dict = {}
            for column_kpis in kpis:
                for kpi in column_kpis:
                    kpi_dict[kpi.id] = kpi
            datasources[datasource_id] = kpi_dict
        return datasources

    def _from_daily_data_sources(self, app: App) -> Dict[str, Dict[str, Kpi]]:
        by_datasource = {
            'user_history': {kpi.id: kpi for kpi in common.KPIS + self._game_specific_kpis(app.app_id())},
        }
        if 'appsflyer' in app.app_config.datasources:
            by_datasource['appsflyer'] = {kpi.id: kpi for kpi in common.APPSFLYER_KPIS}
        return by_datasource

    def load_by_datasource_id(self, app: App, datasource_id: str) -> Dict[str, Kpi]:
        return (self._from_daily_data_sources(app) | self._from_event_data_sources(app))[datasource_id]
