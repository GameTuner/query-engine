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
from typing import List

from queryengine.core.datasource.datasource import DataType, DataSource
from queryengine.core.kpi.kpi import Kpi, Unit


@dataclass(frozen=True)
class KpiDTO:
    id: str
    label: str
    is_daily_kpi: bool
    is_cohort_kpi: bool
    category: str
    recommended: bool
    unit: Unit | None

    @classmethod
    def from_domain_model(cls, datasource: DataSource, kpi: Kpi) -> 'KpiDTO':
        return KpiDTO(
            id=f'{datasource.id}.{kpi.id}',
            label=kpi.label,
            is_daily_kpi=kpi.is_daily_kpi(),
            is_cohort_kpi=kpi.is_cohort_kpi(),
            category=kpi.category,
            recommended=kpi.recommended,
            unit=kpi.unit
        )


@dataclass(frozen=True)
class ColumnDTO:
    id: str
    label: str
    description: str | None
    data_type: DataType
    can_filter: bool
    can_group_by: bool
    available_values: List


@dataclass(frozen=True)
class DataSourceDTO:
    id: str
    label: str
    description: str | None
    columns: List[ColumnDTO]
    kpis: List[KpiDTO]

    @classmethod
    def from_domain_model(cls, datasource: DataSource, kpis: List[Kpi]) -> 'DataSourceDTO':
        return DataSourceDTO(
            id=datasource.id,
            label=datasource.label,
            description=datasource.description,
            columns=[ColumnDTO(
                id=f'{datasource.id}.{column.id}',
                label=column.label,
                description=column.description,
                data_type=column.data_type,
                can_filter=column.can_filter,
                can_group_by=column.can_group_by,
                available_values=column.available_values,
            ) for column in datasource.columns_by_id.values() if not column.hidden],
            kpis=[KpiDTO.from_domain_model(datasource, kpi) for kpi in kpis]
        )
