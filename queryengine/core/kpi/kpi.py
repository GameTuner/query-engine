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
from typing import Dict, Union

from queryengine.core import constants


@dataclass
class Rollup:
    rollup_x_axis: str
    rollup_y_axis: str


@dataclass
class Unit:
    symbol: str
    is_prefix: bool

    @staticmethod
    def dollar():
        return Unit('$', True)

    @staticmethod
    def percent():
        return Unit('%', False)

    @staticmethod
    def minute():
        return Unit('min', False)


@dataclass
class WarehouseMetric:
    select_expression: str
    where_expression: str | None
    data_source_table: str


@dataclass
class Kpi:
    id: str
    formula: str
    metrics: Dict[str, WarehouseMetric]
    x_axis: Dict[str, Rollup]
    label: str = None
    category: str = "Game Specific"
    recommended: bool = False
    description: str = ""
    unit: Union[Unit | None] = None

    def __post_init__(self):
        if self.label is None:
            self.label = self.id.replace('_', ' ').title()

    def is_daily_kpi(self):
        return constants.DATE_PARTITION_COLUMN_NAME in self.x_axis

    def is_cohort_kpi(self):
        return constants.COHORT_DAY_COLUMN_NAME in self.x_axis


@dataclass
class KpiReference:
    datasource_id: str
    kpi: Kpi
