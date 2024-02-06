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

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, date
from typing import List

import pytz
from pandas import Timestamp

from queryengine.api.chart.internal.domain import ChartQueryResult
from queryengine.core import constants
from queryengine.core.kpi.kpi import Unit
from queryengine.core.tabular_data_result import TabularDataResult


@dataclass(frozen=True)
class LinePointDTO:
    group_by_key: List[str | None]
    value: float


@dataclass(frozen=True)
class DatetimeChartPointDTO:
    x_axis_value: datetime
    values: List[LinePointDTO]


@dataclass(frozen=True)
class DateChartPointDTO:
    x_axis_value: date
    values: List[LinePointDTO]


@dataclass(frozen=True)
class CohortChartPointDTO:
    x_axis_value: int
    values: List[LinePointDTO]


@dataclass(frozen=True)
class UnitDTO:
    symbol: str
    is_prefix: bool

    @classmethod
    def from_domain_model(cls, unit: Unit):
        if not unit:
            return None
        return UnitDTO(symbol=unit.symbol, is_prefix=unit.is_prefix)


@dataclass(frozen=True)
class ChartDataDTO:
    chart_points: List | None
    chart_total: List[LinePointDTO] | None
    chart_total_overall: object | None
    compare_period_chart_points: List | None
    compare_period_chart_total: List[LinePointDTO] | None
    compare_period_chart_total_overall: object | None
    unit: UnitDTO | None

    @classmethod
    def from_chart_results(cls, chart_result: ChartQueryResult) -> 'ChartDataDTO':
        return ChartDataDTO(
            chart_points=ChartDataDTO._parse_chart_result(chart_result.chart_data),
            chart_total=ChartDataDTO._parse_chart_total(chart_result.chart_total),
            chart_total_overall=ChartDataDTO._parse_chart_total_overall(chart_result.chart_total_overall),
            compare_period_chart_points=ChartDataDTO._parse_chart_result(
                chart_result.compare_period_chart_data),
            compare_period_chart_total=ChartDataDTO._parse_chart_total(
                chart_result.compare_period_chart_total),
            compare_period_chart_total_overall=ChartDataDTO._parse_chart_total_overall(
                chart_result.compare_period_chart_total_overall),
            unit=UnitDTO.from_domain_model(chart_result.unit)
        )

    @classmethod
    def _parse_chart_total(cls, chart_result: TabularDataResult) -> List[LinePointDTO]:
        if chart_result is None or chart_result.df.empty:
            return []
        line_points = []
        for row in chart_result.df.to_dict(orient='records'):
            line_points.append(LinePointDTO(
                group_by_key=[str(value) for column_name, value in list(row.items())[1:-1]],
                value=row[constants.DATA_COLUMN_ALIAS]
            ))
        return line_points

    @classmethod
    def _parse_chart_total_overall(cls, chart_result: TabularDataResult):
        if chart_result is None or chart_result.df.empty:
            return None
        return float(chart_result.df[constants.DATA_COLUMN_ALIAS][0])

    @classmethod
    def _parse_chart_result(cls, chart_result: TabularDataResult):
        if chart_result is None or chart_result.df.empty:
            return []
        chart_points = defaultdict(list)

        for row in chart_result.df.to_dict(orient='records'):
            # expects format x_axis, [group_by_1, ..., group_by_n], value
            x_axis_value = row[constants.X_AXIS_COLUMN_ALIAS]
            value = row[constants.DATA_COLUMN_ALIAS]
            group_bys = dict(list(row.items())[1:-1])
            chart_points[x_axis_value].append(LinePointDTO(
                group_by_key=[str(v) for v in group_bys.values()],
                value=value))

        return [ChartDataDTO._parse_chart_point(x_axis_value, lines) for x_axis_value, lines in chart_points.items()]

    @classmethod
    def _parse_chart_point(cls, x_axis_value, lines: List[
        LinePointDTO]) -> DateChartPointDTO | DatetimeChartPointDTO | CohortChartPointDTO:
        if type(x_axis_value) == date:
            return DateChartPointDTO(x_axis_value=x_axis_value, values=lines)
        elif type(x_axis_value) == datetime:
            return DatetimeChartPointDTO(x_axis_value=x_axis_value.replace(tzinfo=pytz.UTC), values=lines)
        elif type(x_axis_value) == Timestamp:
            return DatetimeChartPointDTO(x_axis_value=x_axis_value.to_pydatetime().replace(tzinfo=pytz.UTC),
                                         values=lines)
        elif type(x_axis_value) == int:
            return CohortChartPointDTO(x_axis_value=x_axis_value, values=lines)
        else:
            raise Exception(f'Could not parse chart point, unsupported type: {type(x_axis_value)}')
