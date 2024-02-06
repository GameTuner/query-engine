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

from datetime import datetime, date

import pytest
import pytz
from pandas import DataFrame

from queryengine.api.chart.request import ChartQueryDTO
from queryengine.api.chart.response import DateChartPointDTO, LinePointDTO, CohortChartPointDTO
from queryengine.api.chart.service import ChartQueryService
from queryengine.core import constants
from queryengine.core.dateinterval import DateInterval
from queryengine.core.tabular_data_result import TabularDataResult, TabularDataResults


def test_complex_kpi_returning_different_group_bys_with_date_x_axis(
        app_config_repository, datasource_repository, kpi_repository, queued_data_warehouse,
):
    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [
                    datetime(2022, 1, 1).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 1).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 2).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 2).replace(tzinfo=pytz.UTC),
                ],
                'g1': ['a', 'b', 'a', 'b'],
                constants.DATA_COLUMN_ALIAS: [0, 1, 2, 3]
            })),
            'y': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [
                    datetime(2022, 1, 2).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 2).replace(tzinfo=pytz.UTC),
                ],
                'g1': ['b', 'c'],
                constants.DATA_COLUMN_ALIAS: [4, 5]
            }))
        })
    )
    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     queued_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.complex',
        date_interval=DateInterval(date(2022, 1, 1), date(2022, 1, 2)),
        column_filters=[],
        column_group_bys=['user_history.up_string'],
        x_axis_column_id='user_history.date_'
    ))

    assert chart_result.chart_points == [
        DateChartPointDTO(x_axis_value=date(2022, 1, 2), values=[
            LinePointDTO(group_by_key=['b'], value=7)
        ]),
    ]


def test_complex_kpi_returning_different_group_bys_with_cohort_x_axis(
        app_config_repository, datasource_repository, kpi_repository, queued_data_warehouse,
):
    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [0, 0, 1, 1],
                'g1': ['a', 'b', 'a', 'b'],
                constants.DATA_COLUMN_ALIAS: [0, 1, 2, 3]
            })),
            'y': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [0, 0],
                'g1': ['b', 'c'],
                constants.DATA_COLUMN_ALIAS: [4, 5]
            }))
        })
    )
    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     queued_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.complex',
        date_interval=DateInterval(date(2022, 1, 1), date(2022, 1, 2)),
        column_filters=[],
        column_group_bys=['user_history.up_string'],
        x_axis_column_id='user_history.cohort_day'
    ))

    assert chart_result.chart_points == [
        CohortChartPointDTO(x_axis_value=0, values=[
            LinePointDTO(group_by_key=['b'], value=5.0)
        ]),
    ]


@pytest.mark.parametrize("kpi", [
    'user_history.complex_add',
    'user_history.complex_sub',
    'user_history.complex_mul',
    'user_history.complex_div',
])
def test_missing_data_with_complex_date_kpis(
        app_config_repository, datasource_repository, kpi_repository, missing_data_warehouse, kpi
):
    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     missing_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id=kpi,
        date_interval=DateInterval(date(2022, 1, 1), date(2022, 1, 2)),
        column_filters=[],
        column_group_bys=[],
        x_axis_column_id='user_history.date_'
    ))

    assert chart_result.chart_points == []


@pytest.mark.parametrize("kpi", [
    'user_history.complex_add',
    'user_history.complex_sub',
    'user_history.complex_mul',
    'user_history.complex_div',
])
def test_missing_data_with_complex_cohort_kpis(
        app_config_repository, datasource_repository, kpi_repository, missing_data_warehouse, kpi
):
    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     missing_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id=kpi,
        date_interval=DateInterval(date(2022, 1, 1), date(2022, 1, 2)),
        column_filters=[],
        column_group_bys=[],
        x_axis_column_id='user_history.cohort_day'
    ))

    assert chart_result.chart_points == []