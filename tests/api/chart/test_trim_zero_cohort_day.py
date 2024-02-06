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

from datetime import date

from pandas import DataFrame

from queryengine.api.chart.request import ChartQueryDTO
from queryengine.api.chart.response import CohortChartPointDTO, LinePointDTO
from queryengine.api.chart.service import ChartQueryService
from queryengine.core import constants
from queryengine.core.dateinterval import DateInterval
from queryengine.core.tabular_data_result import TabularDataResult, TabularDataResults


def test_trim_zeros_on_start(
        app_config_repository, datasource_repository, kpi_repository, queued_data_warehouse,
):
    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [0, 1, 2, 3, 4],
                constants.DATA_COLUMN_ALIAS: [0, 1, 2, 3, 4]
            })),
        })
    )

    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     queued_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.cohort_single',
        date_interval=DateInterval(date(2022, 1, 10), date(2022, 1, 14)),
        group_by_limit=2,
        x_axis_column_id='user_history.cohort_day'
    ))
    assert chart_result.chart_points == [
        CohortChartPointDTO(x_axis_value=1, values=[LinePointDTO(group_by_key=[], value=1)]),
        CohortChartPointDTO(x_axis_value=2, values=[LinePointDTO(group_by_key=[], value=2)]),
        CohortChartPointDTO(x_axis_value=3, values=[LinePointDTO(group_by_key=[], value=3)]),
        CohortChartPointDTO(x_axis_value=4, values=[LinePointDTO(group_by_key=[], value=4)]),
    ]
    assert chart_result.chart_total == []


def test_trim_zeros_on_end(
        app_config_repository, datasource_repository, kpi_repository, queued_data_warehouse,
):
    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [0, 1, 2, 3, 4],
                constants.DATA_COLUMN_ALIAS: [1, 1, 2, 3, 0]
            })),
        })
    )

    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     queued_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.cohort_single',
        date_interval=DateInterval(date(2022, 1, 10), date(2022, 1, 14)),
        group_by_limit=2,
        x_axis_column_id='user_history.cohort_day'
    ))
    assert chart_result.chart_points == [
        CohortChartPointDTO(x_axis_value=0, values=[LinePointDTO(group_by_key=[], value=1)]),
        CohortChartPointDTO(x_axis_value=1, values=[LinePointDTO(group_by_key=[], value=1)]),
        CohortChartPointDTO(x_axis_value=2, values=[LinePointDTO(group_by_key=[], value=2)]),
        CohortChartPointDTO(x_axis_value=3, values=[LinePointDTO(group_by_key=[], value=3)]),
    ]
    assert chart_result.chart_total == []


def test_trim_zeros_on_start_and_end(
        app_config_repository, datasource_repository, kpi_repository, queued_data_warehouse,
):
    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [0, 1, 2, 3, 4],
                constants.DATA_COLUMN_ALIAS: [0, 1, 2, 3, 0]
            })),
        })
    )

    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     queued_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.cohort_single',
        date_interval=DateInterval(date(2022, 1, 10), date(2022, 1, 15)),
        group_by_limit=2,
        x_axis_column_id='user_history.cohort_day'
    ))
    assert chart_result.chart_points == [
        CohortChartPointDTO(x_axis_value=1, values=[LinePointDTO(group_by_key=[], value=1)]),
        CohortChartPointDTO(x_axis_value=2, values=[LinePointDTO(group_by_key=[], value=2)]),
        CohortChartPointDTO(x_axis_value=3, values=[LinePointDTO(group_by_key=[], value=3)]),
    ]
    assert chart_result.chart_total == []


def test_trim_zeros_when_zero_is_in_middle(
        app_config_repository, datasource_repository, kpi_repository, queued_data_warehouse,
):
    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [0, 1, 2, 3, 4],
                constants.DATA_COLUMN_ALIAS: [0, 1, 0, 3, 0]
            })),
        })
    )

    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     queued_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.cohort_single',
        date_interval=DateInterval(date(2022, 1, 10), date(2022, 1, 16)),
        group_by_limit=2,
        x_axis_column_id='user_history.cohort_day'
    ))

    assert chart_result.chart_points == [
        CohortChartPointDTO(x_axis_value=1, values=[LinePointDTO(group_by_key=[], value=1)]),
        CohortChartPointDTO(x_axis_value=2, values=[LinePointDTO(group_by_key=[], value=0)]),
        CohortChartPointDTO(x_axis_value=3, values=[LinePointDTO(group_by_key=[], value=3)]),
    ]
    assert chart_result.chart_total == []
