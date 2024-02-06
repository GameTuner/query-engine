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

import pytz
from pandas import DataFrame

from queryengine.api.chart.request import ChartQueryDTO
from queryengine.api.chart.response import DateChartPointDTO, LinePointDTO
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
                constants.X_AXIS_COLUMN_ALIAS: [
                    datetime(2022, 1, 10).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 11).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 12).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 13).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 14).replace(tzinfo=pytz.UTC),
                ],
                constants.DATA_COLUMN_ALIAS: [0, 1, 2, 3, 4]
            })),
        })
    )

    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     queued_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.daily_single',
        date_interval=DateInterval(date(2022, 1, 10), date(2022, 1, 14)),
        x_axis_column_id='user_history.date_',
    ))

    assert chart_result.chart_points == [
        DateChartPointDTO(x_axis_value=date(2022, 1, 11), values=[LinePointDTO(group_by_key=[], value=1)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 12), values=[LinePointDTO(group_by_key=[], value=2)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 13), values=[LinePointDTO(group_by_key=[], value=3)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 14), values=[LinePointDTO(group_by_key=[], value=4)]),
    ]
    assert chart_result.chart_total == [LinePointDTO(group_by_key=[], value=10)]
    assert chart_result.chart_total_overall == 10


def test_trim_zeros_on_end(
        app_config_repository, datasource_repository, kpi_repository, queued_data_warehouse,
):
    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [
                    datetime(2022, 1, 10).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 11).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 12).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 13).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 14).replace(tzinfo=pytz.UTC),
                ],
                constants.DATA_COLUMN_ALIAS: [1, 2, 3, 4, 0]
            })),
        })
    )

    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     queued_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.daily_single',
        date_interval=DateInterval(date(2022, 1, 10), date(2022, 1, 14)),
        x_axis_column_id='user_history.date_',
    ))

    assert chart_result.chart_points == [
        DateChartPointDTO(x_axis_value=date(2022, 1, 10), values=[LinePointDTO(group_by_key=[], value=1)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 11), values=[LinePointDTO(group_by_key=[], value=2)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 12), values=[LinePointDTO(group_by_key=[], value=3)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 13), values=[LinePointDTO(group_by_key=[], value=4)]),
    ]
    assert chart_result.chart_total == [LinePointDTO(group_by_key=[], value=10)]
    assert chart_result.chart_total_overall == 10


def test_trim_zeros_on_start_and_end(
        app_config_repository, datasource_repository, kpi_repository, queued_data_warehouse,
):
    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [
                    datetime(2022, 1, 10).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 11).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 12).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 13).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 14).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 15).replace(tzinfo=pytz.UTC),
                ],
                constants.DATA_COLUMN_ALIAS: [0, 1, 2, 3, 4, 0]
            })),
        })
    )

    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     queued_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.daily_single',
        date_interval=DateInterval(date(2022, 1, 10), date(2022, 1, 15)),
        x_axis_column_id='user_history.date_',
    ))

    assert chart_result.chart_points == [
        DateChartPointDTO(x_axis_value=date(2022, 1, 11), values=[LinePointDTO(group_by_key=[], value=1)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 12), values=[LinePointDTO(group_by_key=[], value=2)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 13), values=[LinePointDTO(group_by_key=[], value=3)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 14), values=[LinePointDTO(group_by_key=[], value=4)]),
    ]
    assert chart_result.chart_total == [LinePointDTO(group_by_key=[], value=10)]
    assert chart_result.chart_total_overall == 10


def test_trim_zeros_when_zero_is_in_middle(
        app_config_repository, datasource_repository, kpi_repository, queued_data_warehouse,
):
    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [
                    datetime(2022, 1, 10).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 11).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 12).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 13).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 14).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 15).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 16).replace(tzinfo=pytz.UTC),
                ],
                constants.DATA_COLUMN_ALIAS: [0, 1, 2, 0, 3, 4, 0]
            })),
        })
    )

    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     queued_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.daily_single',
        date_interval=DateInterval(date(2022, 1, 10), date(2022, 1, 16)),
        x_axis_column_id='user_history.date_',
    ))

    assert chart_result.chart_points == [
        DateChartPointDTO(x_axis_value=date(2022, 1, 11), values=[LinePointDTO(group_by_key=[], value=1)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 12), values=[LinePointDTO(group_by_key=[], value=2)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 13), values=[LinePointDTO(group_by_key=[], value=0)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 14), values=[LinePointDTO(group_by_key=[], value=3)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 15), values=[LinePointDTO(group_by_key=[], value=4)]),
    ]
    assert chart_result.chart_total == [LinePointDTO(group_by_key=[], value=10)]
    assert chart_result.chart_total_overall == 10


def test_trim_zeros_total_avg(
        app_config_repository, datasource_repository, kpi_repository, queued_data_warehouse,
):
    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [
                    datetime(2022, 1, 10).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 11).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 12).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 13).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 14).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 15).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 16).replace(tzinfo=pytz.UTC),
                ],
                constants.DATA_COLUMN_ALIAS: [0, 1, 2, 0, 3, 4, 0]
            })),
        })
    )

    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     queued_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.daily_single_x_avg',
        date_interval=DateInterval(date(2022, 1, 10), date(2022, 1, 16)),
        x_axis_column_id='user_history.date_',
    ))

    assert chart_result.chart_points == [
        DateChartPointDTO(x_axis_value=date(2022, 1, 11), values=[LinePointDTO(group_by_key=[], value=1)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 12), values=[LinePointDTO(group_by_key=[], value=2)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 13), values=[LinePointDTO(group_by_key=[], value=0)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 14), values=[LinePointDTO(group_by_key=[], value=3)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 15), values=[LinePointDTO(group_by_key=[], value=4)]),
    ]
    assert chart_result.chart_total == [LinePointDTO(group_by_key=[], value=2.0)]
    assert chart_result.chart_total_overall == 2.0


def test_trim_zeros_width_compare_period(
        app_config_repository, datasource_repository, kpi_repository, queued_data_warehouse,
):
    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [
                    datetime(2022, 1, 10).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 11).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 12).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 13).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 14).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 15).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 16).replace(tzinfo=pytz.UTC),
                ],
                constants.DATA_COLUMN_ALIAS: [0, 1, 2, 0, 3, 4, 0]
            })),
        })
    )

    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [
                    datetime(2022, 1, 3).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 4).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 5).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 6).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 7).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 8).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 9).replace(tzinfo=pytz.UTC),
                ],
                constants.DATA_COLUMN_ALIAS: [0, 0, 5, 0, 1, 6, 0]
            })),
        })
    )

    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     queued_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.daily_single',
        date_interval=DateInterval(date(2022, 1, 10), date(2022, 1, 16)),
        compare_date_interval=DateInterval(date(2022, 1, 3), date(2022, 1, 9)),
        x_axis_column_id='user_history.date_',
    ))

    assert chart_result.chart_points == [
        DateChartPointDTO(x_axis_value=date(2022, 1, 11), values=[LinePointDTO(group_by_key=[], value=1)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 12), values=[LinePointDTO(group_by_key=[], value=2)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 13), values=[LinePointDTO(group_by_key=[], value=0)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 14), values=[LinePointDTO(group_by_key=[], value=3)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 15), values=[LinePointDTO(group_by_key=[], value=4)]),
    ]
    assert chart_result.compare_period_chart_points == [
        DateChartPointDTO(x_axis_value=date(2022, 1, 12), values=[LinePointDTO(group_by_key=[], value=5)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 13), values=[LinePointDTO(group_by_key=[], value=0)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 14), values=[LinePointDTO(group_by_key=[], value=1)]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 15), values=[LinePointDTO(group_by_key=[], value=6)]),
    ]
    assert chart_result.chart_total == [LinePointDTO(group_by_key=[], value=10)]
    assert chart_result.chart_total_overall == 10
    assert chart_result.compare_period_chart_total == [LinePointDTO(group_by_key=[], value=12)]
    assert chart_result.compare_period_chart_total_overall == 12
