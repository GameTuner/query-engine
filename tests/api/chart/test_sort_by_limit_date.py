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


def test_group_by_with_sort_by_returning_correct_data_for_date_x_axis(
        app_config_repository, datasource_repository, kpi_repository, queued_data_warehouse,
):
    # main data
    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [
                    datetime(2022, 1, 10).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 10).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 10).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 11).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 11).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 11).replace(tzinfo=pytz.UTC),
                ],
                'g1': ['a', 'b', 'c', 'a', 'b', 'c'],
                constants.DATA_COLUMN_ALIAS: [0, 1, 2, 3, 4, 5]
            })),
        })
    )

    # sort by data
    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [
                    datetime(2022, 1, 1).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 1).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 1).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 2).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 2).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 2).replace(tzinfo=pytz.UTC),
                ],
                'g1': ['b', 'c', 'd', 'b', 'c', 'd'],
                constants.DATA_COLUMN_ALIAS: [0, 1, 2, 3, 4, 5]
            })),
        })
    )
    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     queued_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.daily_single',
        date_interval=DateInterval(date(2022, 1, 10), date(2022, 1, 11)),
        column_filters=[],
        group_by_limit=3,
        sort_by_kpi_id='user_history.daily_single_optimized',
        column_group_bys=['user_history.up_string'],
        x_axis_column_id='user_history.date_'
    ))

    assert chart_result.chart_points == [
        DateChartPointDTO(x_axis_value=date(2022, 1, 10), values=[
            LinePointDTO(group_by_key=['b'], value=1),
            LinePointDTO(group_by_key=['c'], value=2)
        ]),
        DateChartPointDTO(x_axis_value=date(2022, 1, 11), values=[
            LinePointDTO(group_by_key=['b'], value=4),
            LinePointDTO(group_by_key=['c'], value=5)
        ]),
    ]
    assert chart_result.chart_total == [
        LinePointDTO(group_by_key=['b'], value=5),
        LinePointDTO(group_by_key=['c'], value=7)
    ]
    assert chart_result.chart_total_overall == 12


def test_group_by_disjoint_data(
        app_config_repository, datasource_repository, kpi_repository, queued_data_warehouse,
):
    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [
                    datetime(2022, 1, 10).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 10).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 11).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 11).replace(tzinfo=pytz.UTC),
                ],
                'g1': ['a', 'b', 'a', 'b'],
                constants.DATA_COLUMN_ALIAS: [0, 1, 2, 3]
            })),
        })
    )

    queued_data_warehouse.enqueue_data(
        TabularDataResults({
            'x': TabularDataResult(DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [
                    datetime(2022, 1, 1).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 1).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 2).replace(tzinfo=pytz.UTC),
                    datetime(2022, 1, 2).replace(tzinfo=pytz.UTC),
                ],
                'g1': ['c', 'd', 'c', 'd'],
                constants.DATA_COLUMN_ALIAS: [0, 1, 2, 3]
            })),
        })
    )
    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     queued_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.daily_single',
        date_interval=DateInterval(date(2022, 1, 10), date(2022, 1, 11)),
        column_filters=[],
        group_by_limit=2,
        sort_by_kpi_id='user_history.daily_single_optimized',
        column_group_bys=['user_history.up_string'],
        x_axis_column_id='user_history.date_'

    ))

    assert chart_result.chart_points == []
    assert chart_result.chart_total == []
    assert chart_result.chart_total_overall is None
