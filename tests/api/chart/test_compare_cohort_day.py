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

from queryengine.api.chart.request import ChartQueryDTO
from queryengine.api.chart.response import CohortChartPointDTO, LinePointDTO
from queryengine.api.chart.service import ChartQueryService
from queryengine.core.dateinterval import DateInterval


def test_cohort_day_compare_in_past_with_same_length_period(app_config_repository, datasource_repository,
                                                            kpi_repository, warehouse):
    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository, warehouse).execute(
        'app', ChartQueryDTO(
            page_id='page_id',
            request_id='request_id',
            kpi_id='user_history.cohort_single',
            time_grain=None,
            date_interval=DateInterval(date(2022, 5, 1), date(2022, 5, 3)),
            compare_date_interval=DateInterval(date(2022, 4, 1), date(2022, 4, 3)),
            column_filters=[],
            column_group_bys=[],
            x_axis_column_id='user_history.cohort_day'
        ))

    assert chart_result.chart_points == [
        CohortChartPointDTO(x_axis_value=0, values=[LinePointDTO(group_by_key=[], value=121.0)]),
        CohortChartPointDTO(x_axis_value=1, values=[LinePointDTO(group_by_key=[], value=122.0)]),
        CohortChartPointDTO(x_axis_value=2, values=[LinePointDTO(group_by_key=[], value=123.0)])
    ]

    assert chart_result.compare_period_chart_points == [
        CohortChartPointDTO(x_axis_value=0, values=[LinePointDTO(group_by_key=[], value=91.0)]),
        CohortChartPointDTO(x_axis_value=1, values=[LinePointDTO(group_by_key=[], value=92.0)]),
        CohortChartPointDTO(x_axis_value=2, values=[LinePointDTO(group_by_key=[], value=93.0)])
    ]

    assert chart_result.chart_total == []
    assert chart_result.compare_period_chart_total == []


def test_cohort_day_compare_in_past_larger_length(app_config_repository, datasource_repository, kpi_repository,
                                                  warehouse):
    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository, warehouse).execute(
        'app', ChartQueryDTO(
            page_id='page_id',
            request_id='request_id',
            kpi_id='user_history.cohort_single',
            time_grain=None,
            date_interval=DateInterval(date(2022, 5, 1), date(2022, 5, 3)),
            compare_date_interval=DateInterval(date(2022, 4, 1), date(2022, 4, 4)),
            column_filters=[],
            column_group_bys=[],
            x_axis_column_id='user_history.cohort_day'
        ))

    assert chart_result.chart_points == [
        CohortChartPointDTO(x_axis_value=0, values=[LinePointDTO(group_by_key=[], value=121.0)]),
        CohortChartPointDTO(x_axis_value=1, values=[LinePointDTO(group_by_key=[], value=122.0)]),
        CohortChartPointDTO(x_axis_value=2, values=[LinePointDTO(group_by_key=[], value=123.0)])
    ]

    assert chart_result.compare_period_chart_points == [
        CohortChartPointDTO(x_axis_value=0, values=[LinePointDTO(group_by_key=[], value=91.0)]),
        CohortChartPointDTO(x_axis_value=1, values=[LinePointDTO(group_by_key=[], value=92.0)]),
        CohortChartPointDTO(x_axis_value=2, values=[LinePointDTO(group_by_key=[], value=93.0)]),
        CohortChartPointDTO(x_axis_value=3, values=[LinePointDTO(group_by_key=[], value=94.0)])
    ]

    assert chart_result.chart_total == []
    assert chart_result.compare_period_chart_total == []


def test_cohort_day_compare_in_past_smaller_length(app_config_repository, datasource_repository, kpi_repository,
                                                   warehouse):
    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository, warehouse).execute(
        'app', ChartQueryDTO(
            page_id='page_id',
            request_id='request_id',
            kpi_id='user_history.cohort_single',
            time_grain=None,
            date_interval=DateInterval(date(2022, 5, 1), date(2022, 5, 3)),
            compare_date_interval=DateInterval(date(2022, 4, 2), date(2022, 4, 3)),
            column_filters=[],
            column_group_bys=[],
            x_axis_column_id='user_history.cohort_day'
        ))

    assert chart_result.chart_points == [
        CohortChartPointDTO(x_axis_value=0, values=[LinePointDTO(group_by_key=[], value=121.0)]),
        CohortChartPointDTO(x_axis_value=1, values=[LinePointDTO(group_by_key=[], value=122.0)]),
        CohortChartPointDTO(x_axis_value=2, values=[LinePointDTO(group_by_key=[], value=123.0)])
    ]

    assert chart_result.compare_period_chart_points == [
        CohortChartPointDTO(x_axis_value=0, values=[LinePointDTO(group_by_key=[], value=92.0)]),
        CohortChartPointDTO(x_axis_value=1, values=[LinePointDTO(group_by_key=[], value=93.0)]),
    ]

    assert chart_result.chart_total == []
    assert chart_result.compare_period_chart_total == []


def test_missing_data_inside_bounds(
        app_config_repository, datasource_repository, kpi_repository, missing_data_warehouse,
):
    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     missing_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.cohort_single',
        time_grain=None,
        date_interval=DateInterval(date(2022, 5, 1), date(2022, 5, 2)),
        compare_date_interval=DateInterval(date(2022, 4, 1), date(2022, 4, 2)),
        column_filters=[],
        column_group_bys=[],
        x_axis_column_id='user_history.cohort_day'
    ))

    assert chart_result.chart_points == []
    assert chart_result.compare_period_chart_points == []
    assert chart_result.chart_total_overall == None
    assert chart_result.compare_period_chart_total_overall == None
