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

import pytest

from queryengine.api.chart.request import ChartQueryDTO
from queryengine.api.chart.response import DateChartPointDTO, LinePointDTO
from queryengine.api.chart.service import ChartQueryService
from queryengine.core.dateinterval import DateInterval


def mock_value(dt: date):
    return ((dt - date(2022, 1, 1)).days + 1) * 1.0


@pytest.mark.parametrize("test_case,date_interval,compare_interval,expected_chart_points,expected_compare_chart_points",
                         [
                             ###################################
                             # No clamp
                             ###################################
                             (
                                     "Same length period, compare is before date interval",
                                     DateInterval(date(2022, 1, 10), date(2022, 1, 12)),
                                     DateInterval(date(2022, 1, 3), date(2022, 1, 5)),

                                     [
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 10), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 10)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 11), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 11)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 12), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 12)))]),
                                     ],
                                     [
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 10), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 3)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 11), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 4)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 12), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 5)))]),
                                     ]
                             ),
                             (
                                     "Same length period, compare is after date interval start",
                                     DateInterval(date(2022, 1, 10), date(2022, 1, 12)),
                                     DateInterval(date(2022, 1, 13), date(2022, 1, 15)),

                                     [
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 10), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 10)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 11), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 11)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 12), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 12)))]),
                                     ],
                                     [
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 10), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 13)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 11), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 14)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 12), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 15)))]),
                                     ]
                             ),
                             (
                                     "Compare interval is smaller",
                                     DateInterval(date(2022, 1, 10), date(2022, 1, 12)),
                                     DateInterval(date(2022, 1, 3), date(2022, 1, 4)),

                                     [
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 10), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 10)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 11), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 11)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 12), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 12)))]),
                                     ],
                                     [
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 11), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 3)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 12), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 4)))]),
                                     ]
                             ),
                             (
                                     "Compare interval is bigger",
                                     DateInterval(date(2022, 1, 10), date(2022, 1, 12)),
                                     DateInterval(date(2022, 1, 3), date(2022, 1, 6)),

                                     [
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 10), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 10)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 11), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 11)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 12), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 12)))]),
                                     ],
                                     [
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 10), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 4)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 11), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 5)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 1, 12), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 6)))]),
                                     ]
                             ),
                             ###################################
                             # Clamp
                             ###################################
                             (
                                     "Only date interval is clamped to the right",
                                     DateInterval(date(2022, 12, 30), date(2023, 1, 3)),
                                     DateInterval(date(2022, 12, 27), date(2022, 12, 31)),

                                     [
                                         DateChartPointDTO(x_axis_value=date(2022, 12, 30), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 12, 30)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 12, 31), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 12, 31)))]),
                                         DateChartPointDTO(x_axis_value=date(2023, 1, 1), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2023, 1, 1)))]),
                                     ],
                                     [
                                         DateChartPointDTO(x_axis_value=date(2022, 12, 30), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 12, 27)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 12, 31), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 12, 28)))]),
                                         DateChartPointDTO(x_axis_value=date(2023, 1, 1), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 12, 29)))]),
                                         DateChartPointDTO(x_axis_value=date(2023, 1, 2), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 12, 30)))]),
                                         DateChartPointDTO(x_axis_value=date(2023, 1, 3), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 12, 31)))]),
                                     ]
                             ),
                             (
                                     "Both are clamped to the right",
                                     DateInterval(date(2022, 12, 30), date(2023, 1, 3)),
                                     DateInterval(date(2022, 12, 31), date(2023, 1, 2)),

                                     [
                                         DateChartPointDTO(x_axis_value=date(2022, 12, 30), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 12, 30)))]),
                                         DateChartPointDTO(x_axis_value=date(2022, 12, 31), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 12, 31)))]),
                                         DateChartPointDTO(x_axis_value=date(2023, 1, 1), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2023, 1, 1)))]),
                                     ],
                                     [
                                         DateChartPointDTO(x_axis_value=date(2023, 1, 1), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2022, 12, 31)))]),
                                         DateChartPointDTO(x_axis_value=date(2023, 1, 2), values=[
                                             LinePointDTO(group_by_key=[], value=mock_value(date(2023, 1, 1)))]),
                                     ]
                             ),
                         ])
def test_compared_result(
        test_case,
        app_config_repository, datasource_repository, kpi_repository, warehouse,
        date_interval, compare_interval,
        expected_chart_points, expected_compare_chart_points
):
    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository, warehouse).execute(
        'app', ChartQueryDTO(
            page_id='page_id',
            request_id='request_id',
            kpi_id='user_history.daily_single',
            date_interval=date_interval,
            compare_date_interval=compare_interval,
            column_filters=[],
            column_group_bys=[],
            x_axis_column_id='user_history.date_'
        ))

    assert chart_result.chart_points == expected_chart_points
    assert chart_result.compare_period_chart_points == expected_compare_chart_points


@pytest.mark.parametrize(
    "test_case,date_interval,compare_interval,expected_date,expected_compare,expected_date_total,expected_compare_total",
    [
        (
                "All points should be used for totals",
                DateInterval(date(2022, 1, 10), date(2022, 1, 12)),
                DateInterval(date(2022, 1, 1), date(2022, 1, 2)),

                [
                    DateChartPointDTO(x_axis_value=date(2022, 1, 10),
                                      values=[LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 10)))]),
                    DateChartPointDTO(x_axis_value=date(2022, 1, 11),
                                      values=[LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 11)))]),
                    DateChartPointDTO(x_axis_value=date(2022, 1, 12),
                                      values=[LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 12)))]),
                ],
                [
                    DateChartPointDTO(x_axis_value=date(2022, 1, 11),
                                      values=[LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 1)))]),
                    DateChartPointDTO(x_axis_value=date(2022, 1, 12),
                                      values=[LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 2)))]),
                ],

                mock_value(date(2022, 1, 10)) + mock_value(date(2022, 1, 11)) + mock_value(date(2022, 1, 12)),
                mock_value(date(2022, 1, 1)) + mock_value(date(2022, 1, 2)),
        ),
    ])
def test_compared_totals(
        test_case,
        app_config_repository, datasource_repository, kpi_repository, warehouse,
        date_interval, compare_interval,
        expected_date, expected_compare,
        expected_date_total, expected_compare_total
):
    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository, warehouse).execute(
        'app', ChartQueryDTO(
            page_id='page_id',
            request_id='request_id',
            kpi_id='user_history.daily_single',
            date_interval=date_interval,
            compare_date_interval=compare_interval,
            column_filters=[],
            column_group_bys=[],
            x_axis_column_id='user_history.date_'
        ))

    assert chart_result.chart_points == expected_date
    assert chart_result.compare_period_chart_points == expected_compare
    assert chart_result.chart_total == [LinePointDTO(group_by_key=[], value=expected_date_total)]
    assert chart_result.compare_period_chart_total == [LinePointDTO(group_by_key=[], value=expected_compare_total)]
    assert chart_result.chart_total_overall == expected_date_total
    assert chart_result.compare_period_chart_total_overall == expected_compare_total


@pytest.mark.parametrize(
    "test_case,date_interval,compare_interval,expected_date,expected_compare,expected_date_total,expected_compare_total",
    [
        (
                "No compare, date points in past",
                DateInterval(date(2021, 1, 1), date(2021, 1, 2)),
                None,

                None,
                None,

                None,
                None,
        ),
        (
                "No compare, date points in future",
                DateInterval(date(2031, 1, 1), date(2031, 1, 2)),
                None,

                None,
                None,

                None,
                None,
        ),
        (
                "Compare points in past",
                DateInterval(date(2022, 1, 1), date(2022, 1, 2)),
                DateInterval(date(2020, 1, 1), date(2020, 1, 2)),

                [
                    DateChartPointDTO(x_axis_value=date(2022, 1, 1),
                                      values=[LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 1)))]),
                    DateChartPointDTO(x_axis_value=date(2022, 1, 2),
                                      values=[LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 2)))]),
                ],

                None,

                mock_value(date(2022, 1, 1)) + mock_value(date(2022, 1, 2)),
                None,
        ),
        (
                "Compare points in future",
                DateInterval(date(2022, 1, 1), date(2022, 1, 2)),
                DateInterval(date(2030, 1, 1), date(2030, 1, 2)),

                [
                    DateChartPointDTO(x_axis_value=date(2022, 1, 1),
                                      values=[LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 1)))]),
                    DateChartPointDTO(x_axis_value=date(2022, 1, 2),
                                      values=[LinePointDTO(group_by_key=[], value=mock_value(date(2022, 1, 2)))]),
                ],

                None,

                mock_value(date(2022, 1, 1)) + mock_value(date(2022, 1, 2)),
                None,
        ),
    ])
def test_missing_data_outside_bounds(
        test_case,
        app_config_repository, datasource_repository, kpi_repository, warehouse,
        date_interval, compare_interval,
        expected_date, expected_compare,
        expected_date_total, expected_compare_total
):
    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository, warehouse).execute(
        'app', ChartQueryDTO(
            page_id='page_id',
            request_id='request_id',
            kpi_id='user_history.daily_single',
            date_interval=date_interval,
            compare_date_interval=compare_interval,
            column_filters=[],
            column_group_bys=[],
            x_axis_column_id='user_history.date_'
        ))

    if expected_date is None:
        assert chart_result.chart_points == []
    else:
        assert chart_result.chart_points == expected_date

    if expected_compare is None:
        assert chart_result.compare_period_chart_points == []
    else:
        assert chart_result.compare_period_chart_points == expected_compare

    if expected_date_total is None:
        assert chart_result.chart_total_overall is None
    else:
        assert chart_result.chart_total_overall == expected_date_total

    if expected_compare_total is None:
        assert chart_result.compare_period_chart_total_overall is None
    else:
        assert chart_result.compare_period_chart_total_overall == expected_compare_total


def test_missing_data_inside_bounds(
        app_config_repository, datasource_repository, kpi_repository, missing_data_warehouse,
):
    chart_result = ChartQueryService(app_config_repository, datasource_repository, kpi_repository,
                                     missing_data_warehouse).execute('app', ChartQueryDTO(
        page_id='page_id',
        request_id='request_id',
        kpi_id='user_history.daily_single',
        date_interval=DateInterval(date(2022, 5, 1), date(2022, 5, 2)),
        compare_date_interval=DateInterval(date(2022, 3, 1), date(2022, 3, 2)),
        column_filters=[],
        column_group_bys=[],
        x_axis_column_id='user_history.date_'
    ))

    assert chart_result.chart_points == []
    assert chart_result.compare_period_chart_points == []
    assert chart_result.chart_total_overall == 0
    assert chart_result.compare_period_chart_total_overall == 0
