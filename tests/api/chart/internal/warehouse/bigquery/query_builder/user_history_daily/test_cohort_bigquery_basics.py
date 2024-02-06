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

from datetime import datetime

from queryengine.api.chart.internal.domain import ChartQuery
from queryengine.api.chart.internal.domain import ColumnFilter
from queryengine.api.chart.internal.warehouse.bigquery.query_builder import query_builder
from queryengine.core import constants
from queryengine.core.datasource.datasource import ColumnReference
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.timegrain import TimeGrain


def test_kpi_id_sql(datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'user_history')
    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['daily_single'],
        time_grain=TimeGrain.day,
        date_interval=DatetimeInterval(date_from=datetime(2022, 9, 1), date_to=datetime(2022, 9, 5)),
        compare_interval=None,
        column_filters=[],
        column_group_bys=[],
        x_axis_column=ColumnReference(datasource, constants.DATE_PARTITION_COLUMN_NAME)
    ).to_warehouse_query()

    assert query_builder.build(query)['x'].sql == """SELECT TIMESTAMP(`app_main.user_history`.`date_`) AS x_axis, SUM(`app_main.user_history`.`up_int`) AS value
FROM `app_main.user_history`
WHERE (`app_main.user_history`.`date_` BETWEEN DATE '2022-09-01' AND DATE '2022-09-05')
GROUP BY x_axis
ORDER BY x_axis"""


def test_kpi_id_single_filter_sql(datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'user_history')
    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['daily_single'],
        column_filters=[
            ColumnFilter(
                column_ref=ColumnReference(datasource, 'up_string'),
                operation='=', value_list=['value']
            )
        ],
        column_group_bys=[],
        time_grain=TimeGrain.day,
        date_interval=DatetimeInterval(date_from=datetime(2022, 9, 1), date_to=datetime(2022, 9, 5)),
        compare_interval=None,
        x_axis_column=ColumnReference(datasource, constants.DATE_PARTITION_COLUMN_NAME)
    ).to_warehouse_query()

    assert query_builder.build(query)['x'].sql == """SELECT TIMESTAMP(`app_main.user_history`.`date_`) AS x_axis, SUM(`app_main.user_history`.`up_int`) AS value
FROM `app_main.user_history`
WHERE (`app_main.user_history`.`date_` BETWEEN DATE '2022-09-01' AND DATE '2022-09-05') AND `app_main.user_history`.`up_string` = 'value'
GROUP BY x_axis
ORDER BY x_axis"""


def test_kpi_id_single_group_by_sql(datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'user_history')
    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['daily_single'],
        column_filters=[],
        column_group_bys=[
            ColumnReference(datasource, 'up_string'),
        ],
        time_grain=TimeGrain.day,
        date_interval=DatetimeInterval(date_from=datetime(2022, 9, 1), date_to=datetime(2022, 9, 5)),
        compare_interval=None,
        x_axis_column=ColumnReference(datasource, constants.DATE_PARTITION_COLUMN_NAME)
    ).to_warehouse_query()

    assert query_builder.build(query)['x'].sql == """SELECT TIMESTAMP(`app_main.user_history`.`date_`) AS x_axis, `app_main.user_history`.`up_string` AS group_by_1, SUM(`app_main.user_history`.`up_int`) AS value
FROM `app_main.user_history`
WHERE (`app_main.user_history`.`date_` BETWEEN DATE '2022-09-01' AND DATE '2022-09-05')
GROUP BY x_axis, group_by_1
ORDER BY x_axis"""


def test_kpi_id_multiple_filters_and_group_bys_sql(datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'user_history')
    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['daily_single'],
        column_filters=[
            ColumnFilter(
                column_ref=ColumnReference(datasource, 'up_string'),
                operation='=', value_list=['value']
            ),
            ColumnFilter(
                column_ref=ColumnReference(datasource, 'up_int'),
                operation='=', value_list=['1']
            )
        ],
        column_group_bys=[
            ColumnReference(datasource, 'up_string'),
            ColumnReference(datasource, 'up_int')
        ],
        time_grain=TimeGrain.day,
        date_interval=DatetimeInterval(date_from=datetime(2022, 9, 1), date_to=datetime(2022, 9, 5)),
        compare_interval=None,
        x_axis_column=ColumnReference(datasource, constants.DATE_PARTITION_COLUMN_NAME)
    ).to_warehouse_query()

    assert query_builder.build(query)['x'].sql == """SELECT TIMESTAMP(`app_main.user_history`.`date_`) AS x_axis, `app_main.user_history`.`up_string` AS group_by_1, `app_main.user_history`.`up_int` AS group_by_2, SUM(`app_main.user_history`.`up_int`) AS value
FROM `app_main.user_history`
WHERE (`app_main.user_history`.`date_` BETWEEN DATE '2022-09-01' AND DATE '2022-09-05') AND `app_main.user_history`.`up_string` = 'value' AND `app_main.user_history`.`up_int` = 1
GROUP BY x_axis, group_by_1, group_by_2
ORDER BY x_axis"""
