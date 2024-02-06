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

from queryengine.api.chart.internal.domain import ChartQuery, ColumnFilter
from queryengine.api.chart.internal.warehouse.bigquery.query_builder import query_builder
from queryengine.core import constants
from queryengine.core.datasource.datasource import ColumnReference
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.timegrain import TimeGrain


def test_simple_event_aggregation(datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'events_login')
    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['sum_params.internal_num'],
        column_filters=[],
        date_interval=DatetimeInterval(date_from=datetime(2022, 9, 1), date_to=datetime(2022, 9, 5)),
        compare_interval=None,
        time_grain=TimeGrain.day,
        column_group_bys=[],
        x_axis_column=ColumnReference(datasource, constants.DATE_PARTITION_COLUMN_NAME)
    ).to_warehouse_query()

    assert query_builder.build(query)['x'].sql == """WITH base AS (
SELECT *
FROM `app_raw.login`
WHERE `app_raw.login`.`date_` BETWEEN DATE '2022-01-01' AND DATE '2023-01-01'
 UNION ALL
SELECT *
FROM `app_load.login`
WHERE `app_load.login`.`date_` > DATE '2023-01-01' AND `app_load.login`.`sandbox_mode` IS NOT TRUE
)
SELECT DATE_TRUNC(`base`.`event_tstamp`, DAY) AS x_axis, SUM(`base`.`params`.`internal_num`) AS value
FROM `base`
WHERE (`base`.`date_` BETWEEN DATE '2022-09-01' AND DATE '2022-09-05')
GROUP BY x_axis
ORDER BY x_axis"""


def test_single_event_parameter_and_group_by(datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'events_login')
    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['sum_params.internal_num'],
        column_filters=[
            ColumnFilter(
                column_ref=ColumnReference(datasource, 'params.internal_num'),
                operation='=', value_list=['100']
            )
        ],
        date_interval=DatetimeInterval(date_from=datetime(2022, 9, 1), date_to=datetime(2022, 9, 5)),
        compare_interval=None,
        time_grain=TimeGrain.day,
        column_group_bys=[ColumnReference(datasource, 'params.internal_num')],
        x_axis_column=ColumnReference(datasource, constants.DATE_PARTITION_COLUMN_NAME)
    ).to_warehouse_query()

    assert query_builder.build(query)['x'].sql == """WITH base AS (
SELECT *
FROM `app_raw.login`
WHERE `app_raw.login`.`date_` BETWEEN DATE '2022-01-01' AND DATE '2023-01-01'
 UNION ALL
SELECT *
FROM `app_load.login`
WHERE `app_load.login`.`date_` > DATE '2023-01-01' AND `app_load.login`.`sandbox_mode` IS NOT TRUE
)
SELECT DATE_TRUNC(`base`.`event_tstamp`, DAY) AS x_axis, `base`.`params`.`internal_num` AS group_by_1, SUM(`base`.`params`.`internal_num`) AS value
FROM `base`
WHERE (`base`.`date_` BETWEEN DATE '2022-09-01' AND DATE '2022-09-05') AND `base`.`params`.`internal_num` = 100
GROUP BY x_axis, group_by_1
ORDER BY x_axis"""


def test_multiple_event_parameter_and_group_by(datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'events_login')

    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['sum_params.internal_num'],
        column_filters=[
            ColumnFilter(
                column_ref=ColumnReference(datasource, 'params.internal_num'),
                operation='=', value_list=['100']
            ),
            ColumnFilter(
                column_ref=ColumnReference(datasource, 'num'),
                operation='=', value_list=['200']
            )
        ],
        date_interval=DatetimeInterval(date_from=datetime(2022, 9, 1), date_to=datetime(2022, 9, 5)),
        compare_interval=None,
        time_grain=TimeGrain.day,
        column_group_bys=[
            ColumnReference(datasource, 'params.internal_num'),
            ColumnReference(datasource, 'num')
        ],
        x_axis_column=ColumnReference(datasource, constants.DATE_PARTITION_COLUMN_NAME)
    ).to_warehouse_query()

    assert query_builder.build(query)['x'].sql == """WITH base AS (
SELECT *
FROM `app_raw.login`
WHERE `app_raw.login`.`date_` BETWEEN DATE '2022-01-01' AND DATE '2023-01-01'
 UNION ALL
SELECT *
FROM `app_load.login`
WHERE `app_load.login`.`date_` > DATE '2023-01-01' AND `app_load.login`.`sandbox_mode` IS NOT TRUE
)
SELECT DATE_TRUNC(`base`.`event_tstamp`, DAY) AS x_axis, `base`.`params`.`internal_num` AS group_by_1, `base`.`num` AS group_by_2, SUM(`base`.`params`.`internal_num`) AS value
FROM `base`
WHERE (`base`.`date_` BETWEEN DATE '2022-09-01' AND DATE '2022-09-05') AND `base`.`params`.`internal_num` = 100 AND `base`.`num` = 200
GROUP BY x_axis, group_by_1, group_by_2
ORDER BY x_axis"""


def test_single_user_parameter_and_group_by(datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'events_login')
    user_history_datasource = datasource_repository.load_datasource_by_id(app, 'user_history')
    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['sum_params.internal_num'],
        column_filters=[
            ColumnFilter(
                column_ref=ColumnReference(user_history_datasource, 'up_string'),
                operation='=', value_list=['value']
            )
        ],
        date_interval=DatetimeInterval(date_from=datetime(2022, 9, 1), date_to=datetime(2022, 9, 5)),
        compare_interval=None,
        time_grain=TimeGrain.day,
        column_group_bys=[ColumnReference(user_history_datasource, 'up_string')],
        x_axis_column=ColumnReference(datasource, constants.DATE_PARTITION_COLUMN_NAME)
    ).to_warehouse_query()

    assert query_builder.build(query)['x'].sql == """WITH base AS (
SELECT *
FROM `app_raw.login`
WHERE `app_raw.login`.`date_` BETWEEN DATE '2022-01-01' AND DATE '2023-01-01'
 UNION ALL
SELECT *
FROM `app_load.login`
WHERE `app_load.login`.`date_` > DATE '2023-01-01' AND `app_load.login`.`sandbox_mode` IS NOT TRUE
)
SELECT DATE_TRUNC(`base`.`event_tstamp`, DAY) AS x_axis, `app_main.v_user_history_daily`.`up_string` AS group_by_1, SUM(`base`.`params`.`internal_num`) AS value
FROM `base`
INNER JOIN `app_main.v_user_history_daily` ON `base`.`date_` = `app_main.v_user_history_daily`.`date_` AND `base`.`unique_id` = `app_main.v_user_history_daily`.`unique_id`
WHERE (`base`.`date_` BETWEEN DATE '2022-09-01' AND DATE '2022-09-05') AND `app_main.v_user_history_daily`.`up_string` = 'value'
GROUP BY x_axis, group_by_1
ORDER BY x_axis"""
