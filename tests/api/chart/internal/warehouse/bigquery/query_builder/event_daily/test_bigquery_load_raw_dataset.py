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


def test_simple_event_aggregation_load_dataset(datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'events_login')
    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['sum_params.internal_num'],
        column_filters=[],
        date_interval=DatetimeInterval(date_from=datetime(2023, 1, 2), date_to=datetime(2023, 1, 3)),
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
WHERE (`base`.`date_` BETWEEN DATE '2023-01-02' AND DATE '2023-01-03')
GROUP BY x_axis
ORDER BY x_axis"""


def test_simple_event_aggregation_raw_dataset(datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'events_login')
    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['sum_params.internal_num'],
        column_filters=[],
        date_interval=DatetimeInterval(date_from=datetime(2022, 2, 20), date_to=datetime(2022, 2, 21)),
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
WHERE (`base`.`date_` BETWEEN DATE '2022-02-20' AND DATE '2022-02-21')
GROUP BY x_axis
ORDER BY x_axis"""


def test_simple_event_aggregation_raw_and_load_dataset(datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'events_login')
    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['sum_params.internal_num'],
        column_filters=[],
        date_interval=DatetimeInterval(date_from=datetime(2022, 2, 20), date_to=datetime(2023, 1, 5)),
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
WHERE (`base`.`date_` BETWEEN DATE '2022-02-20' AND DATE '2023-01-05')
GROUP BY x_axis
ORDER BY x_axis"""


def test_event_filter_raw_and_load_dataset(datasource_repository, kpi_repository, app_config_repository):
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
        ], date_interval=DatetimeInterval(date_from=datetime(2022, 2, 20), date_to=datetime(2023, 1, 5)),
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
WHERE (`base`.`date_` BETWEEN DATE '2022-02-20' AND DATE '2023-01-05') AND `base`.`params`.`internal_num` = 100
GROUP BY x_axis
ORDER BY x_axis"""
