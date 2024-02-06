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


def test_registration_column_with_computed_column_from_registration_column(
        datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'user_history')
    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['daily_single'],
        column_filters=[],
        column_group_bys=[ColumnReference(datasource, 'comp_registration')],
        time_grain=TimeGrain.day,
        date_interval=DatetimeInterval(date_from=datetime(2022, 3, 1), date_to=datetime(2022, 3, 5)),
        compare_interval=None,
        x_axis_column=ColumnReference(datasource, constants.DATE_PARTITION_COLUMN_NAME)
    ).to_warehouse_query()

    assert query_builder.build(query)['x'].sql == """SELECT TIMESTAMP(`app_main.user_history`.`date_`) AS x_axis, `app_main.user_history`.`date_` AS group_by_1, SUM(`app_main.user_history`.`up_int`) AS value
FROM `app_main.user_history`
WHERE (`app_main.user_history`.`date_` BETWEEN DATE '2022-03-01' AND DATE '2022-03-05')
GROUP BY x_axis, group_by_1
ORDER BY x_axis"""


def test_registration_column_with_computed_column_from_external_columns(
        datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'user_history')
    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['daily_single'],
        column_filters=[],
        column_group_bys=[ColumnReference(datasource, 'comp_two_externals')],
        time_grain=TimeGrain.day,
        date_interval=DatetimeInterval(date_from=datetime(2022, 3, 1), date_to=datetime(2022, 3, 5)),
        compare_interval=None,
        x_axis_column=ColumnReference(datasource, constants.DATE_PARTITION_COLUMN_NAME)
    ).to_warehouse_query()

    assert query_builder.build(query)['x'].sql == """WITH _external_app_raw_purchase AS (
SELECT `app_raw.purchase`.`date_`, `app_raw.purchase`.`unique_id`, SUM(`app_raw.purchase`.`purchase`) AS ext_int
FROM `app_raw.purchase`
GROUP BY `app_raw.purchase`.`date_`, `app_raw.purchase`.`unique_id`
),
_external_app_raw_purchase_dc7bd85092 AS (
SELECT `app_raw.purchase`.`date_`, `app_raw.purchase`.`unique_id`, SUM(`app_raw.purchase`.`purchase`) AS ext_int_materialized
FROM `app_raw.purchase`
WHERE `app_raw.purchase`.`status` = 'SUCCESS'
GROUP BY `app_raw.purchase`.`date_`, `app_raw.purchase`.`unique_id`
)
SELECT TIMESTAMP(`app_main.user_history`.`date_`) AS x_axis, COALESCE(`_external_app_raw_purchase_dc7bd85092`.`ext_int_materialized`, 0) + `_external_app_raw_purchase`.`ext_int` AS group_by_1, SUM(`app_main.user_history`.`up_int`) AS value
FROM `app_main.user_history`
LEFT JOIN `_external_app_raw_purchase_dc7bd85092` ON `app_main.user_history`.`date_` = `_external_app_raw_purchase_dc7bd85092`.`date_` AND `app_main.user_history`.`unique_id` = `_external_app_raw_purchase_dc7bd85092`.`unique_id`
LEFT JOIN `_external_app_raw_purchase` ON `app_main.user_history`.`date_` = `_external_app_raw_purchase`.`date_` AND `app_main.user_history`.`unique_id` = `_external_app_raw_purchase`.`unique_id`
WHERE (`app_main.user_history`.`date_` BETWEEN DATE '2022-03-01' AND DATE '2022-03-05')
GROUP BY x_axis, group_by_1
ORDER BY x_axis"""


def test_registration_column_with_computed_column_from_computed_column(
        datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'user_history')
    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['daily_single'],
        column_filters=[],
        column_group_bys=[ColumnReference(datasource, 'comp_computed')],
        time_grain=TimeGrain.day,
        date_interval=DatetimeInterval(date_from=datetime(2022, 3, 1), date_to=datetime(2022, 3, 5)),
        compare_interval=None,
        x_axis_column=ColumnReference(datasource, constants.DATE_PARTITION_COLUMN_NAME)
    ).to_warehouse_query()

    assert query_builder.build(query)['x'].sql == """WITH _external_app_raw_purchase AS (
SELECT `app_raw.purchase`.`date_`, `app_raw.purchase`.`unique_id`, SUM(`app_raw.purchase`.`purchase`) AS ext_int
FROM `app_raw.purchase`
GROUP BY `app_raw.purchase`.`date_`, `app_raw.purchase`.`unique_id`
),
_external_app_raw_purchase_dc7bd85092 AS (
SELECT `app_raw.purchase`.`date_`, `app_raw.purchase`.`unique_id`, SUM(`app_raw.purchase`.`purchase`) AS ext_int_materialized
FROM `app_raw.purchase`
WHERE `app_raw.purchase`.`status` = 'SUCCESS'
GROUP BY `app_raw.purchase`.`date_`, `app_raw.purchase`.`unique_id`
)
SELECT TIMESTAMP(`app_main.user_history`.`date_`) AS x_axis, COALESCE(`_external_app_raw_purchase_dc7bd85092`.`ext_int_materialized`, 0) + `_external_app_raw_purchase`.`ext_int` + 1 AS group_by_1, SUM(`app_main.user_history`.`up_int`) AS value
FROM `app_main.user_history`
LEFT JOIN `_external_app_raw_purchase_dc7bd85092` ON `app_main.user_history`.`date_` = `_external_app_raw_purchase_dc7bd85092`.`date_` AND `app_main.user_history`.`unique_id` = `_external_app_raw_purchase_dc7bd85092`.`unique_id`
LEFT JOIN `_external_app_raw_purchase` ON `app_main.user_history`.`date_` = `_external_app_raw_purchase`.`date_` AND `app_main.user_history`.`unique_id` = `_external_app_raw_purchase`.`unique_id`
WHERE (`app_main.user_history`.`date_` BETWEEN DATE '2022-03-01' AND DATE '2022-03-05')
GROUP BY x_axis, group_by_1
ORDER BY x_axis"""