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
from queryengine.core import constants
from queryengine.core.datasource.datasource import ColumnReference
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.timegrain import TimeGrain
from queryengine.api.chart.internal.warehouse.bigquery.query_builder import query_builder


def test_external_materialized_filter(datasource_repository, kpi_repository, app_config_repository):
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
                column_ref=ColumnReference(user_history_datasource, 'ext_int_materialized'),
                operation='=', value_list=['0']
            )
        ],
        date_interval=DatetimeInterval(date_from=datetime(2022, 3, 1), date_to=datetime(2022, 3, 5)),
        compare_interval=None,
        time_grain=TimeGrain.day,
        column_group_bys=[],
        x_axis_column=ColumnReference(datasource, constants.DATE_PARTITION_COLUMN_NAME)
    ).to_warehouse_query()

    assert query_builder.build(query)['x'].sql == """WITH _external_app_raw_purchase_dc7bd85092 AS (
SELECT `app_raw.purchase`.`date_`, `app_raw.purchase`.`unique_id`, SUM(`app_raw.purchase`.`purchase`) AS ext_int_materialized
FROM `app_raw.purchase`
WHERE `app_raw.purchase`.`status` = 'SUCCESS'
GROUP BY `app_raw.purchase`.`date_`, `app_raw.purchase`.`unique_id`
),
base AS (
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
INNER JOIN `app_main.v_user_history_daily` ON `base`.`date_` = `app_main.v_user_history_daily`.`date_` AND `base`.`unique_id` = `app_main.v_user_history_daily`.`unique_id`
LEFT JOIN `_external_app_raw_purchase_dc7bd85092` ON `base`.`date_` = `_external_app_raw_purchase_dc7bd85092`.`date_` AND `base`.`unique_id` = `_external_app_raw_purchase_dc7bd85092`.`unique_id`
WHERE (`base`.`date_` BETWEEN DATE '2022-03-01' AND DATE '2022-03-05') AND COALESCE(`_external_app_raw_purchase_dc7bd85092`.`ext_int_materialized`, 0) = 0
GROUP BY x_axis
ORDER BY x_axis"""


def test_external_materialized_group_by(datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'events_login')
    user_history_datasource = datasource_repository.load_datasource_by_id(app, 'user_history')
    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['sum_params.internal_num'],
        column_filters=[],
        date_interval=DatetimeInterval(date_from=datetime(2022, 3, 1), date_to=datetime(2022, 3, 5)),
        compare_interval=None,
        time_grain=TimeGrain.day,
        column_group_bys=[ColumnReference(user_history_datasource, 'ext_int_materialized')],
        x_axis_column=ColumnReference(datasource, constants.DATE_PARTITION_COLUMN_NAME)
    ).to_warehouse_query()

    assert query_builder.build(query)['x'].sql == """WITH _external_app_raw_purchase_dc7bd85092 AS (
SELECT `app_raw.purchase`.`date_`, `app_raw.purchase`.`unique_id`, SUM(`app_raw.purchase`.`purchase`) AS ext_int_materialized
FROM `app_raw.purchase`
WHERE `app_raw.purchase`.`status` = 'SUCCESS'
GROUP BY `app_raw.purchase`.`date_`, `app_raw.purchase`.`unique_id`
),
base AS (
SELECT *
FROM `app_raw.login`
WHERE `app_raw.login`.`date_` BETWEEN DATE '2022-01-01' AND DATE '2023-01-01'
 UNION ALL
SELECT *
FROM `app_load.login`
WHERE `app_load.login`.`date_` > DATE '2023-01-01' AND `app_load.login`.`sandbox_mode` IS NOT TRUE
)
SELECT DATE_TRUNC(`base`.`event_tstamp`, DAY) AS x_axis, COALESCE(`_external_app_raw_purchase_dc7bd85092`.`ext_int_materialized`, 0) AS group_by_1, SUM(`base`.`params`.`internal_num`) AS value
FROM `base`
INNER JOIN `app_main.v_user_history_daily` ON `base`.`date_` = `app_main.v_user_history_daily`.`date_` AND `base`.`unique_id` = `app_main.v_user_history_daily`.`unique_id`
LEFT JOIN `_external_app_raw_purchase_dc7bd85092` ON `base`.`date_` = `_external_app_raw_purchase_dc7bd85092`.`date_` AND `base`.`unique_id` = `_external_app_raw_purchase_dc7bd85092`.`unique_id`
WHERE (`base`.`date_` BETWEEN DATE '2022-03-01' AND DATE '2022-03-05')
GROUP BY x_axis, group_by_1
ORDER BY x_axis"""


def test_external_materialized_group_by_combined_columns(datasource_repository, kpi_repository, app_config_repository):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'events_login')
    user_history_datasource = datasource_repository.load_datasource_by_id(app, 'user_history')
    query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, datasource.id)['sum_params.internal_num'],
        column_filters=[],
        date_interval=DatetimeInterval(date_from=datetime(2022, 3, 1), date_to=datetime(2022, 3, 5)),
        compare_interval=None,
        time_grain=TimeGrain.day,
        column_group_bys=[ColumnReference(user_history_datasource, 'ext_int_materialized'), ColumnReference(user_history_datasource, 'ext_int')],
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
),
base AS (
SELECT *
FROM `app_raw.login`
WHERE `app_raw.login`.`date_` BETWEEN DATE '2022-01-01' AND DATE '2023-01-01'
 UNION ALL
SELECT *
FROM `app_load.login`
WHERE `app_load.login`.`date_` > DATE '2023-01-01' AND `app_load.login`.`sandbox_mode` IS NOT TRUE
)
SELECT DATE_TRUNC(`base`.`event_tstamp`, DAY) AS x_axis, COALESCE(`_external_app_raw_purchase_dc7bd85092`.`ext_int_materialized`, 0) AS group_by_1, `_external_app_raw_purchase`.`ext_int` AS group_by_2, SUM(`base`.`params`.`internal_num`) AS value
FROM `base`
INNER JOIN `app_main.v_user_history_daily` ON `base`.`date_` = `app_main.v_user_history_daily`.`date_` AND `base`.`unique_id` = `app_main.v_user_history_daily`.`unique_id`
LEFT JOIN `_external_app_raw_purchase_dc7bd85092` ON `base`.`date_` = `_external_app_raw_purchase_dc7bd85092`.`date_` AND `base`.`unique_id` = `_external_app_raw_purchase_dc7bd85092`.`unique_id`
LEFT JOIN `_external_app_raw_purchase` ON `base`.`date_` = `_external_app_raw_purchase`.`date_` AND `base`.`unique_id` = `_external_app_raw_purchase`.`unique_id`
WHERE (`base`.`date_` BETWEEN DATE '2022-03-01' AND DATE '2022-03-05')
GROUP BY x_axis, group_by_1, group_by_2
ORDER BY x_axis"""