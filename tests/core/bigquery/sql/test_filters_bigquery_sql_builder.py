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
from typing import List

import pytest

from queryengine.api.chart.internal.domain import ChartQuery
from queryengine.api.chart.internal.domain import ColumnFilter
from queryengine.api.chart.internal.warehouse.bigquery.query_builder import query_builder
from queryengine.core import constants
from queryengine.core.datasource.datasource import ColumnReference
from queryengine.core.datasource.repository import DataSourceRepository
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.kpi.repository import KpiRepository
from queryengine.core.timegrain import TimeGrain


def get_sql(datasource_repository: DataSourceRepository, kpi_repository: KpiRepository, app_config_repository,
            filters: List[ColumnFilter]) -> str:
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'user_history')
    chart_query = ChartQuery(
        app=app,
        page_id="page",
        request_id="request",
        datasource=datasource,
        kpi=kpi_repository.load_by_datasource_id(app, 'user_history')['daily_single'],
        column_filters=filters,
        date_interval=DatetimeInterval(date_from=datetime(2022, 9, 1), date_to=datetime(2022, 9, 5)),
        compare_interval=None,
        time_grain=TimeGrain.day,
        column_group_bys=[],
        x_axis_column=ColumnReference(datasource, constants.DATE_PARTITION_COLUMN_NAME)
    ).to_warehouse_query()
    return query_builder.build(chart_query)['x'].sql


@pytest.mark.parametrize("column_id,operation,value_list,sql", [
    ("up_string", "=", ['value'], "`app_main.user_history`.`up_string` = 'value'"),
    ("up_string", "!=", ['value'], "`app_main.user_history`.`up_string` != 'value'"),
    ("up_string", ">", ['value'], "`app_main.user_history`.`up_string` > 'value'"),
    ("up_string", ">=", ['value'], "`app_main.user_history`.`up_string` >= 'value'"),
    ("up_string", "<", ['value'], "`app_main.user_history`.`up_string` < 'value'"),
    ("up_string", "<=", ['value'], "`app_main.user_history`.`up_string` <= 'value'"),
])
def test_operations_work_with_all_types(datasource_repository, kpi_repository, app_config_repository, column_id,
                                        operation, value_list, sql):
    app = app_config_repository.from_app_id('app')
    datasource = datasource_repository.load_datasource_by_id(app, 'user_history')
    filters = [ColumnFilter(
        column_ref=ColumnReference(datasource, column_id),
        operation=operation,
        value_list=value_list)]

    assert get_sql(datasource_repository, kpi_repository, app_config_repository, filters) == f"""SELECT TIMESTAMP(`app_main.user_history`.`date_`) AS x_axis, SUM(`app_main.user_history`.`up_int`) AS value
FROM `app_main.user_history`
WHERE (`app_main.user_history`.`date_` BETWEEN DATE '2022-09-01' AND DATE '2022-09-05') AND {sql}
GROUP BY x_axis
ORDER BY x_axis"""
