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

from typing import Dict

from queryengine.core.app.app import App
from queryengine.core.kpi.kpi import Kpi, Rollup, WarehouseMetric
from queryengine.core.kpi.repository import InMemoryKpiRepository


class TestKpiRepository(InMemoryKpiRepository):
    def _from_daily_data_sources(self, app: App) -> Dict[str, Dict[str, Kpi]]:
        return {'user_history': {
            'daily_single': Kpi('daily_single', formula="x",
                                metrics={'x': WarehouseMetric("SUM({up_int})", None, 'user_history')},
                                x_axis={'date_': Rollup('SUM', 'SUM')}),

            'daily_single_x_avg': Kpi('daily_single', formula="x",
                                      metrics={'x': WarehouseMetric("SUM({up_int})", None, 'user_history')},
                                      x_axis={'date_': Rollup('AVG', 'SUM')}),

            'daily_single_optimized': Kpi('daily_single_optimized', formula="x",
                                          metrics={'x': WarehouseMetric("SUM({up_int})", None, 'user_history_daily')},
                                          x_axis={'date_': Rollup('SUM', 'SUM')}),

            'daily_single_filter': Kpi('daily_single_filter', formula="x",
                                       metrics={
                                           'x': WarehouseMetric("SUM({up_int})", "{up_string} = 'a'", 'user_history')},
                                       x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

            'cohort_single': Kpi('cohort_single', formula="x",
                                 metrics={'x': WarehouseMetric("SUM({up_int})", None, 'user_history')},
                                 x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

            'cohort_single_optimized': Kpi('cohort_single_optimized', formula="x",
                                           metrics={'x': WarehouseMetric("SUM({up_int})", None, 'user_history_daily')},
                                           x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

            'daily_external': Kpi('daily_external', formula="x",
                                metrics={'x': WarehouseMetric("SUM({ext_int})", None, 'user_history')},
                                x_axis={'date_': Rollup('SUM', 'SUM')}),

            'daily_external_materialized': Kpi('ext_int_materialized', formula="x",
                                  metrics={'x': WarehouseMetric("SUM({ext_int_materialized})", None, 'user_history')},
                                  x_axis={'date_': Rollup('SUM', 'SUM')}),

            'daily_external_combine_same_filter': Kpi('daily_external_combine_same_filter', formula="x",
                                               metrics={'x': WarehouseMetric("SUM({ext_int_materialized} + {ext_int_materialized_same_filter_different_formula})", None, 'user_history')},
                                               x_axis={'date_': Rollup('SUM', 'SUM')}),

            'daily_external_combine_different_filter': Kpi('daily_external_combine_different_filter', formula="x",
                                                      metrics={'x': WarehouseMetric("SUM({ext_int} + {ext_int_materialized})", None, 'user_history')},
                                                      x_axis={'date_': Rollup('SUM', 'SUM')}),

            'daily_with_external_filter': Kpi('daily_with_external_filter', formula="x",
                                  metrics={'x': WarehouseMetric("SUM({up_int})", "{ext_int_materialized} > 0", 'user_history')},
                                  x_axis={'date_': Rollup('SUM', 'SUM')}),

            'complex': Kpi('complex', formula="x + y",
                           metrics={
                               'x': WarehouseMetric("SUM({up_int})", None, 'user_history'),
                               'y': WarehouseMetric("SUM({up_int})", None, 'user_history')
                           },
                           x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),
            'complex_add': Kpi('complex', formula="x + y",
                           metrics={
                               'x': WarehouseMetric("SUM({up_int})", None, 'user_history'),
                               'y': WarehouseMetric("SUM({up_int})", None, 'user_history')
                           },
                           x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),
            'complex_sub': Kpi('complex', formula="x - y",
                               metrics={
                                   'x': WarehouseMetric("SUM({up_int})", None, 'user_history'),
                                   'y': WarehouseMetric("SUM({up_int})", None, 'user_history')
                               },
                               x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),
            'complex_mul': Kpi('complex', formula="x * y",
                               metrics={
                                   'x': WarehouseMetric("SUM({up_int})", None, 'user_history'),
                                   'y': WarehouseMetric("SUM({up_int})", None, 'user_history')
                               },
                               x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),
            'complex_div': Kpi('complex', formula="x / y",
                               metrics={
                                   'x': WarehouseMetric("SUM({up_int})", None, 'user_history'),
                                   'y': WarehouseMetric("SUM({up_int})", None, 'user_history')
                               },
                               x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),
        }}
