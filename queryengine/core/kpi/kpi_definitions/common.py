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

from typing import List, Callable

from queryengine.core.kpi.kpi import Kpi, Rollup, Unit, WarehouseMetric

COHORT_DAYS = [1, 3, 7, 14, 28, 30, 45, 60, 90, 180, 365]


def generate_daily_cohorted_kpis(cohort_days: List[int], generator: Callable[[int], Kpi]) -> List[Kpi]:
    kpis = []
    for cohort_day in cohort_days:
        kpis.append(generator(cohort_day))
    return kpis


def user_history_table_from_cohort_day(cohort_day: int, is_daily_active: bool = False):
    if cohort_day <= 30:
        return 'v_user_history_monthly'
    elif is_daily_active:
        return 'v_user_history_daily'
    else:
        return 'v_user_history'


KPIS = [
    # registrations
    Kpi('cohort_size', formula="x",
        category="Engagement",
        metrics={'x': WarehouseMetric("SUM({cohort_size})", None, 'v_user_history')},
        x_axis={'date_': Rollup('AVG', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('registrations', formula="x",
        category="Engagement", recommended=True,
        metrics={'x': WarehouseMetric("SUM({cohort_size})", '{cohort_day} = 0', 'v_user_history_daily')},
        x_axis={'date_': Rollup('SUM', 'SUM')}),

    # activity
    Kpi('dau', label="DAU", formula="x",
        category="Engagement", recommended=True,
        metrics={'x': WarehouseMetric("SUM({dau})", None, 'v_user_history_daily')},
        x_axis={'date_': Rollup('AVG', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('mdau', label="mDAU", formula="x",
        category="Engagement", recommended=True,
        metrics={'x': WarehouseMetric("SUM({dau})", '{is_payer}', 'v_user_history_daily')},
        x_axis={'date_': Rollup('AVG', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('wau', label="WAU", formula="x",
        category="Engagement",
        metrics={'x': WarehouseMetric("SUM({wau})", None, 'v_user_history_monthly')},
        x_axis={'date_': Rollup('AVG', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('mau', label="MAU", formula="x",
        category="Engagement",
        metrics={'x': WarehouseMetric("SUM({mau})", None, 'v_user_history_monthly')},
        x_axis={'date_': Rollup('AVG', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('mau_lost', label="MAU Lost", formula="x",
        category="Engagement",
        metrics={'x': WarehouseMetric("SUM(if({date_} - {last_login_day} = interval 29 day, 1, 0))", None,
                                      'v_user_history_monthly')},
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('mau_reactivated', label="MAU Reactivated", formula="x",
        category="Engagement",
        metrics={'x': WarehouseMetric(
            "SUM(if({date_} - {previous_login_day} >= interval 29 day and {last_login_day} = {date_}, 1, 0))", None,
            'v_user_history_daily')},
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('stickiness', unit=Unit.percent(), formula="dau * 100 / mau",
        category="Engagement",
        metrics={
            'dau': WarehouseMetric("SUM({dau})", None, 'v_user_history_daily'),
            'mau': WarehouseMetric("SUM({mau})", None, 'v_user_history_monthly'),
        },
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('retention', formula="dau * 100 / cohort_size",
        category="Engagement", recommended=True,
        metrics={
            'dau': WarehouseMetric("SUM({dau})", None, 'v_user_history_daily'),
            'cohort_size': WarehouseMetric("SUM({cohort_size})", None, 'v_user_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('customer_retention', formula="daily_payers * 100 / mdau",
        category="Revenue",
        metrics={
            'daily_payers': WarehouseMetric("SUM({dau})", None, 'v_user_history_daily'),
            'mdau': WarehouseMetric("SUM({dau})", '{is_payer}', 'v_user_history_daily'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('avg_sessions', label='Number of Sessions', formula="sessions_count / dau",
        category="Engagement",
        metrics={
            'sessions_count': WarehouseMetric("SUM({sessions_count})", None, 'v_user_history_daily'),
            'dau': WarehouseMetric("SUM({dau})", None, 'v_user_history_daily'),
        },
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('avg_session_length', unit=Unit.minute(), formula="playtime / 60 / sessions_count",
        category="Engagement",
        metrics={
            'playtime': WarehouseMetric("SUM({playtime})", None, 'v_user_history_daily'),
            'sessions_count': WarehouseMetric("SUM({sessions_count})", None, 'v_user_history_daily'),
        },
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('avg_playtime', unit=Unit.minute(), formula="playtime / 60 / dau",
        category="Engagement",
        metrics={
            'playtime': WarehouseMetric("SUM({playtime})", None, 'v_user_history_daily'),
            'dau': WarehouseMetric("SUM({dau})", None, 'v_user_history_daily'),
        },
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    # revenue
    Kpi('gross_revenue_usd', label="IAP Revenue Gross", unit=Unit.dollar(), formula="x",
        category="Revenue", recommended=True,
        metrics={'x': WarehouseMetric("SUM({gross_revenue_usd})", None, 'v_user_history_daily')},
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('net_revenue_usd', label="IAP Revenue Net", unit=Unit.dollar(), formula="x",
        category="Revenue",
        metrics={'x': WarehouseMetric("SUM({net_revenue_usd})", None, 'v_user_history_daily')},
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('daily_purchase_rate', unit=Unit.percent(), formula="daily_payers * 100 / dau",
        category="Revenue", recommended=True,
        metrics={
            'daily_payers': WarehouseMetric("SUM({daily_payers})", None, 'v_user_history_daily'),
            'dau': WarehouseMetric("SUM({dau})", None, 'v_user_history_daily'),
        },
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('daily_conversion', unit=Unit.percent(), formula="new_payers / dau",
        category="Revenue",
        metrics={
            'new_payers': WarehouseMetric("SUM({new_payers})", None, 'v_user_history_daily'),
            'dau': WarehouseMetric("SUM({dau})", None, 'v_user_history_daily'),
        },
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('arpdau_gross', label='IAP ARPDAU Gross', unit=Unit.dollar(), formula="gross_revenue_usd / dau",
        category="Revenue", recommended=True,
        metrics={
            'gross_revenue_usd': WarehouseMetric("SUM({gross_revenue_usd})", None, 'v_user_history_daily'),
            'dau': WarehouseMetric("SUM({dau})", None, 'v_user_history_daily'),
        },
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('arpdau_net', label='IAP ARPDAU Net', unit=Unit.dollar(), formula="net_revenue_usd / dau",
        category="Revenue",
        metrics={
            'net_revenue_usd': WarehouseMetric("SUM({net_revenue_usd})", None, 'v_user_history_daily'),
            'dau': WarehouseMetric("SUM({dau})", None, 'v_user_history_daily'),
        },
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('arppu_gross', label='IAP ARPPU Gross', unit=Unit.dollar(), formula="gross_revenue_usd / daily_payers",
        category="Revenue", recommended=True,
        metrics={
            'gross_revenue_usd': WarehouseMetric("SUM({gross_revenue_usd})", None, 'v_user_history_daily'),
            'daily_payers': WarehouseMetric("SUM({daily_payers})", None, 'v_user_history_daily'),
        },
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('arppu_net', label='IAP ARPPU Net', unit=Unit.dollar(), formula="net_revenue_usd / daily_payers",
        category="Revenue",
        metrics={
            'net_revenue_usd': WarehouseMetric("SUM({net_revenue_usd})", None, 'v_user_history_daily'),
            'daily_payers': WarehouseMetric("SUM({daily_payers})", None, 'v_user_history_daily'),
        },
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('avg_transaction_gross', label='Avg Transaction Gross', unit=Unit.dollar(), formula="gross_revenue_usd / transactions_count",
        category="Revenue",
        metrics={
            'gross_revenue_usd': WarehouseMetric("SUM({gross_revenue_usd})", None, 'v_user_history_daily'),
            'transactions_count': WarehouseMetric("SUM({transactions_count})", None, 'v_user_history_daily'),
        },
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('avg_transaction_net', label='Avg Transaction Net', unit=Unit.dollar(), formula="net_revenue_usd / transactions_count",
        category="Revenue",
        metrics={
            'net_revenue_usd': WarehouseMetric("SUM({net_revenue_usd})", None, 'v_user_history_daily'),
            'transactions_count': WarehouseMetric("SUM({transactions_count})", None, 'v_user_history_daily'),
        },
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('new_payers', formula="x",
        category="Revenue",
        metrics={'x': WarehouseMetric("SUM({new_payers})", None, 'v_user_history_daily')},
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('daily_payers', formula="x",
        category="Revenue",
        metrics={'x': WarehouseMetric("SUM({daily_payers})", None, 'v_user_history_daily')},
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('total_payers', formula="x",
        category="Revenue",
        metrics={'x': WarehouseMetric("SUM({total_payers})", None, 'v_user_history')},
        x_axis={'date_': Rollup('SUM', 'SUM'), 'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('ltv', label='LTV Gross', unit=Unit.dollar(), formula="revenue_total / cohort_size",
        category="Revenue",
        metrics={
            'revenue_total': WarehouseMetric("SUM({gross_revenue_usd_total})", None, 'v_user_history'),
            'cohort_size': WarehouseMetric("SUM({cohort_size})", None, 'v_user_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('ltv_net', label='LTV Net', unit=Unit.dollar(), formula="revenue_total / cohort_size",
        category="Revenue",
        metrics={
            'revenue_total': WarehouseMetric("SUM({net_revenue_usd_total})", None, 'v_user_history'),
            'cohort_size': WarehouseMetric("SUM({cohort_size})", None, 'v_user_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('cohort_conversion', unit=Unit.percent(), formula="total_payers * 100 / cohort_size",
        category="Revenue",
        metrics={
            'total_payers': WarehouseMetric("SUM({total_payers})", None, 'v_user_history'),
            'cohort_size': WarehouseMetric("SUM({cohort_size})", None, 'v_user_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),
]

KPIS.extend(generate_daily_cohorted_kpis(COHORT_DAYS, lambda cohort_day:
Kpi(f'retention_d{cohort_day}', label=f'Retention D{cohort_day}', unit=Unit.percent(),
    formula="dau * 100 / cohort_size",
    category="Engagement",
    metrics={
        'dau': WarehouseMetric("SUM({dau})", "{cohort_day} = " + str(cohort_day),
                               user_history_table_from_cohort_day(cohort_day, True)),
        'cohort_size': WarehouseMetric("SUM({cohort_size})", "{cohort_day} = " + str(cohort_day),
                                       user_history_table_from_cohort_day(cohort_day)),
    },
    x_axis={'date_': Rollup('SUM', 'SUM')})))

KPIS.extend(generate_daily_cohorted_kpis(COHORT_DAYS, lambda cohort_day:
Kpi(f'ltv_d{cohort_day}', label=f'LTV D{cohort_day}', unit=Unit.dollar(),
    formula="revenue_total / cohort_size",
    category="Revenue",
    metrics={
        'revenue_total': WarehouseMetric("SUM({gross_revenue_usd_total})", "{cohort_day} = " + str(cohort_day),
                                         user_history_table_from_cohort_day(cohort_day)),
        'cohort_size': WarehouseMetric("SUM({cohort_size})", "{cohort_day} = " + str(cohort_day),
                                       user_history_table_from_cohort_day(cohort_day)),
    },
    x_axis={'date_': Rollup('SUM', 'SUM')})))

KPIS.extend(generate_daily_cohorted_kpis(COHORT_DAYS, lambda cohort_day:
Kpi(f'ltv_net_d{cohort_day}', label=f'LTV Net D{cohort_day}', unit=Unit.dollar(),
    formula="revenue_total / cohort_size",
    category="Revenue",
    metrics={
        'revenue_total': WarehouseMetric("SUM({net_revenue_usd_total})", "{cohort_day} = " + str(cohort_day),
                                         user_history_table_from_cohort_day(cohort_day)),
        'cohort_size': WarehouseMetric("SUM({cohort_size})", "{cohort_day} = " + str(cohort_day),
                                       user_history_table_from_cohort_day(cohort_day)),
    },
    x_axis={'date_': Rollup('SUM', 'SUM')})))

KPIS.extend(generate_daily_cohorted_kpis(COHORT_DAYS, lambda cohort_day:
Kpi(f'cohort_conversion_d{cohort_day}', label=f'Cohort Conversion D{cohort_day}', unit=Unit.percent(),
    formula="total_payers * 100 / cohort_size",
    category="Revenue",
    metrics={
        'total_payers': WarehouseMetric("SUM({total_payers})", "{cohort_day} = " + str(cohort_day),
                                        user_history_table_from_cohort_day(cohort_day)),
        'cohort_size': WarehouseMetric("SUM({cohort_size})", "{cohort_day} = " + str(cohort_day),
                                       user_history_table_from_cohort_day(cohort_day)),
    },
    x_axis={'date_': Rollup('SUM', 'SUM')})))

APPSFLYER_KPIS = [
    
    Kpi('spend', unit=Unit.dollar(), formula="x", label="[AppsFlyer] Spend",
        category="Marketing",
        metrics={
            'x': WarehouseMetric("SUM({cost})", None, 'v_appsflyer_activity')
        },
        x_axis={'date_': Rollup('SUM', 'SUM')}),
    
    Kpi('impresions', formula="x", label="[AppsFlyer] Impresions",
        category="Marketing",
        metrics={
            'x': WarehouseMetric("SUM({impressions})", None, 'v_appsflyer_activity')
        },
        x_axis={'date_': Rollup('SUM', 'SUM')}),

    Kpi('cpm', unit=Unit.dollar(), formula="cost / impressions * 1000", label="[AppsFlyer] CPM",
        category="Marketing",
        metrics={
            'cost': WarehouseMetric("SUM({cost})", None, 'v_appsflyer_activity'),
            'impressions': WarehouseMetric("SUM({impressions})", None, 'v_appsflyer_activity')
        },
        x_axis={'date_': Rollup('SUM', 'SUM')}),
    
    Kpi('clicks', formula="x", label="[AppsFlyer] Clicks",
        category="Marketing",
        metrics={'x': WarehouseMetric("SUM({clicks})", None, 'v_appsflyer_activity')},
        x_axis={'date_': Rollup('SUM', 'SUM')}),

    Kpi('ctr', formula="clicks / impressions", label="[AppsFlyer] CTR",
        category="Marketing",
        metrics={
            'clicks': WarehouseMetric("SUM({clicks})", None, 'v_appsflyer_activity'),
            'impressions': WarehouseMetric("SUM({impressions})", None, 'v_appsflyer_activity')
        },
        x_axis={'date_': Rollup('SUM', 'SUM')}),

    Kpi('cpc', unit=Unit.dollar(), formula="cost / clicks", label="[AppsFlyer] CPC",
        category="Marketing",
        metrics={
            'cost': WarehouseMetric("SUM({cost})", None, 'v_appsflyer_activity'),
            'clicks': WarehouseMetric("SUM({clicks})", None, 'v_appsflyer_activity')
        },
        x_axis={'date_': Rollup('SUM', 'SUM')}),

    # registrations
    Kpi('installations', formula="x", label="[AppsFlyer] Installations",
        category="Marketing",
        metrics={'x': WarehouseMetric("SUM({installs})", None, 'v_appsflyer_activity')},
        x_axis={'date_': Rollup('SUM', 'SUM')}),

    Kpi('cpi', unit=Unit.dollar(), formula="cost / installs", label="[AppsFlyer] CPI",
        category="Marketing",
        metrics={
            'cost': WarehouseMetric("SUM({cost})", None, 'v_appsflyer_activity'),
            'installs': WarehouseMetric("SUM({installs})", None, 'v_appsflyer_activity')
        },
        x_axis={'date_': Rollup('SUM', 'SUM')}),

    Kpi('retention', formula="dau * 100 / cohort_size", label="[AppsFlyer] Retention", unit=Unit.percent(),
        category="Marketing",
        metrics={
            'dau': WarehouseMetric("SUM({unique_users})", None, 'v_appsflyer_cohort_unified_history'),
            'cohort_size': WarehouseMetric("SUM({installs})", None, 'v_appsflyer_cohort_unified_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('iap_purchase_cohort', formula="x", label="[AppsFlyer] IAP Purchase Gross", unit=Unit.dollar(),
        category="Marketing",
        metrics={'x': WarehouseMetric("SUM({iap_revenue_usd})", None, 'v_appsflyer_cohort_unified_history')},
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('iap_purchase_net_cohort', formula="x", label="[AppsFlyer] IAP Purchase Net", unit=Unit.dollar(),
        category="Marketing",
        metrics={'x': WarehouseMetric("SUM({iap_net_revenue_usd})", None, 'v_appsflyer_cohort_unified_history')},
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('iap_purchase', unit=Unit.dollar(), formula="x", label="[AppsFlyer] IAP Purchase Gross",
        category="Marketing",
        metrics={'x': WarehouseMetric("SUM({iap_revenue_usd})", None, 'v_appsflyer_activity')},
        x_axis={'date_': Rollup('SUM', 'SUM')}),

    Kpi('iap_purchase_net', unit=Unit.dollar(), formula="x", label="[AppsFlyer] IAP Purchase Net",
        category="Marketing",
        metrics={'x': WarehouseMetric("SUM({iap_net_revenue_usd})", None, 'v_appsflyer_activity')},
        x_axis={'date_': Rollup('SUM', 'SUM')}),

    Kpi('ad_revenue_cohort', formula="x", label="[AppsFlyer] Ad Revenue", unit=Unit.dollar(),
        category="Marketing",
        metrics={'x': WarehouseMetric("SUM({ad_revenue_usd})", None, 'v_appsflyer_cohort_unified_history')},
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('ad_revenue', unit=Unit.dollar(), formula="x", label="[AppsFlyer] Ad Revenue",
        category="Marketing",
        metrics={'x': WarehouseMetric("SUM({ad_revenue_usd})", None, 'v_appsflyer_activity')},
        x_axis={'date_': Rollup('SUM', 'SUM')}),

    Kpi('revenue_cohort', unit=Unit.dollar(), formula="purchase + ad_revenue", label="[AppsFlyer] Revenue Gross",
        category="Marketing",
        metrics={
            'purchase': WarehouseMetric("SUM({iap_revenue_usd})", None, 'v_appsflyer_cohort_unified_history'),
            'ad_revenue': WarehouseMetric("SUM({ad_revenue_usd})", None, 'v_appsflyer_cohort_unified_history'),
            },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('revenue', unit=Unit.dollar(), formula="purchase + ad_revenue", label="[AppsFlyer] Revenue Gross",
        category="Marketing",
        metrics={
            'purchase': WarehouseMetric("SUM({iap_revenue_usd})", None, 'v_appsflyer_activity'),
            'ad_revenue': WarehouseMetric("SUM({ad_revenue_usd})", None, 'v_appsflyer_activity'),
            },
        x_axis={'date_': Rollup('SUM', 'SUM')}),

    Kpi('revenue_net', unit=Unit.dollar(), formula="purchase + ad_revenue", label="[AppsFlyer] Revenue Net",
        category="Marketing",
        metrics={
            'purchase': WarehouseMetric("SUM({iap_net_revenue_usd})", None, 'v_appsflyer_activity'),
            'ad_revenue': WarehouseMetric("SUM({ad_revenue_usd})", None, 'v_appsflyer_activity'),
            },
        x_axis={'date_': Rollup('SUM', 'SUM')}),

    Kpi('cvr', unit=Unit.dollar(), formula="installs / clicks", label="[AppsFlyer] CVR",
        category="Marketing",
        metrics={
            'installs': WarehouseMetric("SUM({installs})", None, 'v_appsflyer_activity'),
            'clicks': WarehouseMetric("SUM({clicks})", None, 'v_appsflyer_activity')
        },
        x_axis={'date_': Rollup('SUM', 'SUM')}),

    Kpi('roas_iap_purchase', formula="purchase / cost * 100", label="[AppsFlyer] ROAS Purchase Gross", unit=Unit.percent(),
        category="Marketing",
        metrics={
            'purchase': WarehouseMetric("SUM({iap_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'),
            'cost': WarehouseMetric("SUM({cost})", None, 'v_appsflyer_cohort_unified_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('roas_iap_net_purchase', formula="purchase / cost * 100", label="[AppsFlyer] ROAS Purchase Net", unit=Unit.percent(),
        category="Marketing",
        metrics={
            'purchase': WarehouseMetric("SUM({iap_net_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'), 
            'cost': WarehouseMetric("SUM({cost})", None, 'v_appsflyer_cohort_unified_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('roas_ad_revenue', formula="ad_revenue / cost * 100", label="[AppsFlyer] ROAS Ad Revenue", unit=Unit.percent(),
        category="Marketing",
        metrics={
            'ad_revenue': WarehouseMetric("SUM({ad_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'),
            'cost': WarehouseMetric("SUM({cost})", None, 'v_appsflyer_cohort_unified_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('roas_net', formula="((ad_revenue + purchase) / cost) * 100", label="[AppsFlyer] ROAS Net", unit=Unit.percent(),
        category="Marketing",
        metrics={
            'purchase': WarehouseMetric("SUM({iap_net_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'), 
            'ad_revenue': WarehouseMetric("SUM({ad_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'),
            'cost': WarehouseMetric("SUM({cost})", None, 'v_appsflyer_cohort_unified_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('roas_gross', formula="((ad_revenue + purchase) / cost) * 100", label="[AppsFlyer] ROAS Gross", unit=Unit.percent(),
        category="Marketing",
        metrics={
            'purchase': WarehouseMetric("SUM({iap_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'), 
            'ad_revenue': WarehouseMetric("SUM({ad_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'),
            'cost': WarehouseMetric("SUM({cost})", None, 'v_appsflyer_cohort_unified_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('roi_iap_purchase', formula="((purchase - cost) / cost) * 100", label="[AppsFlyer] ROI Purchase Gross", unit=Unit.percent(),
        category="Marketing",
        metrics={
            'purchase': WarehouseMetric("SUM({iap_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'),
            'cost': WarehouseMetric("SUM({cost})", None, 'v_appsflyer_cohort_unified_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('roi_iap_net_purchase', formula="((purchase - cost) / cost) * 100", label="[AppsFlyer] ROI Purchase Net", unit=Unit.percent(),
        category="Marketing",
        metrics={
            'purchase': WarehouseMetric("SUM({iap_net_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'),
            'cost': WarehouseMetric("SUM({cost})", None, 'v_appsflyer_cohort_unified_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('roi_ad_revenue', formula="((ad_revenue - cost) / cost) * 100", label="[AppsFlyer] ROI Ad Revenue", unit=Unit.percent(),
        category="Marketing",
        metrics={
            'ad_revenue': WarehouseMetric("SUM({ad_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'),
            'cost': WarehouseMetric("SUM({cost})", None, 'v_appsflyer_cohort_unified_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('roi_net', formula="((purchase + ad_revenue - cost) / cost) * 100", label="[AppsFlyer] ROI Net", unit=Unit.percent(),
        category="Marketing",
        metrics={
            'ad_revenue': WarehouseMetric("SUM({ad_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'),
            'purchase': WarehouseMetric("SUM({iap_net_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'),
            'cost': WarehouseMetric("SUM({cost})", None, 'v_appsflyer_cohort_unified_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('roi_gross', formula="((purchase + ad_revenue - cost) / cost) * 100", label="[AppsFlyer] ROI Gross", unit=Unit.percent(),
        category="Marketing",
        metrics={
            'ad_revenue': WarehouseMetric("SUM({ad_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'),
            'purchase': WarehouseMetric("SUM({iap_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'),
            'cost': WarehouseMetric("SUM({cost})", None, 'v_appsflyer_cohort_unified_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    

    Kpi('ltv', unit=Unit.dollar(), formula="revenue_total / cohort_size", label="[AppsFlyer] LTV",
        category="Marketing",
        metrics={
            'revenue_total': WarehouseMetric("SUM({iap_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'),
            'cohort_size': WarehouseMetric("SUM({installs})", None, 'v_appsflyer_cohort_unified_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    Kpi('ltv_net', unit=Unit.dollar(), formula="(purchase + ad_revenue)/ cohort_size", label="[AppsFlyer] LTV Net",
        category="Marketing",
        metrics={
            'ad_revenue': WarehouseMetric("SUM({ad_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'),
            'purchase': WarehouseMetric("SUM({iap_net_revenue_usd_total})", None, 'v_appsflyer_cohort_unified_history'),
            'cohort_size': WarehouseMetric("SUM({installs})", None, 'v_appsflyer_cohort_unified_history'),
        },
        x_axis={'cohort_day': Rollup('SUM', 'SUM')}),

    ]

APPSFLYER_KPIS.extend(generate_daily_cohorted_kpis(COHORT_DAYS, lambda cohort_day:
Kpi(f'retention_d{cohort_day}', label=f'[AppsFlyer] Retention D{cohort_day}', unit=Unit.percent(),
    formula="dau * 100 / cohort_size",
    category="Marketing",
    metrics={
        'dau': WarehouseMetric("SUM({unique_users})", "{cohort_day} = " + str(cohort_day), "v_appsflyer_cohort_unified_history",),
        'cohort_size': WarehouseMetric("SUM({installs})", "{cohort_day} = " + str(cohort_day), "v_appsflyer_cohort_unified_history"),
    },
    x_axis={'date_': Rollup('SUM', 'SUM')})))

APPSFLYER_KPIS.extend(generate_daily_cohorted_kpis(COHORT_DAYS, lambda cohort_day:
Kpi(f'ltv_d{cohort_day}', label=f'[AppsFlyer] LTV D{cohort_day} Net', unit=Unit.dollar(),
    formula="revenue_total / cohort_size",
    category="Marketing",
    metrics={
        'revenue_total': WarehouseMetric("SUM({iap_net_revenue_usd_total})", "{cohort_day} = " + str(cohort_day), "v_appsflyer_cohort_unified_history"),
        'cohort_size': WarehouseMetric("SUM({installs})", "{cohort_day} = " + str(cohort_day), "v_appsflyer_cohort_unified_history"),
    },
    x_axis={'date_': Rollup('SUM', 'SUM')})))

APPSFLYER_KPIS.extend(generate_daily_cohorted_kpis(COHORT_DAYS, lambda cohort_day:
Kpi(f'roi_gross_d{cohort_day}', label=f'[AppsFlyer] ROI D{cohort_day} Gross', unit=Unit.percent(),
    formula="((purchase + ad_revenue - cost) / cost) * 100",
    category="Marketing",
    metrics={
        'ad_revenue': WarehouseMetric("SUM({ad_revenue_usd_total})", "{cohort_day} = " + str(cohort_day), 'v_appsflyer_cohort_unified_history'),
        'purchase': WarehouseMetric("SUM({iap_revenue_usd_total})", "{cohort_day} = " + str(cohort_day), 'v_appsflyer_cohort_unified_history'),
        'cost': WarehouseMetric("SUM({cost})", "{cohort_day} = " + str(cohort_day), 'v_appsflyer_cohort_unified_history')
    },
    x_axis={'date_': Rollup('SUM', 'SUM')})))

APPSFLYER_KPIS.extend(generate_daily_cohorted_kpis(COHORT_DAYS, lambda cohort_day:
Kpi(f'roi_net_d{cohort_day}', label=f'[AppsFlyer] ROI D{cohort_day} Net', unit=Unit.percent(),
    formula="((purchase + ad_revenue - cost) / cost) * 100",
    category="Marketing",
    metrics={
        'ad_revenue': WarehouseMetric("SUM({ad_revenue_usd_total})", "{cohort_day} = " + str(cohort_day), 'v_appsflyer_cohort_unified_history'),
        'purchase': WarehouseMetric("SUM({iap_net_revenue_usd_total})", "{cohort_day} = " + str(cohort_day), 'v_appsflyer_cohort_unified_history'),
        'cost': WarehouseMetric("SUM({cost})", "{cohort_day} = " + str(cohort_day), 'v_appsflyer_cohort_unified_history')
    },
    x_axis={'date_': Rollup('SUM', 'SUM')})))