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

import pytz
from pandas import DataFrame
from pandas.testing import assert_frame_equal

from queryengine.core import constants
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.tabular_data_result import TabularDataResult
from queryengine.core.timegrain import TimeGrain


def test_from_date_interval_hourly_grain_initialized_correctly():
    table_result = TabularDataResult.from_date_interval(
        date_interval=DatetimeInterval(
            date_from=datetime(2023, 1, 1),
            date_to=datetime(2023, 1, 1, 23, 59),
        ),
        time_grain=TimeGrain.hour,
        group_by_columns=[],
        group_by_values=set()
    )
    expected_df = DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [datetime(2023, 1, 1, h).replace(tzinfo=pytz.UTC) for h in range(24)],
        constants.DATA_COLUMN_ALIAS: [0] * 24
    })
    assert_frame_equal(table_result.df, expected_df, check_like=True)


def test_from_date_interval_daily_grain_initialized_correctly():
    table_result = TabularDataResult.from_date_interval(
        date_interval=DatetimeInterval(
            date_from=datetime(2023, 1, 1),
            date_to=datetime(2023, 1, 3),
        ),
        time_grain=TimeGrain.day,
        group_by_columns=[],
        group_by_values=set()
    )
    expected_df = DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [
            datetime(2023, 1, 1).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 2).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 3).replace(tzinfo=pytz.UTC)],
        constants.DATA_COLUMN_ALIAS: [0, 0, 0]
    })
    assert_frame_equal(table_result.df, expected_df, check_like=True)


def test_from_date_interval_daily_grain_group_by_initialized_correctly():
    table_result = TabularDataResult.from_date_interval(
        date_interval=DatetimeInterval(
            date_from=datetime(2023, 1, 1),
            date_to=datetime(2023, 1, 3),
        ),
        time_grain=TimeGrain.day,
        group_by_columns=['g1', 'g2'],
        group_by_values={('a', 1), ('b', 2)}
    )
    expected_df = DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [
            datetime(2023, 1, 1).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 1).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 2).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 2).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 3).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 3).replace(tzinfo=pytz.UTC)
        ],
        'g1': ['a', 'b', 'a', 'b', 'a', 'b'],
        'g2': [1, 2, 1, 2, 1, 2],
        constants.DATA_COLUMN_ALIAS: [0, 0, 0, 0, 0, 0],
    })
    assert_frame_equal(table_result.df.sort_index(), expected_df.sort_index(), check_like=True)
