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

from pandas import DataFrame
from pandas.testing import assert_frame_equal

from queryengine.core import constants
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.tabular_data_result import TabularDataResult


def test_from_cohort_days_initialized_correctly():
    table_result = TabularDataResult.from_cohort_days(
        days=DatetimeInterval(
            date_from=datetime(2023, 1, 1),
            date_to=datetime(2023, 1, 5),
        ).days(),
        group_by_columns=[],
        group_by_values=set()
    )
    expected_df = DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [0, 1, 2, 3, 4],
        constants.DATA_COLUMN_ALIAS: [0, 0, 0, 0, 0]
    })
    assert_frame_equal(table_result.df, expected_df, check_like=True)


def test_from_cohort_days_group_by_initialized_correctly():
    table_result = TabularDataResult.from_cohort_days(
        days=DatetimeInterval(
            date_from=datetime(2023, 1, 1),
            date_to=datetime(2023, 1, 5),
        ).days(),
        group_by_columns=['g1', 'g2'],
        group_by_values={('a', 1), ('b', 2)}
    )
    expected_df = DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [0, 0, 1, 1, 2, 2, 3, 3, 4, 4],
        'g1': ['a', 'b', 'a', 'b', 'a', 'b', 'a', 'b', 'a', 'b'],
        'g2': [1, 2, 1, 2, 1, 2, 1, 2, 1, 2],
        constants.DATA_COLUMN_ALIAS: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    })
    assert_frame_equal(table_result.df.sort_index(), expected_df.sort_index(), check_like=True)
