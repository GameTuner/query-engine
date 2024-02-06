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


def test_merge_without_group_by():
    base = TabularDataResult.from_cohort_days(
        days=DatetimeInterval(
            date_from=datetime(2023, 1, 1),
            date_to=datetime(2023, 1, 4),
        ).days(),
        group_by_columns=[],
        group_by_values=set()
    )

    merge = TabularDataResult(
        DataFrame({
            constants.X_AXIS_COLUMN_ALIAS: [0, 1, 2, 3],
            constants.DATA_COLUMN_ALIAS: [1, 2, 3, 4]
        }),
    )

    expected_df = DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [0, 1, 2, 3],
        constants.DATA_COLUMN_ALIAS: [1, 2, 3, 4]
    })

    base = base.merge_values(merge)
    assert_frame_equal(base.df, expected_df, check_like=True)


def test_merge_with_group_by():
    base = TabularDataResult.from_cohort_days(
        days=DatetimeInterval(
            date_from=datetime(2023, 1, 1),
            date_to=datetime(2023, 1, 4),
        ).days(),
        group_by_columns=['g1', 'g2'],
        group_by_values={('a', 1), ('b', 2)}
    )

    merge = TabularDataResult(
        DataFrame({
            constants.X_AXIS_COLUMN_ALIAS: [1, 1],
            constants.DATA_COLUMN_ALIAS: [100, 101],
            'g1': ['a', 'b'],
            'g2': [1, 2]
        }),
    )

    expected_df = DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [0, 0, 1, 1, 2, 2, 3, 3],
        constants.DATA_COLUMN_ALIAS: [0.0, 0.0, 100.0, 101.0, 0.0, 0.0, 0.0, 0.0],
        'g1': ['a', 'b', 'a', 'b', 'a', 'b', 'a', 'b'],
        'g2': [1, 2, 1, 2, 1, 2, 1, 2]
    })

    base = base.merge_values(merge)
    assert_frame_equal(base.df, expected_df, check_like=True)
