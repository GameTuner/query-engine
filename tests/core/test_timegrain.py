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

import pytest

from queryengine.core.timegrain import TimeGrain


@pytest.mark.parametrize("dt,grain,expected_dt", [
    # min15
    (datetime(2022, 3, 10, 3, 0, 0), TimeGrain.min15, datetime(2022, 3, 10, 3, 0, 0)),
    (datetime(2022, 3, 10, 3, 0, 5), TimeGrain.min15, datetime(2022, 3, 10, 3, 0, 0)),
    (datetime(2022, 3, 10, 3, 16, 5), TimeGrain.min15, datetime(2022, 3, 10, 3, 15, 0)),

    # hour
    (datetime(2022, 3, 10, 3, 0, 0), TimeGrain.hour, datetime(2022, 3, 10, 3, 0, 0)),
    (datetime(2022, 3, 10, 3, 0, 5), TimeGrain.hour, datetime(2022, 3, 10, 3, 0, 0)),
    (datetime(2022, 3, 10, 3, 16, 5), TimeGrain.hour, datetime(2022, 3, 10, 3, 0, 0)),

    # day
    (datetime(2022, 3, 10, 3, 0, 0), TimeGrain.day, datetime(2022, 3, 10, 0, 0, 0)),
    (datetime(2022, 3, 10, 3, 0, 5), TimeGrain.day, datetime(2022, 3, 10, 0, 0, 0)),
    (datetime(2022, 3, 10, 3, 16, 5), TimeGrain.day, datetime(2022, 3, 10, 0, 0, 0)),

    # week
    (datetime(2022, 3, 10, 3, 0, 0), TimeGrain.week, datetime(2022, 3, 7, 0, 0, 0)),
    (datetime(2022, 3, 11, 3, 0, 5), TimeGrain.week, datetime(2022, 3, 7, 0, 0, 0)),
    (datetime(2022, 3, 11, 3, 16, 5), TimeGrain.week, datetime(2022, 3, 7, 0, 0, 0)),

    # month
    (datetime(2022, 3, 10, 3, 0, 0), TimeGrain.month, datetime(2022, 3, 1, 0, 0, 0)),
    (datetime(2022, 3, 11, 3, 0, 5), TimeGrain.month, datetime(2022, 3, 1, 0, 0, 0)),

    # quarter
    (datetime(2022, 3, 10, 3, 0, 0), TimeGrain.quarter, datetime(2022, 1, 1, 0, 0, 0)),
    (datetime(2022, 3, 11, 3, 0, 5), TimeGrain.quarter, datetime(2022, 1, 1, 0, 0, 0)),
    (datetime(2022, 5, 11, 3, 0, 5), TimeGrain.quarter, datetime(2022, 4, 1, 0, 0, 0)),
    (datetime(2022, 12, 11, 3, 0, 5), TimeGrain.quarter, datetime(2022, 10, 1, 0, 0, 0)),

    # year
    (datetime(2022, 3, 10, 3, 0, 0), TimeGrain.year, datetime(2022, 1, 1, 0, 0, 0)),
    (datetime(2022, 3, 11, 3, 0, 5), TimeGrain.year, datetime(2022, 1, 1, 0, 0, 0)),
    (datetime(2022, 5, 11, 3, 0, 5), TimeGrain.year, datetime(2022, 1, 1, 0, 0, 0)),
])
def test_truncate_datetime(dt: datetime, grain: TimeGrain, expected_dt: datetime):
    assert grain.truncate_datetime(dt) == expected_dt


@pytest.mark.parametrize("dt,grain,expected_dt", [
    # min15
    (datetime(2022, 3, 10, 3, 0, 0), TimeGrain.min15, datetime(2022, 3, 10, 3, 15, 0)),
    (datetime(2022, 3, 10, 3, 45, 0), TimeGrain.min15, datetime(2022, 3, 10, 4, 0, 0)),

    # hour
    (datetime(2022, 3, 10, 3, 0, 0), TimeGrain.hour, datetime(2022, 3, 10, 4, 0, 0)),
    (datetime(2022, 3, 10, 23, 0, 0), TimeGrain.hour, datetime(2022, 3, 11, 0, 0, 0)),

    # day
    (datetime(2022, 3, 10, 0, 0, 0), TimeGrain.day, datetime(2022, 3, 11, 0, 0, 0)),
    (datetime(2022, 3, 31, 0, 0, 0), TimeGrain.day, datetime(2022, 4, 1, 0, 0, 0)),

    # week
    (datetime(2022, 3, 28, 0, 0, 0), TimeGrain.week, datetime(2022, 4, 4, 0, 0, 0)),

    # month
    (datetime(2022, 12, 1, 0, 0, 0), TimeGrain.month, datetime(2023, 1, 1, 0, 0, 0)),

    # quarter
    (datetime(2022, 4, 1, 0, 0, 0), TimeGrain.quarter, datetime(2022, 7, 1, 0, 0, 0)),
    (datetime(2022, 10, 1, 0, 0, 0), TimeGrain.quarter, datetime(2023, 1, 1, 0, 0, 0)),

    # year
    (datetime(2022, 1, 1, 0, 0, 0), TimeGrain.year, datetime(2023, 1, 1, 0, 0, 0)),
])
def test_next_datetime(dt: datetime, grain: TimeGrain, expected_dt: datetime):
    assert grain.next_datetime(dt) == expected_dt
