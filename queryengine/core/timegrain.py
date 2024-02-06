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

from datetime import timedelta, datetime
from enum import Enum

from dateutil.relativedelta import relativedelta


class TimeGrain(str, Enum):
    min15 = 'min15'
    hour = 'hour'
    day = 'day'
    week = 'week'
    month = 'month'
    quarter = 'quarter'
    year = 'year'

    def __lt__(self, other):
        return self.to_minutes() < other.to_minutes()

    def to_minutes(self):
        if self == TimeGrain.min15:
            return 15
        if self == TimeGrain.hour:
            return TimeGrain.min15.to_minutes() * 4
        if self == TimeGrain.day:
            return TimeGrain.hour.to_minutes() * 24
        elif self == TimeGrain.week:
            return TimeGrain.day.to_minutes() * 7
        elif self == TimeGrain.month:
            return TimeGrain.day.to_minutes() * 30
        elif self == TimeGrain.quarter:
            return TimeGrain.month.to_minutes() * 3
        elif self == TimeGrain.year:
            return TimeGrain.month.to_minutes() * 12
        else:
            raise ValueError(f"Not supported TimeGrain: {self.value}")

    def next_datetime(self, dt: datetime) -> datetime:
        if self == TimeGrain.min15:
            return dt + timedelta(minutes=15)
        if self == TimeGrain.hour:
            return dt + timedelta(hours=1)
        if self == TimeGrain.day:
            return dt + timedelta(days=1)
        elif self == TimeGrain.week:
            return dt + timedelta(days=7)
        elif self == TimeGrain.month:
            return dt + relativedelta(months=1)
        elif self == TimeGrain.quarter:
            return dt + relativedelta(months=3)
        elif self == TimeGrain.year:
            return dt + relativedelta(years=1)
        else:
            raise ValueError(f"Not supported TimeGrain: {self.value}")

    def truncate_datetime(self, dt: datetime) -> datetime:
        if self == TimeGrain.min15:
            return dt.replace(minute=15 * round(dt.minute / 15), second=0, microsecond=0)
        if self == TimeGrain.hour:
            return dt.replace(minute=0, second=0, microsecond=0)
        if self == TimeGrain.day:
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        elif self == TimeGrain.week:
            return TimeGrain.day.truncate_datetime(dt) - timedelta(days=dt.weekday())
        elif self == TimeGrain.month:
            return TimeGrain.day.truncate_datetime(dt).replace(day=1)
        elif self == TimeGrain.quarter:
            quarter = (dt.month - 1) // 3 + 1
            return TimeGrain.month.truncate_datetime(dt).replace(month=(quarter - 1) * 3 + 1)
        elif self == TimeGrain.year:
            return TimeGrain.month.truncate_datetime(dt).replace(month=1)
        else:
            raise ValueError(f"Not supported TimeGrain: {self.value}")
