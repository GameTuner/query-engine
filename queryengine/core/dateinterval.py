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

from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import List, Optional

import pytz

from queryengine.core.timegrain import TimeGrain


@dataclass
class DatetimeInterval:
    date_from: datetime
    date_to: datetime

    def __post_init__(self):
        self.date_from = self.date_from.replace(tzinfo=pytz.UTC)
        self.date_to = self.date_to.replace(tzinfo=pytz.UTC)

    def days(self):
        return (self.date_to - self.date_from).days + 1

    def generate_all_dates(self, time_grain: TimeGrain) -> List[datetime]:
        dates = []
        current_datetime = time_grain.truncate_datetime(self.date_from)
        while current_datetime <= self.date_to:
            dates.append(current_datetime)
            current_datetime = time_grain.next_datetime(current_datetime)
        return dates

    def clamp(self, date_from: datetime, date_to: datetime) -> Optional['DatetimeInterval']:
        if self.date_from > date_to or self.date_to < date_from:
            return None
        date_from = max(date_from, self.date_from)
        date_to = min(date_to, self.date_to)
        date_to = max(date_to, date_from)
        return DatetimeInterval(date_from, date_to)

    def intersection(self, date_interval) -> Optional['DatetimeInterval']:
        latest_start = max(self.date_from, date_interval.date_from)
        earliest_end = min(self.date_to, date_interval.date_to)
        if latest_start > earliest_end:
            return None
        return DatetimeInterval(latest_start, earliest_end)

    def contains_date(self, d: date) -> bool:
        return self.date_from.date() <= d <= self.date_to.date()


@dataclass(frozen=True)
class DateInterval:
    date_from: date
    date_to: date

    def to_datetime_interval(self, time_grain: TimeGrain | None = None) -> DatetimeInterval:
        if time_grain is None or time_grain.to_minutes() >= TimeGrain.day.to_minutes():
            return DatetimeInterval(
                date_from=datetime.combine(self.date_from, datetime.min.time()),
                date_to=datetime.combine(self.date_to, datetime.min.time()),
            )
        else:
            return DatetimeInterval(
                date_from=datetime.combine(self.date_from, datetime.min.time()),
                date_to=datetime.combine(self.date_to, datetime.min.time()) + timedelta(days=1) - timedelta(
                    milliseconds=1),
            )
