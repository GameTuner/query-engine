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

from queryengine.core.app.app import App
from queryengine.core.datasource.datasource import ColumnReference
from queryengine.core.dateinterval import DatetimeInterval


class ColumnValuesQuery:
    def __init__(self, app: App, page_id: str, request_id: str, date_interval: DatetimeInterval,
                 column: ColumnReference):
        self.app = app
        self.page_id = page_id
        self.request_id = request_id
        self.column = column
        self.requested_date_interval = date_interval
        self.date_interval = self.column.datasource.clamp_date_interval(self.requested_date_interval)
