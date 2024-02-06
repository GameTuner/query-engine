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
from queryengine.api.event_errors.internal.domain import EventErrorsQuery

from queryengine.core.app.datasource import AppRepository
from queryengine.core.datasource.repository import DataSourceRepository
from queryengine.core.dateinterval import DateInterval


@dataclass(frozen=True)
class EventErrorsQueryDTO:
    page_id: str
    request_id: str
    date_interval: DateInterval
    event_name: str | None = None

    def to_domain_model(self, app_id: str, app_repository: AppRepository) -> EventErrorsQuery:
        app = app_repository.from_app_id(app_id)
        return EventErrorsQuery(
            app=app,
            page_id=self.page_id,
            request_id=self.request_id,
            date_interval=self.date_interval.to_datetime_interval(),
            event_name=self.event_name
        )
