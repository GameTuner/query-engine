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
from typing import List

import pytz

from queryengine.core.app.app import App, Schema
from queryengine.core.datasource.datasource import DailyDataSource, Column, Cardinality, DataSource, DataType
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.timegrain import TimeGrain
from queryengine.core.user_history_definition.user_history_definition import UserHistoryDefinition


class UserHistoryDataSource(DailyDataSource):
    def __init__(self, app: App, user_history_definition: UserHistoryDefinition):
        self.user_history_definition = user_history_definition
        super(UserHistoryDataSource, self).__init__(app, 'user_history', "User History", "", 'main', 'v_user_history',
                                                    user_history_definition.get_columns(), Cardinality.one)

    def user_enrich_table_name(self):
        return 'v_user_history_daily'


class AppsflyerDataSource(DailyDataSource):
    def __init__(self, app: App, columns: List[Column]):
        super(AppsflyerDataSource, self).__init__(app, 'appsflyer', "Appsflyer", "", 'main', 'v_appsflyer_activity',
                                                  columns, Cardinality.zero)


class EventDataSource(DataSource):
    def __init__(self, app: App, id: str, label: str, description: str, table_name: str, columns: List[Column],
                 schema: str = 'raw'):
        super(EventDataSource, self).__init__(app, id, label, description, schema, table_name,
                                              columns, Cardinality.many, TimeGrain.min15)
        self.realtime_schema = 'load'
        self.raw_data_availability = self._raw_data_availability(app)

    @staticmethod
    def from_event_schema(app: App, event_schema: Schema) -> 'EventDataSource':
        all_columns = []
        all_columns.extend([Column(
            id=f'params.{param.name}',
            label=param.name.split(',')[-1],
            description=param.description,
            data_type=DataType.parse(param.type),
            can_filter=True,
            can_group_by=True,
            available_values=[],
        ) for param in event_schema.parameters])

        all_columns.extend([Column(
            id=name,
            label=name,
            description="",
            data_type=DataType.parse(data_type),
            can_filter=True,
            can_group_by=True,
            available_values=[],
        ) for name, data_type in app.common_configs.atomic_fields.items()])

        for context_schema in app.common_configs.embedded_context_schemas.values():
            all_columns.extend([Column(
                id=f'{context_schema.name}.{param.name}',
                label=param.alias,
                description=param.description,
                data_type=DataType.parse(param.type),
                can_filter=True,
                can_group_by=True,
                available_values=[],
            ) for param in context_schema.parameters])

        return EventDataSource(
            app=app,
            id="events_" + event_schema.name,
            label=event_schema.alias,
            table_name=event_schema.name,
            description=event_schema.description,
            columns=all_columns,
            schema=app.app_config.events_database
        )

    def _data_availability(self, app: App) -> DatetimeInterval | None:
        date_from = datetime.combine(app.app_config.has_data_from('user_history'), datetime.min.time()).replace(
            tzinfo=pytz.UTC)
        date_to = datetime.utcnow().replace(tzinfo=pytz.UTC)
        return DatetimeInterval(date_from, date_to)

    def _raw_data_availability(self, app: App) -> DatetimeInterval | None:
        if not app.app_config.has_data_up_to('user_history'):
            return None
        date_from = datetime.combine(app.app_config.has_data_from('user_history'), datetime.min.time()).replace(
            tzinfo=pytz.UTC)
        date_to = datetime.combine(app.app_config.has_data_up_to('user_history'), datetime.min.time()).replace(
            tzinfo=pytz.UTC)
        return DatetimeInterval(date_from, date_to)