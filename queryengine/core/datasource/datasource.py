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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Dict

import pytz

from queryengine.core.app.app import App
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.timegrain import TimeGrain


class Cardinality(str, Enum):
    zero = 'zero'
    one = 'one'
    many = 'many'


class DataType(str, Enum):
    @staticmethod
    def parse(data_type: str) -> 'DataType':
        data_type = {
            'float': 'number',
            'map<string,float>': 'map<string,number>',
            'timestamp': 'datetime',
            'map<string,timestamp>': 'map<string,datetime>'
        }.get(data_type, data_type)
        return DataType(data_type)

    number = 'number'
    integer = 'integer'
    string = 'string'
    date = 'date'
    datetime = 'datetime'
    boolean = 'boolean'

    number_map = 'map<string,number>'
    integer_map = 'map<string,integer>'
    string_map = 'map<string,string>'
    date_map = 'map<string,date>'
    datetime_map = 'map<string,datetime>'
    boolean_map = 'map<string,boolean>'


@dataclass
class Column:
    id: str
    data_type: DataType
    description: str = ""
    label: str = None
    available_values: List = None
    can_filter: bool = True
    can_group_by: bool = True
    hidden: bool = False

    def __post_init__(self):
        if self.label is None:
            self.label = self.id.replace('_', ' ').title()
        if self.available_values is None:
            self.available_values = []


class DataSource(ABC):
    def __init__(self, app: App, id: str, label: str, description: str, schema: str,
                 table_name: str, columns: List[Column], rows_per_user: Cardinality,
                 time_grain: TimeGrain):
        self.id = id
        self.label = label
        self.description = description
        self.schema = schema
        self.table_name = table_name
        self.columns_by_id: Dict[str, Column] = {column.id: column for column in columns}
        self.rows_per_user = rows_per_user
        self.time_grain = time_grain
        self.data_availability = self._data_availability(app)

    def can_enrich_user_from_datasource(self, datasource: 'DataSource'):
        return self.rows_per_user == Cardinality.many and datasource.rows_per_user == Cardinality.one

    def user_enrich_table_name(self):
        return self.table_name

    def clamp_date_interval(self, date_interval: DatetimeInterval) -> DatetimeInterval | None:
        if not self.data_availability:
            return None
        return date_interval.clamp(self.data_availability.date_from, self.data_availability.date_to)

    @abstractmethod
    def _data_availability(self, app: App) -> DatetimeInterval | None:
        pass

    def __eq__(self, othr):
        return isinstance(othr, type(self)) and self.id == othr.id

    def __hash__(self):
        return hash(self.id)


class DailyDataSource(DataSource):
    def __init__(self, app: App, id: str, label: str, description: str, schema: str, table_name: str,
                 columns: List[Column], rows_per_user: Cardinality):
        super(DailyDataSource, self).__init__(app, id, label, description, schema, table_name,
                                              columns, rows_per_user, TimeGrain.day)

    def _data_availability(self, app: App) -> DatetimeInterval | None:
        if not app.app_config.has_data_up_to(self.id):
            return None
        date_from = datetime.combine(app.app_config.has_data_from(self.id), datetime.min.time()).replace(
            tzinfo=pytz.UTC)
        date_to = datetime.combine(app.app_config.has_data_up_to(self.id), datetime.min.time()).replace(tzinfo=pytz.UTC)
        return DatetimeInterval(date_from, date_to)


@dataclass
class ColumnReference:
    datasource: DataSource
    column_id: str

    def column(self) -> Column:
        return self.datasource.columns_by_id[self.column_id]
