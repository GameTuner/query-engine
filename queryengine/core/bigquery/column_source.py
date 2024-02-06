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

from abc import ABC
from typing import List

from queryengine.core.bigquery.sql.expression import Expression
from queryengine.core.bigquery.sql.table_like import TableLike
from queryengine.core.dateinterval import DatetimeInterval


class ColumnSource(ABC):
    def __init__(self, table: TableLike):
        self.table = table

    def get_and_load_column(self, column_name: str, date_intervals: List[DatetimeInterval]) -> Expression:
        pass


class TableColumnSource(ColumnSource):
    def get_and_load_column(self, column_name: str, date_intervals: List[DatetimeInterval]) -> Expression:
        return self.table.column(column_name=column_name)
