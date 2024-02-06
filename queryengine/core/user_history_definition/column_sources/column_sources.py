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

from abc import abstractmethod
from typing import List

from queryengine.core.user_history_definition.column_sources import external_table_columns, registration_columns
from queryengine.core.bigquery.column_source import ColumnSource
from queryengine.core.bigquery.sql.expression import Expression, DotsFormatter, TemplateDict
from queryengine.core.bigquery.sql.sql_builder import QueryBuilder, SelectStatement
from queryengine.core.bigquery.sql.table_like import TableLike
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.user_history_definition.user_history_definition import UserHistoryDefinition, RegistrationColumn, \
    TotalColumn, ExternalTableColumn, ComputedColumn


class BaseUserHistoryColumnSource(ColumnSource):
    def __init__(self, app_id: str, user_history_table: TableLike, user_history_definition: UserHistoryDefinition,
                 sql_builder: QueryBuilder, select_statement: SelectStatement):
        super().__init__(user_history_table)
        self._app_id = app_id
        self._user_history_definition = user_history_definition
        self._sql_builder = sql_builder
        self._select_statement = select_statement

    def get_and_load_column(self, column_name: str, date_intervals: List[DatetimeInterval]) -> Expression:
        if column_name in self._user_history_definition.registration_columns:
            return self.get_and_load_registration_column(self._user_history_definition.registration_columns[column_name], date_intervals)
        if column_name in self._user_history_definition.total_columns:
            return self.get_and_load_total_column(self._user_history_definition.total_columns[column_name], date_intervals)
        if column_name in self._user_history_definition.external_table_columns:
            return self.get_and_load_external_table_column(self._user_history_definition.external_table_columns[column_name], date_intervals)
        if column_name in self._user_history_definition.computed_columns:
            return self.get_and_load_computed_column(self._user_history_definition.computed_columns[column_name], date_intervals)
        raise Exception(f'Column {column_name} not found in user history definition')

    @abstractmethod
    def get_and_load_registration_column(self, registration_column: RegistrationColumn, date_intervals: List[DatetimeInterval]) -> Expression:
        pass

    @abstractmethod
    def get_and_load_total_column(self, total_column: TotalColumn, date_intervals: List[DatetimeInterval]) -> Expression:
        pass

    @abstractmethod
    def get_and_load_external_table_column(self, external_table_column: ExternalTableColumn, date_intervals: List[DatetimeInterval]) -> Expression:
        pass

    def get_and_load_computed_column(self, computed_column: ComputedColumn, date_intervals: List[DatetimeInterval]) -> Expression:
        column_names = DotsFormatter().get_format_strings(computed_column.formula)
        return Expression(computed_column.formula, template_dict=TemplateDict(
            {c: self.get_and_load_column(c, date_intervals).to_sql() for c in column_names}))


class QueryUserHistoryColumnSource(BaseUserHistoryColumnSource):
    def get_and_load_registration_column(self, registration_column: RegistrationColumn, date_intervals: List[DatetimeInterval]) -> Expression:
        return self.table.column(column_name=registration_column.name)

    def get_and_load_total_column(self, total_column: TotalColumn, date_intervals: List[DatetimeInterval]) -> Expression:
        return self.table.column(column_name=total_column.name)

    def get_and_load_external_table_column(self, external_table_column: ExternalTableColumn, date_intervals: List[DatetimeInterval]) -> Expression:
        return external_table_columns.get_column(
            external_table_column=external_table_column,
            date_intervals=date_intervals,
            sql_builder=self._sql_builder,
            select_statement=self._select_statement,
            allow_materialized=True,
        )


class InsertUserHistoryColumnSource(BaseUserHistoryColumnSource):
    def get_and_load_column(self, column_name: str, date_intervals: List[DatetimeInterval]) -> Expression:
        if len(date_intervals) != 1 or date_intervals[0].date_from != date_intervals[0].date_to:
            raise Exception('InsertUserHistoryColumnSource expects exactly one date')
        return super().get_and_load_column(column_name, date_intervals)

    def get_and_load_registration_column(self, registration_column: RegistrationColumn, date_intervals: List[DatetimeInterval]) -> Expression:
        return registration_columns.get_column_for_insert_query(
            app_id=self._app_id,
            registration_column=registration_column,
            sql_builder=self._sql_builder,
            select_statement=self._select_statement
        )

    def get_and_load_total_column(self, total_column: TotalColumn, date_intervals: List[DatetimeInterval]) -> Expression:
        # find total with, join bla bla
        # insert colu
        return self.table.column(column_name=total_column.name)

    def get_and_load_external_table_column(self, external_table_column: ExternalTableColumn, date_intervals: List[DatetimeInterval]) -> Expression:
        return external_table_columns.get_column(
            external_table_column=external_table_column,
            date_intervals=date_intervals,
            sql_builder=self._sql_builder,
            select_statement=self._select_statement,
            allow_materialized=False
        )

