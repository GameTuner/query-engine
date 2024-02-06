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

from queryengine.core.bigquery.sql.base import SqlToken, Statement
from queryengine.core.bigquery.sql.expression import AliasedExpression, Expression


class TableLike(SqlToken, ABC):
    def column(self, column_name: str, alias: str = None) -> 'Expression':
        rendered_column_name = '.'.join(f'`{c}`' for c in column_name.split('.'))
        return AliasedExpression(Expression(f'{self.to_sql()}.{rendered_column_name}'), alias)


class Table(TableLike):
    def __init__(self, dataset_name, table_name):
        super().__init__()
        self.dataset_name = dataset_name
        self.table_name = table_name

    def to_sql(self) -> str:
        return f'`{self.dataset_name}.{self.table_name}`'


class From(SqlToken):
    def __init__(self, table_like: 'TableLike'):
        super().__init__()
        self.table_like = table_like

    def to_sql(self) -> str:
        return f'FROM {self.table_like.to_sql()}'


class Cte(TableLike):
    def __init__(self, cte_name: str, select: Statement):
        super().__init__()
        self.cte_name = cte_name
        self.select = select

    def to_definition_to_sql(self):
        return f"""{self.cte_name} AS (
{self.select.to_sql()})"""

    def to_sql(self) -> str:
        return f'`{self.cte_name}`'
