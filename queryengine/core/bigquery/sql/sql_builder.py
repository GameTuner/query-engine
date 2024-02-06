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

from typing import List, Dict

from queryengine.core.bigquery.sql.base import SqlToken, Statement
from queryengine.core.bigquery.sql.boolean_expression import BooleanExpression
from queryengine.core.bigquery.sql.expression import Expression
from queryengine.core.bigquery.sql.join import Join
from queryengine.core.bigquery.sql.table_like import TableLike, From, Cte


class Select(SqlToken):
    def __init__(self, expressions: List[Expression]):
        self.expressions = expressions

    def to_sql(self) -> str:
        return f"SELECT {', '.join([e.to_definition_sql() for e in self.expressions])}"


class Where(SqlToken):
    def __init__(self, boolean_expression: BooleanExpression):
        super().__init__()
        self.boolean_expression = boolean_expression

    def to_sql(self) -> str:
        return f'WHERE {self.boolean_expression.to_sql()}'


class GroupBy(SqlToken):
    def __init__(self, expressions: List[Expression]):
        self.expressions = expressions

    def to_sql(self) -> str:
        return 'GROUP BY ' + ', '.join([e.to_reference_sql() for e in self.expressions])


class OrderBy(SqlToken):
    def __init__(self, expressions: List[Expression]):
        self.expressions = expressions

    def to_sql(self) -> str:
        return 'ORDER BY ' + ', '.join([e.to_reference_sql() for e in self.expressions])


class Limit(SqlToken):
    def __init__(self, limit: int):
        self.limit = limit

    def to_sql(self) -> str:
        return f'LIMIT {self.limit}'


class SelectStatement(Statement):
    def __init__(self):
        super().__init__()
        self._select = None
        self._from: From | None = None
        self._joins: List[Join] = []
        self._where: Where | None = None
        self._group_by = None
        self._order_by = None
        self._limit = None

    def get_select(self) -> Select:
        return self._select

    def get_table(self) -> TableLike:
        return self._from.table_like

    def get_boolean_expression(self) -> BooleanExpression:
        return self._where.boolean_expression

    def get_joins(self):
        return self._joins

    def from_(self, table_like: TableLike) -> 'SelectStatement':
        self._from = From(table_like)
        return self

    def select(self, expressions: List[Expression]) -> 'SelectStatement':
        self._select = Select(expressions)
        return self

    def select_star(self) -> 'SelectStatement':
        return self.select([Expression('*')])

    def join(self, join: Join) -> 'SelectStatement':
        self._joins.append(join)
        return self

    def joins(self, joins: List[Join]) -> 'SelectStatement':
        self._joins.extend(joins)
        return self

    def where(self, boolean_expression: BooleanExpression) -> 'SelectStatement':
        self._where = Where(boolean_expression)
        return self

    def and_where(self, boolean_expression) -> 'SelectStatement':
        if self._where is not None:
            self._where.boolean_expression.and_(boolean_expression)
        else:
            self.where(boolean_expression)
        return self

    def or_where(self, boolean_expression) -> 'SelectStatement':
        if self._where is not None:
            self._where.boolean_expression.or_(boolean_expression)
        else:
            self.where(boolean_expression)
        return self

    def group_by(self, expressions: List[Expression]) -> 'SelectStatement':
        self._group_by = GroupBy(expressions)
        return self

    def order_by(self, expressions: List[Expression]) -> 'SelectStatement':
        self._order_by = OrderBy(expressions)
        return self

    def limit(self, limit: int) -> 'SelectStatement':
        self._limit = Limit(limit)
        return self

    def _render_joins(self):
        return '\n'.join([j.to_sql() for j in self._joins])

    def to_sql(self) -> str:
        return f"""{self._select.to_sql()}
{self._from.to_sql() if self._from else ''}
{self._render_joins()}
{self._where.to_sql() if self._where else ''}
{self._group_by.to_sql() if self._group_by else ''}
{self._order_by.to_sql() if self._order_by else ''}
{self._limit.to_sql() if self._limit else ''}"""


class UnionStatement(Statement):
    def __init__(self, select_statements, union_all=True):
        super().__init__()
        self.select_statements = select_statements
        self.union_all = union_all

    def _union_separator(self):
        return 'UNION ALL\n' if self.union_all else 'UNION'

    def to_sql(self) -> str:
        return f'\n {self._union_separator()}'.join(
            [select_statement.to_sql() for select_statement in self.select_statements])

class QueryBuilder(SqlToken):
    def __init__(self):
        self.cte_map: Dict[str, Cte] = {}
        self.statement = None

    def select(self, statement: Statement):
        self.statement = statement

    def with_cte(self, cte: Cte) -> 'QueryBuilder':
        # by convention, those ctes are going to the top of the chain
        if cte.cte_name.startswith("_"):
            self.cte_map = {cte.cte_name: cte} | self.cte_map
        else:
            self.cte_map[cte.cte_name] = cte
        return self

    def to_sql(self) -> str:
        ctes = ''
        if self.cte_map:
            ctes = 'WITH ' + ',\n'.join(cte.to_definition_to_sql() for _, cte in self.cte_map.items()) + '\n'
        sql = ctes + self.statement.to_sql()
        cleaned_sql = "\n".join([s for s in sql.split("\n") if s])
        return cleaned_sql
