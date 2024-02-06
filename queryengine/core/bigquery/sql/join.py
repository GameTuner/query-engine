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

from enum import Enum

from queryengine.core.bigquery.sql.base import SqlToken
from queryengine.core.bigquery.sql.boolean_expression import BooleanExpression
from queryengine.core.bigquery.sql.table_like import TableLike


class JoinType(Enum):
    INNER = 'INNER'
    LEFT = 'LEFT'


class Join(SqlToken):
    def __init__(self, join_type: JoinType, table: TableLike):
        self.table = table
        self.join_type = join_type
        self.boolean_expression: BooleanExpression | None = None

    @staticmethod
    def inner(table: TableLike) -> 'Join':
        return Join(JoinType.INNER, table)

    def on(self, boolean_expression: BooleanExpression):
        self.boolean_expression = boolean_expression
        return self

    def and_(self, boolean_expression: BooleanExpression):
        self.boolean_expression.and_(boolean_expression)
        return self

    def or_(self, boolean_expression: BooleanExpression):
        self.boolean_expression.or_(boolean_expression)
        return self

    def to_sql(self) -> str:
        # TODO handle case when there is no "ON"
        return f'{self.join_type.value} JOIN {self.table.to_sql()} ON {self.boolean_expression.to_sql()}'
