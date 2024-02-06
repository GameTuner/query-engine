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
from typing import List

from queryengine.core.bigquery.sql.base import SqlToken
from queryengine.core.bigquery.sql.expression import Constant
from queryengine.core.bigquery.sql.expression import Expression
from queryengine.core.datasource.datasource import DataType
from queryengine.core.dateinterval import DatetimeInterval


class BooleanExpression(Expression):
    def __init__(self, expression: Expression):
        super().__init__(expression.expression, expression.template_dict)
        self.next_node: BooleanExpressionNode | None = None

    @staticmethod
    def as_(expression: str):
        return BooleanExpression(Expression(expression))

    @staticmethod
    def all_and_(boolean_expressions: List['BooleanExpression']):
        first = boolean_expressions[0]
        current = boolean_expressions[0]
        for boolean_expression in boolean_expressions[1:]:
            current.and_(boolean_expression)
        return first

    def _find_tail(self):
        boolean_expression = self
        while boolean_expression.next_node:
            boolean_expression = boolean_expression.next_node.boolean_expression
        return boolean_expression

    def and_(self, boolean_expression: 'BooleanExpression') -> 'BooleanExpression':
        tail = self._find_tail()
        tail.next_node = BooleanExpressionNode(BooleanOperator.AND, boolean_expression)
        return self

    def or_(self, boolean_expression: 'BooleanExpression') -> 'BooleanExpression':
        tail = self._find_tail()
        tail.next_node = BooleanExpressionNode(BooleanOperator.AND, boolean_expression)
        return self

    @staticmethod
    def from_filter(expression: Expression, operator: str, values: List[str],
                    data_type: DataType) -> 'BooleanExpression':
        #  TODO implement other operations
        constants = [Constant(value, data_type).to_sql() for value in values]
        if operator in ['<', '<=', '>', '>=', '=', '!=']:
            return BooleanExpression.as_(f'{expression.to_sql()} {operator} {constants[0]}')
        elif operator == 'like':
            return BooleanExpression.as_(f'{expression.to_sql()} LIKE {",".join(constants)}')
        elif operator == 'not_like':
            return BooleanExpression.as_(f'{expression.to_sql()} NOT LIKE {",".join(constants)}')
        elif operator == 'in':
            return BooleanExpression.as_(f'{expression.to_sql()} IN ({",".join(constants)})')
        elif operator == 'not_in':
            return BooleanExpression.as_(f'{expression.to_sql()} NOT IN ({",".join(constants)})')
        elif operator == 'is_null':
            return BooleanExpression.as_(f'{expression.to_sql()} IS NULL')
        elif operator == 'is_not_null':
            return BooleanExpression.as_(f'{expression.to_sql()} IS NOT NULL')
        # also works with null (null is true equals to false), but (null = true equals to null)
        elif operator == 'boolean_is':
            return BooleanExpression.as_(f'{expression.to_sql()} IS {constants[0]}')
        elif operator == 'boolean_is_not':
            return BooleanExpression.as_(f'{expression.to_sql()} IS NOT {constants[0]}')

        elif operator == 'not_in':
            return BooleanExpression.as_(f'{expression.to_sql()} NOT IN ({",".join(constants)})')
        elif operator == 'between':
            return BooleanExpression.as_(f'{expression.to_sql()} BETWEEN {constants[0]} AND {constants[1]}')
        else:
            raise Exception(f'unsupported operation {operator}')

    def _base_expression_to_sql(self):
        return super().to_sql()

    def to_sql(self) -> str:
        tail = self.next_node.to_sql() if self.next_node else ''
        return f"{self._base_expression_to_sql()}{tail}"

    @staticmethod
    def from_date(expression: Expression, date_interval: DatetimeInterval) -> 'BooleanExpression':
        return BooleanExpression.from_filter(
            expression,
            'between',
            [str(date_interval.date_from.date()), str(date_interval.date_to.date())], DataType.date)

    @staticmethod
    def from_datetime(expression: Expression, date_interval: DatetimeInterval) -> 'BooleanExpression':
        return BooleanExpression.from_filter(
            expression,
            'between',
            [str(date_interval.date_from), str(date_interval.date_to)], DataType.date)
    
    @staticmethod
    def from_timestamp(expression: Expression, date_interval: DatetimeInterval) -> 'BooleanExpression':
        return BooleanExpression.from_filter(
            expression,
            'between',
            [str(date_interval.date_from), str(date_interval.date_to)], DataType.datetime)


class BooleanOperator(Enum):
    AND = 'AND'
    OR = 'OR'


class BooleanExpressionNode(SqlToken):
    def __init__(self, boolean_operator: BooleanOperator, boolean_expression: BooleanExpression):
        super().__init__()
        self.boolean_operator = boolean_operator
        self.boolean_expression = boolean_expression

    def to_sql(self) -> str:
        return f' {self.boolean_operator.value} {self.boolean_expression.to_sql()}'


class BooleanExpressionParenthesis(BooleanExpression):
    def __init__(self, boolean_expression: BooleanExpression):
        self.expression = boolean_expression.to_sql()
        self.next_node = None
        self.template_dict = {}

    def _base_expression_to_sql(self):
        return f'({super()._base_expression_to_sql()})'
