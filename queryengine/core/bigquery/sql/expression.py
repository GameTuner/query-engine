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

import string
from typing import Dict, Callable, MutableMapping, Set

from queryengine.core.bigquery.sql.base import SqlToken
from queryengine.core.datasource.datasource import DataType


class DotsFormatter(string.Formatter):
    """
    Used to support dots in python string formats (i.e. {params.value})
    Default python string format does not support it
    """
    def get_field(self, field_name, args, kwargs):
        return self.get_value(field_name, args, kwargs), field_name

    def get_format_strings(self, format_string: str) -> Dict[str, object]:
        # Use dict instead of set because we want to preserve order
        return {v[1]: None for v in self.parse(format_string) if v[1]}


class TemplateDict(MutableMapping):
    def __init__(self, mapping: Dict[str, SqlToken | str], on_missing: Callable[[str], SqlToken | str] | None = None):
        self._mapping = mapping
        self._on_missing = on_missing

    def __getitem__(self, key):
        if key not in self._mapping:
            if not self._on_missing:
                raise KeyError(key)
            token = self._on_missing(key)
        else:
            token = self._mapping[key]
        return token.to_sql() if isinstance(token, SqlToken) else str(token)

    def __setitem__(self, key, value):
        self._mapping[key] = value

    def __delitem__(self, key):
        del self._mapping[key]

    def __iter__(self):
        return iter(self._mapping)

    def __len__(self):
        return len(self._mapping)


class Expression(SqlToken):
    def __init__(self, expression: str, template_dict: TemplateDict = None):
        super().__init__()
        self.expression = expression
        self.template_dict = template_dict

    def to_definition_sql(self):
        return self.to_sql()

    def to_reference_sql(self):
        return self.to_sql()

    def to_sql(self) -> str:
        return DotsFormatter().vformat(self.expression, [], self.template_dict)

    def as_alias(self, alias: str) -> 'AliasedExpression':
        return AliasedExpression(self, alias)


class Constant(Expression):
    def __init__(self, value: str, data_type: DataType):
        super().__init__(self._expression(value, data_type))

    def _expression(self, value: str, data_type: DataType) -> str:
        if data_type == DataType.string:
            return f"'{value}'"
        if data_type == DataType.date:
            return f"DATE '{value}'"
        if data_type == DataType.datetime:
            return f"TIMESTAMP '{value}'"
        elif data_type in [DataType.number, DataType.integer, DataType.boolean]:
            return value
        else:
            raise Exception(f'Unknown type {data_type}')


class AliasedExpression(Expression):
    def __init__(self, expression: Expression, alias: str):
        super().__init__(expression.expression, expression.template_dict)
        self.alias = alias

    def to_definition_sql(self):
        if self.alias:
            return f'{super().to_sql()} AS {self.alias}'
        else:
            return self.to_sql()

    def to_reference_sql(self):
        if self.alias:
            return self.alias
        else:
            return self.to_sql()
