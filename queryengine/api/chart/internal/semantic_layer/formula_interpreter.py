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

import ast
from typing import Dict

from queryengine.core import constants
from queryengine.core.tabular_data_result import TabularDataResult

builtin_functions = {}


def evaluate(formula: str, identity: TabularDataResult, values: Dict[str, object]) -> TabularDataResult:
    def _evaluate_node(node):
        if isinstance(node, ast.Name):
            return values[node.id.lower()]
        elif isinstance(node, ast.Constant) and type(node.value) in [int, float]:
            return node.value
        elif isinstance(node, ast.Call):
            func = builtin_functions[node.func.id.lower()]
            args = [_evaluate_node(arg) for arg in node.args]
            return func(*args)
        elif isinstance(node, ast.BinOp):
            if isinstance(node.op, ast.Add):
                return _evaluate_node(node.left) + _evaluate_node(node.right)
            elif isinstance(node.op, ast.Sub):
                return _evaluate_node(node.left) - _evaluate_node(node.right)
            elif isinstance(node.op, ast.Mult):
                return _evaluate_node(node.left) * _evaluate_node(node.right)
            elif isinstance(node.op, ast.Div):
                return _evaluate_node(node.left) / _evaluate_node(node.right)
            else:
                raise ValueError(f'Not supported operator {node.op}')
        else:
            raise ValueError(f"Unsupported AST node type: {type(node)}")

    if identity.df.empty:
        return identity

    expr_ast = ast.parse(formula, mode='eval')
    expr_ast = ast.fix_missing_locations(expr_ast)
    expr_ast = ast.copy_location(expr_ast, expr_ast)
    expr_ast = ast.fix_missing_locations(expr_ast)
    result = _evaluate_node(expr_ast.body)
    if type(result) in [int, float]:
        identity.df[constants.DATA_COLUMN_ALIAS] = identity.df[constants.DATA_COLUMN_ALIAS].apply(lambda x: result)
        return identity
    elif isinstance(result, TabularDataResult):
        return identity.merge_values(result)
    else:
        raise Exception(f'Not supported result type {type(result)}')
