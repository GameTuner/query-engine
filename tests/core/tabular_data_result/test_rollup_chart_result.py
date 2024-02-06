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

from pandas import DataFrame

from queryengine.api.chart.internal.semantic_layer.rollup_data_result import RollupDataResult
from queryengine.core import constants
from queryengine.core.tabular_data_result import TabularDataResult


def test_rollup_no_group_by():
    rollup_table_result = RollupDataResult(
        warehouse_result=TabularDataResult(DataFrame({
            constants.X_AXIS_COLUMN_ALIAS: [0, 1, 2, 3],
            constants.DATA_COLUMN_ALIAS: [0, 1, 2, 3]
        })),
        x_axis_rollup_function_name='sum',
        y_axis_rollup_function_name='avg'
    )

    table_result = rollup_table_result.rollup(
        x_axis_mapper=lambda x: x // 2,
    )
    assert table_result == TabularDataResult(DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [0, 1],
        constants.DATA_COLUMN_ALIAS: [1, 5]
    }))


def test_rollup_group_by():
    rollup_table_result = RollupDataResult(
        warehouse_result=TabularDataResult(DataFrame({
            constants.X_AXIS_COLUMN_ALIAS: [0, 0, 1, 1],
            'g1': ['a', 'b', 'a', 'b'],
            'g2': ['c', 'c', 'c', 'd'],
            constants.DATA_COLUMN_ALIAS: [0, 1, 2, 3]
        })),
        x_axis_rollup_function_name='sum',
        y_axis_rollup_function_name='avg'
    )

    table_result = rollup_table_result.rollup(
        x_axis_mapper=lambda x: x // 2,
    )
    assert table_result == TabularDataResult(DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [0, 0, 0],
        'g1': ['a', 'b', 'b'],
        'g2': ['c', 'c', 'd'],
        constants.DATA_COLUMN_ALIAS: [2.0, 1.0, 3.0]
    }))


def test_rollup_to_single_value_group_by():
    rollup_table_result = RollupDataResult(
        warehouse_result=TabularDataResult(DataFrame({
            constants.X_AXIS_COLUMN_ALIAS: [0, 0, 1, 1],
            'g1': ['a', 'b', 'a', 'b'],
            'g2': ['c', 'c', 'c', 'd'],
            constants.DATA_COLUMN_ALIAS: [0, 1, 2, 3]
        })),
        x_axis_rollup_function_name='sum',
        y_axis_rollup_function_name='avg'
    )

    table_result = rollup_table_result.rollup(
        x_axis_mapper=lambda x: 0,
        group_by_columns_mapper=lambda x: 0
    )

    assert table_result == TabularDataResult(
        DataFrame({
            constants.X_AXIS_COLUMN_ALIAS: [0],
            'g1': [0],
            'g2': [0],
            constants.DATA_COLUMN_ALIAS: [3.0]
        }))
