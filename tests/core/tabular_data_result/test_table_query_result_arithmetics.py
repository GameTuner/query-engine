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

from queryengine.core import constants
from queryengine.core.tabular_data_result import TabularDataResult


def test_addition():
    table_query_result_1 = TabularDataResult(DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [0, 1, 2, 3],
        constants.DATA_COLUMN_ALIAS: [0, 1, 2, 3]
    }))
    table_query_result_2 = TabularDataResult(DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [0, 1, 2, 3],
        constants.DATA_COLUMN_ALIAS: [0, 1, 2, 3]
    }))

    assert table_query_result_1 + table_query_result_2 == TabularDataResult(DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [0, 1, 2, 3],
        constants.DATA_COLUMN_ALIAS: [0, 2, 4, 6]
    }))
