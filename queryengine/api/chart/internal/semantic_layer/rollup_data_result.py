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

from typing import Callable, Dict, List, Tuple

from queryengine.core.tabular_data_result import TabularDataResult

rollup_functions = {
    'sum': lambda group_by: group_by.sum(),
    'avg': lambda group_by: group_by.mean(),
    'count': lambda group_by: group_by.count(),
}


class RollupDataResult:
    def __init__(self, warehouse_result: TabularDataResult, x_axis_rollup_function_name: str,
                 y_axis_rollup_function_name: str):
        self.warehouse_result = warehouse_result
        self.x_axis_rollup_function_name = x_axis_rollup_function_name
        self.y_axis_rollup_function_name = y_axis_rollup_function_name

    def rollup(self,
               x_axis_mapper: Callable[[object], object] = lambda x: x,
               group_by_columns_mapper: Callable[[object], object] = lambda x: x) -> TabularDataResult:
        return self.warehouse_result \
            .map_group_by_columns(group_by_columns_mapper) \
            .group_by_group_by_values(rollup_functions[self.y_axis_rollup_function_name.lower()]) \
            .map_x_axis(x_axis_mapper) \
            .group_by_x_axis(rollup_functions[self.x_axis_rollup_function_name.lower()])

    def filter(self, filter: Callable[[object], bool]) -> 'RollupDataResult':
        return RollupDataResult(
            self.warehouse_result.filter(filter),
            self.x_axis_rollup_function_name,
            self.y_axis_rollup_function_name)

    def filter_by_group_by_values(self, group_by_values: List[Tuple]) -> 'RollupDataResult':
        return RollupDataResult(
            self.warehouse_result.filter_by_group_by_values(group_by_values),
            self.x_axis_rollup_function_name,
            self.y_axis_rollup_function_name)

    def trim_zeros(self) -> 'RollupDataResult':
        return RollupDataResult(
            self.warehouse_result.trim_zeros(),
            self.x_axis_rollup_function_name,
            self.y_axis_rollup_function_name)


class RollupDataResults:
    def __init__(self):
        self._results_map: Dict[str, RollupDataResult] = {}

    def add(self, id: str, rollup_results: 'RollupDataResult'):
        self._results_map[id] = rollup_results

    def trim_zeros(self):
        for id, result in self._results_map.items():
            self._results_map[id] = result.trim_zeros()
        return self

    def group_by_values(self):
        group_by_values = set()
        for id, result in self._results_map.items():
            group_by_values = group_by_values.union(result.warehouse_result.group_by_values())
        return group_by_values

    def rollup(self,
               x_axis_mapper: Callable[[object], object] = lambda x: x,
               group_by_columns_mapper: Callable[[object], object] = lambda x: x) -> Dict[str, TabularDataResult]:
        return {id: result.rollup(x_axis_mapper, group_by_columns_mapper) for id, result in self._results_map.items()}

    def filter(self, filter: Callable[[object], bool]) -> 'RollupDataResults':
        for result_id in self._results_map.keys():
            self._results_map[result_id] = self._results_map[result_id].filter(filter)
        return self

    def filter_by_group_by_values(self, group_by_values: List[Tuple]) -> 'RollupDataResults':
        for result_id in self._results_map.keys():
            self._results_map[result_id] = self._results_map[result_id].filter_by_group_by_values(group_by_values)
        return self
