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

from typing import List, Set, Dict, Tuple, Callable
from fastapi.logger import logger

from pandas import DataFrame, MultiIndex, unique
from pandas.core.groupby import GroupBy

from queryengine.core import constants
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.timegrain import TimeGrain


def _safe_division(num, denom, default_value=0):
    if denom == 0:
        return default_value
    return num / denom


class TabularDataResult:
    def __init__(self, df: DataFrame):
        self.df = df
        # self.df.set_index(self._merge_columns())

    def __add__(self, other):
        if self.df.empty:
            return self
        if isinstance(other, (int, float)):
            return TabularDataResult(self.df.assign(**{
                constants.DATA_COLUMN_ALIAS: lambda x: x[constants.DATA_COLUMN_ALIAS] + other}))
        if isinstance(other, TabularDataResult):
            if other.df.empty:
                return other
            return self.combine_values(other, lambda x, y: x + y)
        raise ValueError(f'Unsupported type {type(other)}')

    def __radd__(self, other):
        return self.__add__(other)

    def __sub__(self, other):
        return self.__add__(-other)

    def __rsub__(self, other):
        return -self.__sub__(other)

    def __mul__(self, other):
        if self.df.empty:
            return self
        if isinstance(other, (int, float)):
            return TabularDataResult(self.df.assign(**{
                constants.DATA_COLUMN_ALIAS: lambda x: x[constants.DATA_COLUMN_ALIAS] * other}))
        if isinstance(other, TabularDataResult):
            if other.df.empty:
                return other
            return self.combine_values(other, lambda x, y: x * y)
        raise ValueError(f'Unsupported type {type(other)}')

    def __rmul__(self, other):
        return self.__mul__(other)

    def __truediv__(self, other):
        if self.df.empty:
            return self
        if isinstance(other, (int, float)):
            return TabularDataResult(self.df.assign(**{
                constants.DATA_COLUMN_ALIAS: lambda x: _safe_division(x[constants.DATA_COLUMN_ALIAS], other)}))
        if isinstance(other, TabularDataResult):
            if other.df.empty:
                return other
            return self.combine_values(other, lambda x, y: _safe_division(x, y))
        raise ValueError(f'Unsupported type {type(other)}')

    def __rtruediv__(self, other):
        if self.df.empty:
            return self
        if isinstance(other, (int, float)):
            return TabularDataResult(self.df.assign(**{
                constants.DATA_COLUMN_ALIAS: lambda x: _safe_division(other, x[constants.DATA_COLUMN_ALIAS])}))
        if isinstance(other, TabularDataResult):
            if other.df.empty:
                return other
            return self.combine_values(other, lambda x, y: _safe_division(y, x))
        raise ValueError(f'Unsupported type {type(other)}')

    def __neg__(self):
        return self.__mul__(-1)

    def __eq__(self, other):
        return self.df.equals(other.df)

    def _merge_columns(self) -> List[str]:
        return [constants.X_AXIS_COLUMN_ALIAS] + self.group_by_columns()

    def group_by_columns(self) -> List[str]:
        return self.df.columns.tolist()[1:-1]

    def group_by_values(self) -> List[Tuple]:
        return [tuple(x) for x in self.df[self.group_by_columns()].values if len(x) > 0]

    def get_top_n_values(self, n: int) -> 'TabularDataResult':
        df = self.df.sort_values(constants.DATA_COLUMN_ALIAS, ascending=False).head(n)
        return TabularDataResult(df)

    def get_bottom_n_values(self, n: int) -> 'TabularDataResult':
        df = self.df.sort_values(constants.DATA_COLUMN_ALIAS, ascending=True).head(n)
        return TabularDataResult(df)

    def filter_by_group_by_values(self, group_by_values: List[Tuple]) -> 'TabularDataResult':
        if self.df.empty or len(group_by_values) == 0:
            return self
        sorting_score = {v: idx for idx, v in enumerate(group_by_values)}
        # df = self.df[self.df[self.group_by_columns()].apply(tuple, axis=1).isin(group_by_values)]
        df = self.df[MultiIndex.from_arrays([self.df[x] for x in self.group_by_columns()]).isin(group_by_values)]
        df['_sorting_score'] = df[self.group_by_columns()].apply(tuple, axis=1).map(lambda key: sorting_score[key])
        df.sort_values(by=[constants.X_AXIS_COLUMN_ALIAS, '_sorting_score'], inplace=True)
        df.drop(['_sorting_score'], inplace=True, axis=1)
        return TabularDataResult(df)

    def trim_zeros(self) -> 'TabularDataResult':
        if self.df.empty:
            return self

        def trim_zeros_from_start_and_end(row):
            # Check if all values are zero
            if row.eq(0).all():
                row.loc[:] = True
                return row
            first_non_zero = row.ne(0).idxmax() - 1
            last_non_zero = row[::-1].ne(0).idxmax() + 1
            row.loc[:] = False
            row.loc[:first_non_zero] = True
            row.loc[last_non_zero:] = True

            return row

        result_df = self.df.sort_values(self.group_by_columns() + [constants.X_AXIS_COLUMN_ALIAS])#.reset_index(drop=True)
        grouped_df = result_df.groupby(self.group_by_columns(), dropna=False) if len(self.group_by_columns()) > 0 else result_df
        result_df['trim_row'] = grouped_df[
            constants.DATA_COLUMN_ALIAS].apply(lambda x: trim_zeros_from_start_and_end(x.copy())) if len(
            self.group_by_columns()) > 0 else trim_zeros_from_start_and_end(grouped_df[
                                                                                constants.DATA_COLUMN_ALIAS].copy())
        
        return_df = self.df.drop(self.df[result_df['trim_row'] == True].index)
        return TabularDataResult(return_df)

    def filter(self, filter: Callable[[object], bool]):
        if self.df.empty:
            return self
        df = self.df[self.df[constants.X_AXIS_COLUMN_ALIAS].map(filter)]
        return TabularDataResult(df)

    def map_x_axis(self, mapper: Callable[[object], object]) -> 'TabularDataResult':
        if self.df.empty:
            return self
        df = self.df.assign(**{constants.X_AXIS_COLUMN_ALIAS: lambda x: mapper(x[constants.X_AXIS_COLUMN_ALIAS])})
        return TabularDataResult(df)

    def map_group_by_columns(self, mapper: Callable[[object], object]) -> 'TabularDataResult':
        mapper_dict = {}

        def make_func(column):
            return lambda x: mapper(x[column])

        for group_by_column in self.group_by_columns():
            mapper_dict[group_by_column] = make_func(group_by_column)
        df = self.df.assign(**mapper_dict)
        return TabularDataResult(df)

    def merge_values(self, table_result: 'TabularDataResult') -> 'TabularDataResult':
        merged = self.df.merge(table_result.df, on=self._merge_columns(), how='left', suffixes=('_1', '_2'))
        merged[constants.DATA_COLUMN_ALIAS] = merged[f'{constants.DATA_COLUMN_ALIAS}_2'].fillna(
            merged[f'{constants.DATA_COLUMN_ALIAS}_1'])
        df = merged[table_result.df.columns.values.tolist()]
        return TabularDataResult(df)

    def combine_values(self, table_result: 'TabularDataResult',
                       combiner: Callable[[float, float], float]) -> 'TabularDataResult':
        merged = self.df.merge(table_result.df, on=self._merge_columns(), suffixes=('_1', '_2'))
        merged[constants.DATA_COLUMN_ALIAS] = merged.apply(lambda row: combiner(
            row[f'{constants.DATA_COLUMN_ALIAS}_1'],
            row[f'{constants.DATA_COLUMN_ALIAS}_2'],
        ), axis=1)
        return TabularDataResult(merged[self.df.columns.tolist()])

    def group_by_x_axis(self, reducer: Callable[[GroupBy], DataFrame]) -> 'TabularDataResult':
        df = reducer(self.df.groupby(self._merge_columns(), as_index=False, sort=False))
        return TabularDataResult(df[self.df.columns.tolist()])

    def group_by_group_by_values(self, reducer: Callable[[GroupBy], DataFrame]) -> 'TabularDataResult':
        if not self.group_by_columns():
            return self
        df = reducer(self.df.groupby(self._merge_columns(), as_index=False, sort=False))
        return TabularDataResult(df[self.df.columns.tolist()])

    @classmethod
    def _from_x_axis_values(cls, x_axis_values: List, group_by_columns: List[str], group_by_values: Set[Tuple]):
        x_axis_repeated_values = []
        for x_axis_value in x_axis_values:
            for i in range(max(1, len(group_by_values))):
                x_axis_repeated_values.append(x_axis_value)
        data: Dict[str, List[object]] = {
            constants.X_AXIS_COLUMN_ALIAS: x_axis_repeated_values,
        }

        # add group by series
        for group_by_column in group_by_columns:
            data[group_by_column] = []
        for group_by_value in sorted(group_by_values, key=lambda x: (None in x, x)):
            for idx, group_by_column in enumerate(group_by_columns):
                data[group_by_column].append(group_by_value[idx])
        for group_by_column in group_by_columns:
            # if len(data[group_by_column]) == 0:
            #     data[group_by_column].append(None)
            data[group_by_column] *= max(1, len(x_axis_values))

        data[constants.DATA_COLUMN_ALIAS] = [0] * len(data[constants.X_AXIS_COLUMN_ALIAS])
        return TabularDataResult(DataFrame(data))

    @classmethod
    def from_cohort_days(cls, days: int, group_by_columns: List[str],
                         group_by_values: Set[Tuple]) -> 'TabularDataResult':
        return TabularDataResult._from_x_axis_values(
            x_axis_values=list(range(days)),
            group_by_columns=group_by_columns,
            group_by_values=group_by_values)

    @classmethod
    def from_date_interval(cls, date_interval: DatetimeInterval, time_grain: TimeGrain, group_by_columns: List[str],
                           group_by_values: Set[Tuple]) -> 'TabularDataResult':
        return TabularDataResult._from_x_axis_values(
            x_axis_values=date_interval.generate_all_dates(time_grain),
            group_by_columns=group_by_columns,
            group_by_values=group_by_values)


class TabularDataResults:
    def __init__(self, results_map: Dict[str, TabularDataResult] = None):
        self.results_map: Dict[str, TabularDataResult] = results_map or {}

    def add(self, id: str, tabular_data_result: TabularDataResult):
        self.results_map[id] = tabular_data_result

    def group_by_values(self):
        group_by_values = set()
        for id, result in self.results_map.items():
            group_by_values = group_by_values.union(result.group_by_values())
        return group_by_values
    
    def group_by_columns_distinct_values_count(self):
        group_by_values = set()
        for id, result in self.results_map.items():
            group_by_values = group_by_values.union(unique(result.df[result.group_by_columns()].values.ravel('K')))
        return len(group_by_values)

    def group_by_columns(self) -> List[str]:
        return list(self.results_map.values())[0].group_by_columns()

    def map_x_axis(self, mapper: Callable[[object], object]) -> 'TabularDataResults':
        for result_id in self.results_map.keys():
            self.results_map[result_id] = self.results_map[result_id].map_x_axis(mapper)
        return self

    def filter(self, filter: Callable[[object], bool]) -> 'TabularDataResults':
        for result_id in self.results_map.keys():
            self.results_map[result_id] = self.results_map[result_id].filter(filter)
        return self

    def filter_by_group_by_values(self, group_by_values: List[Tuple]) -> 'TabularDataResults':
        for result_id in self.results_map.keys():
            self.results_map[result_id] = self.results_map[result_id].filter_by_group_by_values(group_by_values)
        return self
    
    def get_max_rows(self):
        max_rows = 0
        for result_id in self.results_map.keys():
            max_rows = max(max_rows, len(self.results_map[result_id].df))
        return max_rows
