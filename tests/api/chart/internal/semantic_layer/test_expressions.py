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

from datetime import datetime

import pytz
from pandas import DataFrame
from pandas.testing import assert_frame_equal

from queryengine.api.chart.internal.semantic_layer import formula_interpreter
from queryengine.core import constants
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.tabular_data_result import TabularDataResult
from queryengine.core.timegrain import TimeGrain


def get_identity():
    return TabularDataResult.from_date_interval(
        date_interval=DatetimeInterval(
            date_from=datetime(2023, 1, 1),
            date_to=datetime(2023, 1, 3),
        ),
        time_grain=TimeGrain.day,
        group_by_columns=[],
        group_by_values=set()
    )


def test_simple_dataframe_return():
    identity = get_identity()
    values = {
        'x': identity.merge_values(TabularDataResult(
            DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [datetime(2023, 1, 1).replace(tzinfo=pytz.UTC)],
                constants.DATA_COLUMN_ALIAS: [100]
            }),
        ))
    }

    result = formula_interpreter.evaluate(
        identity=identity,
        formula='x',
        values=values,
    )

    assert_frame_equal(result.df, DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [
            datetime(2023, 1, 1).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 2).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 3).replace(tzinfo=pytz.UTC)],
        constants.DATA_COLUMN_ALIAS: [100.0, 0.0, 0.0]
    }), check_like=True)


def test_dataframe_and_constant_expression():
    identity = get_identity()
    values = {
        'x': identity.merge_values(TabularDataResult(
            DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [datetime(2023, 1, 1).replace(tzinfo=pytz.UTC)],
                constants.DATA_COLUMN_ALIAS: [100]
            }),
        )),
        'y': 100
    }

    result = formula_interpreter.evaluate(
        identity=identity,
        formula='x + y + 1',
        values=values,
    )

    assert_frame_equal(result.df, DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [
            datetime(2023, 1, 1).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 2).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 3).replace(tzinfo=pytz.UTC)],
        constants.DATA_COLUMN_ALIAS: [201.0, 101.0, 101.0]
    }), check_like=True)


def test_dataframe_and_dataframe_expression():
    identity = get_identity()
    values = {
        'x': identity.merge_values(TabularDataResult(
            DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [datetime(2023, 1, 1).replace(tzinfo=pytz.UTC)],
                constants.DATA_COLUMN_ALIAS: [100]
            }),
        )),
        'y': identity.merge_values(TabularDataResult(
            DataFrame({
                constants.X_AXIS_COLUMN_ALIAS: [
                    datetime(2023, 1, 1).replace(tzinfo=pytz.UTC),
                    datetime(2023, 1, 2).replace(tzinfo=pytz.UTC)],
                constants.DATA_COLUMN_ALIAS: [100, 200]
            }),
        )),
    }

    result = formula_interpreter.evaluate(
        identity=identity,
        formula='x + y + 1',
        values=values,
    )

    assert_frame_equal(result.df, DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [
            datetime(2023, 1, 1).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 2).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 3).replace(tzinfo=pytz.UTC)],
        constants.DATA_COLUMN_ALIAS: [201.0, 201.0, 1.0]
    }), check_like=True)


def test_simple_constant_return():
    identity = get_identity()
    values = {}

    result = formula_interpreter.evaluate(
        identity=identity,
        formula='100',
        values=values,
    )

    assert_frame_equal(result.df, DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [
            datetime(2023, 1, 1).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 2).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 3).replace(tzinfo=pytz.UTC)],
        constants.DATA_COLUMN_ALIAS: [100, 100, 100]
    }), check_like=True)


def test_simple_expression():
    identity = get_identity()
    values = {
        'x': 100,
        'y': 50
    }

    result = formula_interpreter.evaluate(
        identity=identity,
        formula='100 + (x + y)',
        values=values,
    )

    assert_frame_equal(result.df, DataFrame({
        constants.X_AXIS_COLUMN_ALIAS: [
            datetime(2023, 1, 1).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 2).replace(tzinfo=pytz.UTC),
            datetime(2023, 1, 3).replace(tzinfo=pytz.UTC)],
        constants.DATA_COLUMN_ALIAS: [250, 250, 250]
    }), check_like=True)
