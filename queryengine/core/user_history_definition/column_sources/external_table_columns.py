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

from typing import List
import hashlib

from queryengine.core import constants
from queryengine.core.bigquery.sql.boolean_expression import BooleanExpression
from queryengine.core.bigquery.sql.expression import Expression, TemplateDict
from queryengine.core.bigquery.sql.join import JoinType, Join
from queryengine.core.bigquery.sql.sql_builder import QueryBuilder, SelectStatement
from queryengine.core.bigquery.sql.table_like import Table, Cte, TableLike
from queryengine.core.dateinterval import DatetimeInterval
from queryengine.core.user_history_definition.user_history_definition import ExternalTableColumn


def get_column(external_table_column: ExternalTableColumn, date_intervals: List[DatetimeInterval], sql_builder: QueryBuilder,
               select_statement: SelectStatement, allow_materialized: bool) -> Expression:

    formula_column = _formula_column(external_table_column)
    user_history_table = select_statement.get_table()

    cte = _build_cte(external_table_column)
    if cte.cte_name in sql_builder.cte_map:
        cte = sql_builder.cte_map[cte.cte_name]
        if not isinstance(cte.select, SelectStatement):
            raise Exception(f'Cte {cte.cte_name} not compatible!')
        cte.select.get_select().expressions.append(formula_column)
    else:
        if _is_column_materialized_for_whole_period(external_table_column, date_intervals) and allow_materialized:
            cte = None
        else:
            _register_and_join_cte(cte, select_statement, sql_builder, user_history_table)

    return _build_user_history_final_column(cte, external_table_column, select_statement, user_history_table,
                                            date_intervals, allow_materialized)


def _build_user_history_final_column(cte: Cte, external_table_column: ExternalTableColumn,
                                     select_statement: SelectStatement, user_history_table: TableLike,
                                     date_intervals: List[DatetimeInterval], allow_materialized: bool):
    materialized_column = user_history_table.column(external_table_column.name)
    if not cte:
        expression = Expression(materialized_column.to_sql())
    else:
        external_column = cte.column(external_table_column.name)
        if _is_column_not_materialized_for_whole_period(external_table_column, date_intervals) or not allow_materialized:
            expression = external_column
        else:
            expression = Expression("IF({date} < '{materialized_from}', {external}, {user_history})", template_dict=TemplateDict({
                "date": select_statement.get_table().column(constants.DATE_PARTITION_COLUMN_NAME),
                "materialized_from": external_table_column.materialized_from,
                "external": external_column,
                "user_history": materialized_column,
            }))

    return Expression(external_table_column.user_history_formula, template_dict=TemplateDict({
        external_table_column.name: expression.to_sql(),
    }))


def _formula_column(external_table_column: ExternalTableColumn):
    external_table = Table(external_table_column.dataset_name, external_table_column.table_name)
    return Expression(
        external_table_column.table_aggregation_formula,
        template_dict=TemplateDict({}, on_missing=lambda key: external_table.column(key))
    ).as_alias(external_table_column.name)


def _build_cte(external_table_column: ExternalTableColumn):
    cte_name = f'_external_{external_table_column.dataset_name}_{external_table_column.table_name}'
    if external_table_column.table_filter_formula:
        cte_name += f'_{hashlib.md5(external_table_column.table_filter_formula.encode()).hexdigest()[:10]}'
    external_table = Table(external_table_column.dataset_name, external_table_column.table_name)
    select = SelectStatement()\
        .select([
            Expression(external_table.column(constants.DATE_PARTITION_COLUMN_NAME).to_sql()),
            Expression(external_table.column(constants.UNIQUE_ID_COLUMN_NAME).to_sql()),
            _formula_column(external_table_column),
        ])\
        .from_(external_table) \
        .group_by([
            Expression(external_table.column(constants.DATE_PARTITION_COLUMN_NAME).to_sql()),
            Expression(external_table.column(constants.UNIQUE_ID_COLUMN_NAME).to_sql()),
        ])

    if external_table_column.table_filter_formula:
        select = select.where(BooleanExpression(Expression(
            external_table_column.table_filter_formula,
            template_dict=TemplateDict({}, on_missing=lambda key: external_table.column(key))
        )))
    return Cte(cte_name, select)


def _is_column_materialized_for_whole_period(external_table_column: ExternalTableColumn, date_intervals: List[DatetimeInterval]) -> bool:
    return external_table_column.materialized_from and \
           all([interval.date_from.date() >= external_table_column.materialized_from for interval in date_intervals])


def _is_column_not_materialized_for_whole_period(external_table_column: ExternalTableColumn, date_intervals: List[DatetimeInterval]) -> bool:
    return not external_table_column.materialized_from or \
           all([interval.date_to.date() < external_table_column.materialized_from for interval in date_intervals])


def _register_and_join_cte(cte: Cte, select_statement: SelectStatement, sql_builder: QueryBuilder, user_history_table: TableLike):
    sql_builder.with_cte(cte)
    select_statement.join(Join(join_type=JoinType.LEFT, table=cte).on(BooleanExpression.all_and_([
        BooleanExpression.as_(
            f'{user_history_table.column(constants.DATE_PARTITION_COLUMN_NAME).to_sql()} = {cte.column(constants.DATE_PARTITION_COLUMN_NAME).to_sql()}'),
        BooleanExpression.as_(
            f'{user_history_table.column(constants.UNIQUE_ID_COLUMN_NAME).to_sql()} = {cte.column(constants.UNIQUE_ID_COLUMN_NAME).to_sql()}'),
    ])))




