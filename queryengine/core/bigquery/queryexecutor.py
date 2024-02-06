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

import os
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, Future
from threading import Thread, BoundedSemaphore
from typing import Callable

from expiringdict import ExpiringDict
from fastapi.logger import logger
from google.cloud.bigquery import Client, QueryJobConfig
from opentelemetry import context as otel_context
from opentelemetry import trace

from queryengine.core.tabular_data_result import TabularDataResult, TabularDataResults
from queryengine.core.warehouse import FutureResult, TooManyRequestsException, TooManyRowsException


class BigQueryQuery:
    def __init__(self, page_id: str, request_id: str, metric_id: str, sql: str, app_id: str):
        self.page_id = page_id
        self.request_id = request_id
        self.metric_id = metric_id
        self.sql = sql
        self.app_id = app_id

    def get_environment(self):
        if os.environ.get('SERVICE_SUFFIX', '') == '':
            return 'production'
        return os.environ.get('SERVICE_SUFFIX', '')


class BigQueryFutureResult(FutureResult):
    def __init__(self, futures):
        self._futures = futures

    def get(self) -> TabularDataResults:
        query_results = TabularDataResults()
        for symbol_id, query, future in self._futures:
            query_result = future.result()
            query_results.add(symbol_id, query_result)
        return query_results


class BigQueryExecutor(ABC):
    @abstractmethod
    def execute(self, query: BigQueryQuery) -> Future[TabularDataResult]:
        pass

    def _on_query_start(self, query: BigQueryQuery, job_id: str):
        pass

    def _on_query_end(self, query: BigQueryQuery, job_id: str):
        pass

    def cancel_by_page_id(self, page_id: str):
        pass

    def cancel_by_request_id(self, request_id: str):
        pass


class TracedThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def with_otel_context(self, context: otel_context.Context, fn: Callable):
        otel_context.attach(context)
        return fn()

    def submit(self, fn, *args, **kwargs):
        # get the current otel context
        context = otel_context.get_current()
        if context:
            return super().submit(
                lambda: self.with_otel_context(
                    context, lambda: fn(*args, **kwargs)
                ),
            )
        else:
            return super().submit(lambda: fn(*args, **kwargs))


class BoundedExecutor:
    def __init__(self, max_workers):
        self.executor = TracedThreadPoolExecutor(max_workers=max_workers)
        self.semaphore = BoundedSemaphore(max_workers)

    def submit(self, fn, *args, **kwargs):
        if not self.semaphore.acquire(blocking=False):
            raise TooManyRequestsException()

        try:
            future = self.executor.submit(fn, *args, **kwargs)
        except:
            self.semaphore.release()
            raise
        else:
            future.add_done_callback(lambda x: self.semaphore.release())
            return future

    def shutdown(self, wait=True):
        self.executor.shutdown(wait)


class SimpleBigQueryExecutor(BigQueryExecutor):
    def __init__(self, project: str, threads: int, max_rows: int):
        self.client = Client(project=project)
        self.executor = BoundedExecutor(max_workers=threads)
        self.max_rows = max_rows

    def _execute_sync(self, query: BigQueryQuery) -> TabularDataResult:
        tracer = trace.get_tracer("query_engine")

        with tracer.start_as_current_span(f"query {query.metric_id}"):
            with tracer.start_as_current_span("execution"):
                job_config = QueryJobConfig(use_query_cache=True)
                job_config.labels = {
                    'app_id': query.app_id,
                    'environment': query.get_environment(),
                    'service': 'query_engine',
                    'metric_id': query.metric_id.replace('.', '_'),
                }
                query_job = self.client.query(query.sql, job_config=job_config)
                logger.info(f'Running query: \n{query_job.query}')
                self._on_query_start(query, query_job.job_id)
                result = query_job.result(max_results=self.max_rows)
                if result.total_rows > self.max_rows:
                    raise TooManyRowsException()
            with tracer.start_as_current_span("data import"):
                df = result.to_dataframe()
        self._on_query_end(query, query_job.job_id)
        return TabularDataResult(df)

    def execute(self, query: BigQueryQuery) -> Future[TabularDataResult]:
        return self.executor.submit(self._execute_sync, query)  # TODO TooManyRequestsException


class CancellableBigQueryExecutor(BigQueryExecutor):
    def __init__(self, backing_bigquery_executor: SimpleBigQueryExecutor):
        self.backing_bigquery_executor = backing_bigquery_executor
        self.cancelled_request_ids = ExpiringDict(max_len=100, max_age_seconds=60)
        self.cancelled_page_ids = ExpiringDict(max_len=100, max_age_seconds=60)
        self.jobs_by_request_id = defaultdict(set)
        self.jobs_by_page_id = defaultdict(set)
        self.cancel_thread = Thread(target=self._periodic_cancel)
        self.cancel_thread.start()

    def _on_query_start(self, query: BigQueryQuery, job_id: str):
        super(CancellableBigQueryExecutor, self)._on_query_start(query, job_id)
        if query.request_id in self.cancelled_request_ids or query.page_id in self.cancelled_page_ids:
            raise Exception('Query cancelled')
        self.jobs_by_request_id[query.request_id].add(job_id)
        self.jobs_by_page_id[query.page_id].add(job_id)

    def _on_query_end(self, query: BigQueryQuery, job_id: str):
        super(CancellableBigQueryExecutor, self)._on_query_end(query, job_id)
        request_id_jobs = self.jobs_by_request_id[query.request_id]
        request_id_jobs.remove(job_id)
        if not request_id_jobs:
            del self.jobs_by_request_id[query.request_id]

        page_id_jobs = self.jobs_by_page_id[query.page_id]
        page_id_jobs.remove(job_id)
        if not page_id_jobs:
            del self.jobs_by_page_id[query.page_id]

    def cancel_by_page_id(self, page_id: str):
        self.cancelled_page_ids[page_id] = True

    def cancel_by_request_id(self, request_id: str):
        self.cancelled_request_ids[request_id] = True

    def _periodic_cancel(self):
        while True:
            for page_id in self.cancelled_page_ids:
                for job_id in self.jobs_by_page_id[page_id]:
                    try:
                        self.backing_bigquery_executor.client.cancel_job(job_id=job_id)
                    except:
                        pass  # TODO log here
                    finally:
                        del self.cancelled_page_ids[page_id]

            for request_id in self.cancelled_request_ids:
                for job_id in self.jobs_by_request_id[request_id]:
                    try:
                        self.backing_bigquery_executor.client.cancel_job(job_id=job_id)
                    except:
                        pass  # TODO log here
                    finally:
                        del self.cancelled_request_ids[request_id]
            time.sleep(5)

    def execute(self, query: BigQueryQuery) -> Future[TabularDataResult]:
        return self.backing_bigquery_executor.execute(query)
