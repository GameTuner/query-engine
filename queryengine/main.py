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

from fastapi import FastAPI
from fastapi.logger import logger
from opentelemetry import trace
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

import logging
from queryengine.api.cancel_query.web import router as cancel_query_router
from queryengine.api.chart.web import router as chart_router
from queryengine.api.column_values.web import router as column_values_router
from queryengine.api.datasource.web import router as datasource_router
from queryengine.api.healthcheck.web import router as healthcheck_router
from queryengine.api.event_errors.web import router as event_errors_router
from queryengine.logging.middleware import LoggingMiddleware
from queryengine.logging.setup_logging import setup_logging

app = FastAPI()

app.include_router(cancel_query_router)
app.include_router(chart_router)
app.include_router(column_values_router)
app.include_router(datasource_router)
app.include_router(healthcheck_router)
app.include_router(event_errors_router)

if os.environ.get('JSON_LOGS', '0') == '1':
    tracer_provider = TracerProvider()
    cloud_trace_exporter = CloudTraceSpanExporter()
    tracer_provider.add_span_processor(BatchSpanProcessor(cloud_trace_exporter))
    trace.set_tracer_provider(tracer_provider)

    setup_logging()
    app.add_middleware(LoggingMiddleware)
else:
    logger.handlers = []
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"))
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)
