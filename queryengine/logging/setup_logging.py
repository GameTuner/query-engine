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

import logging
import os

import google.cloud.logging
from fastapi.logger import logger
from google.cloud.logging_v2.handlers import CloudLoggingHandler

from queryengine.logging.filter import GoogleCloudLogFilter


def setup_logging():
    client = google.cloud.logging.Client()
    handler = CloudLoggingHandler(client, name="gametuner-query-engine" + os.getenv("SERVICE_SUFFIX", ""))
    handler.setLevel(logging.DEBUG)
    handler.filters = []
    handler.addFilter(GoogleCloudLogFilter(project=client.project))
    logger.handlers = []
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
