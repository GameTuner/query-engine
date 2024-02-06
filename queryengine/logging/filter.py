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

from google.cloud.logging_v2.handlers import CloudLoggingFilter

from queryengine.logging.middleware import http_request_context, cloud_trace_context


class GoogleCloudLogFilter(CloudLoggingFilter):

    def filter(self, record: logging.LogRecord) -> bool:
        if http_request_context.get():
            record.http_request = http_request_context.get()

            trace = cloud_trace_context.get()
            if trace:
                record.trace = f"projects/{self.project}/traces/{trace['trace']}"
                record.span_id = trace["span_id"]
        super().filter(record)
        return True
