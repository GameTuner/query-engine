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

import binascii
import contextvars
import os
import random
import sys
import uuid

from fastapi.logger import logger
from google.cloud import error_reporting
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response, JSONResponse

cloud_trace_context = contextvars.ContextVar('cloud_trace_context', default=dict({}))
http_request_context = contextvars.ContextVar('http_request_context', default=dict({}))

client = error_reporting.Client(service=f"gametuner-query-engine{os.environ.get('SERVICE_SUFFIX', '')}")


def trace():
    #  TODO read this from header? web app could send request id, page id or something
    return str(uuid.uuid4())


def span_id():
    # Generate an 8-byte array with uniformly random values
    byte_array = bytearray(random.getrandbits(8) for _ in range(8))
    # Ensure that the first byte is not zero
    if byte_array[0] == 0:
        byte_array[0] = random.randint(1, 255)
    # Convert the byte array to a hexadecimal string
    span_id = binascii.hexlify(byte_array).decode()
    # Pad the string with leading zeros if necessary
    return span_id.zfill(16)


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(
            self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        cloud_trace_context.set({
            'trace': trace(),
            'span_id': span_id()
        })

        http_request = {
            'requestMethod': request.method,
            'requestUrl': request.url.path,
            'requestSize': sys.getsizeof(request),
            'remoteIp': request.client.host,
            'protocol': request.url.scheme,
        }

        if 'referrer' in request.headers:
            http_request['referrer'] = request.headers.get('referrer')

        if 'user-agent' in request.headers:
            http_request['userAgent'] = request.headers.get('user-agent')

        http_request_context.set(http_request)

        try:
            return await call_next(request)
        except Exception as ex:
            client.report_exception()
            logger.exception(f'Request failed {request.method} {request.url.path}')
            return JSONResponse(
                status_code=500,
                content={
                    'success': False,
                    'message': ex
                }
            )
