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

import time
from typing import Callable

from fastapi import Response, Request
from fastapi.logger import logger
from fastapi.routing import APIRoute
from starlette.background import BackgroundTask
from starlette.responses import StreamingResponse


def log_info(duration, status, method, path, body):
    logger.info(f"Processed request in {duration:.2f}s - {method} {path} {status} {body}")


class LoggingRoute(APIRoute):
    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            before = time.time()
            req_body = await request.body()
            try:
                response = await original_route_handler(request)
            except Exception as e:
                duration = time.time() - before
                logger.exception(
                    f"Failed to process request in {duration:.2f}s - {request.method} {request.url.path} {req_body} {e}")
                raise
            duration = time.time() - before

            if isinstance(response, StreamingResponse):
                res_body = b''
                async for item in response.body_iterator:
                    res_body += item

                task = BackgroundTask(log_info, duration, response.status_code, request.method, request.url.path,
                                      req_body)
                return Response(content=res_body, status_code=response.status_code,
                                headers=dict(response.headers), media_type=response.media_type, background=task)
            else:
                response.background = BackgroundTask(log_info, duration, response.status_code, request.method,
                                                     request.url.path, req_body)
                return response

        return custom_route_handler
