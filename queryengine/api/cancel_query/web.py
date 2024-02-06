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

from fastapi import Depends, APIRouter

from queryengine import dependencies
from queryengine.core.bigquery.queryexecutor import CancellableBigQueryExecutor
from queryengine.logging.router import LoggingRoute

router = APIRouter(route_class=LoggingRoute)


@router.post("/api/v1/cancel-by-request-id/{request_id}")
def cancel_by_request_id(request_id: str,
                         bigquery_executor: CancellableBigQueryExecutor = Depends(dependencies.bigquery_executor)):
    bigquery_executor.cancel_by_request_id(request_id)


@router.post("/api/v1/cancel-by-page-id/{page_id}")
def cancel_by_page_id(page_id: str,
                      bigquery_executor: CancellableBigQueryExecutor = Depends(dependencies.bigquery_executor)):
    bigquery_executor.cancel_by_page_id(page_id)
