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

import datetime

from queryengine.core.app.app import MetadataAppsConfig, CommonConfigs, AppConfig, Datasource, Schema, \
    Parameter, App
from queryengine.core.app.datasource import AppRepository
from queryengine.core.datasource.datasource import DataType


class TestAppRepository(AppRepository):
    def __init__(self):
        super(TestAppRepository, self).__init__()
        self._apps_config = MetadataAppsConfig(
            common_configs=CommonConfigs(
                atomic_fields={'num': 'number', 'date_': 'date', "sandbox_mode": "boolean"},
                gdpr_event_parameters={},
                gdpr_atomic_parameters=[],
                close_event_partition_after_hours=4,
                non_embedded_context_schemas={},
                embedded_context_schemas={},
                gdpr_context_parameters={}
            ),
            app_id_configs={
                'app': AppConfig(
                    app_id='app',
                    gdpr_event_parameters={},
                    timezone='UTC',
                    datasources={'user_history': Datasource(
                        'user_history',
                        datetime.datetime(2022, 1, 1).date(),
                        datetime.datetime(2023, 1, 1).date(),
                        materialized_columns=list()),
                    },
                    event_schemas={
                        'login': Schema(
                            url="",
                            vendor="",
                            name="login",
                            alias="Login",
                            version="",
                            description="",
                            parameters=[Parameter(name="internal_num", type=DataType.number, alias="", description="")]
                        )
                    },
                    external_services=None,
                )
            }
        )

    def from_app_id(self, app_id: str):
        return App(
            common_configs=self._apps_config.common_configs,
            app_config=self._apps_config.app_id_configs[app_id])
