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

import threading
import time
from abc import ABC, abstractmethod

import requests
from fastapi.logger import logger

from queryengine.core.app import app
from queryengine.core.app.app import MetadataAppsConfig, App


class AppRepository(ABC):
    def __init__(self):
        self.load_data()

    def load_data(self):
        pass

    @abstractmethod
    def from_app_id(self, app_id: str) -> App:
        pass


def get_metadata_apps_config(metadata_ip_address: str, metadata_port: int) -> MetadataAppsConfig:
    r = requests.get(f'http://{metadata_ip_address}:{metadata_port}/api/v1/apps-detailed')
    r.raise_for_status()
    return app.load_from_dict(r.json())


class MetadataAppRepository(AppRepository):
    def __init__(self, metadata_ip_address: str, metadata_port: int):
        self.metadata_ip_address = metadata_ip_address
        self.metadata_port = metadata_port
        self._apps_config = None
        super().__init__()

    def load_data(self):
        self._apps_config = get_metadata_apps_config(self.metadata_ip_address, self.metadata_port)

    def from_app_id(self, app_id: str) -> App:
        return App(
            common_configs=self._apps_config.common_configs,
            app_config=self._apps_config.app_id_configs[app_id])


class CachedMetadataAppRepository(AppRepository):
    def __init__(self, base_app_config_repository: AppRepository):
        self.base_app_config_repository = base_app_config_repository
        self.refresh_thread = None
        super(CachedMetadataAppRepository, self).__init__()

    def load_data(self):
        if not self.refresh_thread:
            self.refresh_thread = threading.Thread(target=self._refresh_apps_config)
            self.refresh_thread.daemon = True
            self.refresh_thread.start()

    def _refresh_apps_config(self):
        while True:
            try:
                self.base_app_config_repository.load_data()
            except:
                logger.exception("Failed to update apps config!")
            time.sleep(30)

    def from_app_id(self, app_id: str) -> App:
        return self.base_app_config_repository.from_app_id(app_id)
