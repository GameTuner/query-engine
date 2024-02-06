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

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Dict, Union


@dataclass
class Parameter:
    name: str
    type: str
    alias: str | None
    description: str


@dataclass
class Schema:
    url: str
    vendor: str
    name: str
    alias: str
    version: str
    description: str | None
    parameters: List[Parameter]


@dataclass
class AppsFlyerConfig:
    reports: List[str]
    home_folder: str
    app_ids: List[str]


@dataclass
class AppsFlyerCostEtlConfig:
    bucket_name: str
    reports: List[str]
    android_app_id: str
    ios_app_id: str


@dataclass
class ExternalServices:
    apps_flyer: Union[AppsFlyerConfig, None]
    apps_flyer_cost_etl: Union[AppsFlyerCostEtlConfig, None]

    def has_external_services(self):
        return self.apps_flyer is not None or self.apps_flyer_cost_etl is not None

@dataclass
class MaterializedColumns:
    column_name: str
    external_table_name: str
    external_dataset_name: str
    select_formula: str
    data_type: str
    user_history_formula: str | None
    totals: bool | None = True
    can_filter: bool | None = True
    can_group_by: bool | None = False
    materialized_from: date | None = None
    hidden: bool | None = False


@dataclass
class Datasource:
    id: str
    has_data_from: date
    has_data_up_to: date | None
    materialized_columns: List[MaterializedColumns] | None

    def build_materialized_columns(materialized_columns_dict):
        return [MaterializedColumns(
            column_name=materialized_column_dict['column_name'],
            external_table_name=materialized_column_dict['table_name'],
            external_dataset_name=materialized_column_dict['dataset'],
            select_formula=materialized_column_dict['select_expression'],
            data_type=materialized_column_dict['data_type'],
            user_history_formula=materialized_column_dict['user_history_formula'],
            totals=materialized_column_dict['totals'],
            can_filter=materialized_column_dict['can_filter'],
            can_group_by=materialized_column_dict['can_group_by'],
            materialized_from=datetime.strptime(materialized_column_dict['materialized_from'], "%Y-%m-%d").date(),
            hidden=materialized_column_dict['hidden']
        ) for materialized_column_dict in materialized_columns_dict]

@dataclass
class AppConfig:
    app_id: str
    gdpr_event_parameters: Dict[str, List[str]]
    timezone: str
    datasources: Dict[str, Datasource]
    event_schemas: Dict[str, Schema]
    external_services: ExternalServices
    events_database: str = 'raw'
    use_context_schemas: bool = True

    def has_data_from(self, datasource_id: str):
        return self.datasources[datasource_id].has_data_from

    def has_data_up_to(self, datasource_id: str):
        return self.datasources[datasource_id].has_data_up_to


@dataclass
class CommonConfigs:
    atomic_fields: Dict[str, str]
    gdpr_event_parameters: Dict[str, List[str]]
    gdpr_context_parameters: Dict[str, List[str]]
    gdpr_atomic_parameters: List[str]
    close_event_partition_after_hours: int
    non_embedded_context_schemas: Dict[str, Schema]
    embedded_context_schemas: Dict[str, Schema]


@dataclass
class MetadataAppsConfig:
    common_configs: CommonConfigs
    app_id_configs: Dict[str, AppConfig]


def build_external_service(app_id_config_dict) -> ExternalServices:
    apps_flyer = None
    apps_flyer_config = next(
        (x['service_params'] for x in app_id_config_dict['external_services'] if x['service_name'] == "apps_flyer"),
        None)
    if apps_flyer_config:
        apps_flyer = AppsFlyerConfig(
            reports=apps_flyer_config['reports'],
            home_folder=apps_flyer_config['home_folder'],
            app_ids=apps_flyer_config['app_ids'],

        )
    apps_flyer_cost_etl = None
    apps_flyer_cost_etl_config = next((x['service_params'] for x in app_id_config_dict['external_services'] if
                                       x['service_name'] == "apps_flyer_cost_etl"), None)
    if apps_flyer_config:
        apps_flyer_cost_etl = AppsFlyerCostEtlConfig(
            bucket_name=apps_flyer_cost_etl_config['bucket_name'],
            reports=apps_flyer_cost_etl_config['reports'],
            android_app_id=apps_flyer_cost_etl_config['android_app_id'],
            ios_app_id=apps_flyer_cost_etl_config['ios_app_id']
        )

    return ExternalServices(
        apps_flyer=apps_flyer,
        apps_flyer_cost_etl=apps_flyer_cost_etl
    )


def build_schemas(schemas_dict):
    return {id: Schema(
        url=schema_dict["url"],
        vendor=schema_dict["vendor"],
        name=schema_dict["name"],
        version=schema_dict["version"],
        alias=schema_dict["alias"],
        description=schema_dict["description"],
        parameters=[Parameter(
            name=parameter_dict["name"],
            type=parameter_dict["type"],
            alias=parameter_dict["alias"],
            description=parameter_dict["description"]
        ) for parameter_dict in schema_dict["parameters"]]
    ) for id, schema_dict in schemas_dict.items()}


def load_from_dict(raw_dict) -> MetadataAppsConfig:
    metadata_apps_config = MetadataAppsConfig(
        common_configs=CommonConfigs(
            atomic_fields=raw_dict['common_configs']['atomic_fields'],
            gdpr_event_parameters=raw_dict['common_configs']['gdpr_event_parameters'],
            gdpr_context_parameters=raw_dict['common_configs']['gdpr_context_parameters'],
            gdpr_atomic_parameters=raw_dict['common_configs']['gdpr_atomic_parameters'],
            close_event_partition_after_hours=raw_dict['common_configs']['close_event_partition_after_hours'],
            non_embedded_context_schemas=build_schemas(raw_dict['common_configs']['non_embedded_context_schemas']),
            embedded_context_schemas=build_schemas(raw_dict['common_configs']['embedded_context_schemas'])
        ),
        app_id_configs={app_id: AppConfig(
            app_id=app_id,
            gdpr_event_parameters=app_id_config_dict['gdpr_event_parameters'],
            timezone=app_id_config_dict['timezone'],
            datasources={ds_id: Datasource(
                ds_id,
                datetime.strptime(ds['has_data_from'], "%Y-%m-%d"),
                datetime.strptime(ds['has_data_up_to'], "%Y-%m-%d") if ds['has_data_up_to'] else None,
                Datasource.build_materialized_columns(ds['materialized_columns']),
            )
                for ds_id, ds in app_id_config_dict['datasources'].items()},
            event_schemas=build_schemas(app_id_config_dict['event_schemas']),
            external_services=build_external_service(app_id_config_dict)
        ) for app_id, app_id_config_dict in raw_dict['app_id_configs'].items()}
    )

    return metadata_apps_config


@dataclass
class App:
    common_configs: CommonConfigs
    app_config: AppConfig

    def app_id(self):
        return self.app_config.app_id

    def all_event_names(self):
        return list(self.app_config.event_schemas.keys())

    def event_schema(self, name: str):
        return self.app_config.event_schemas[name]

    def event_gdpr_fields(self, event_name: str):
        params = []
        if event_name in self.common_configs.gdpr_event_parameters:
            params.extend([f'params.{name}' for name in self.common_configs.gdpr_event_parameters[event_name]])

        if event_name in self.app_config.gdpr_event_parameters:
            params.extend([f'params.{name}' for name in self.app_config.gdpr_event_parameters[event_name]])

        # add embedded contexts
        for context_name, gdpr_params in self.common_configs.gdpr_context_parameters.items():
            params.extend([f'{context_name}.{name}' for name in gdpr_params])
        return params

    def context_gdpr_fields(self, context_name: str):
        if context_name in self.common_configs.gdpr_context_parameters:
            return [f'params.{name}' for name in self.common_configs.gdpr_context_parameters[context_name]]

        return []
