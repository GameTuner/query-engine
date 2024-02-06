# GameTuner Query Engine

## Overview

Query engine is a service that is responsible for executing the queries that are built from API requests. It is a REST API service that is built using FastAPI. It implements custom semantic layer for BigQuery data. It is written in Python and uses FastAPI as a web framework. 

## Installation

### Dependencies

GameTuner Query Engine service is dependent on [GameTuner Metadata][gametuner-metadata] service. MetaData service is used for fetching configurations of applications for query engine. If MetaData service is not available, application will not start.

### Run locally

#### Pre-requisites

In order to run this service you need to have the following installed:

- Python 3.10+

#### Configuration

The service is configured using environment variables. The following environment variables are used:

- `GCP_PROJECT_ID`      - GCP project id
- `JSON_LOGS`           - If set to 1, logging is enabled
- `METADATA_IP_ADDRESS` - Metadata service IP address
- `METADATA_PORT`       - Metadata service port
- `UVICORN_PORT`        - Port on which the service will run

Examples of environment variables can be found in the `.env` file.

#### Run service

To run the service locally, you need to install the dependencies first. You can do that by running the following command:

```bash
pip install -r requirements.txt
```

After that you can run the service by running the following command:

```bash
uvicorn queryengine.main:app --reload
```

### Running on GCP

For deploying the application on GCP, you should first build the application by running google cloud build `gcloud builds submit --config=cloudbuild.yaml .`. Script submits docker image to GCP artifact registry. Once the image is submitted, you should deploy the application on GCP. You can do that by running terraform script in [GameTuner terraform][gametuner-terraform] project.

## Usage

### API

Service provides REST API for managing organisations, apps, events and event schemas. You can find the full API documentation on http://localhost:8001/docs

## Licence

The GameTuner Query Engine is copyright 2022-2024 AlgebraAI.

GameTuner Query Engine is released under the [Apache 2.0 License][license].


[gametuner-terraform]:https://github.com/GameTuner/gametuner-terraform-gcp.git
[gametuner-metadata]:https://github.com/GameTuner/metadata.git
[license]: https://www.apache.org/licenses/LICENSE-2.0