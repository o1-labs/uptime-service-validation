# Uptime Service Validation

[![CI](https://github.com/MinaFoundation/uptime-service-validation/actions/workflows/ci.yaml/badge.svg)](https://github.com/MinaFoundation/uptime-service-validation/actions/workflows/ci.yaml)

## Overview

This repository is home for Validator/Coordinator component for Mina Delegation Program.

## Getting Started

### Prerequisites

- Python >= 3.10
- [Poetry](https://python-poetry.org/docs/), a tool for dependency management and packaging in Python.

### Setting Up Your Development Environment

1. **Install dependencies:**

```sh
git clone https://github.com/MinaFoundation/uptime-service-validation.git
cd uptime-service-validation

poetry install
```

2. **Activate the Virtual Environment:**

After installing the project dependencies with `poetry install`, you can activate the virtual environment by running:

```sh
poetry shell
```

This will spawn a new shell (subshell) with the virtual environment activated. It ensures that all commands and scripts are run within the context of your project's dependencies.

### Testing

```sh
poetry run pytest -v
```

## Configuration

The program requires the setting of several environment variables for operation. These variables should be defined in a `.env` file, which the program will load at runtime.

### AWS Keyspaces Configuration

To connect to AWS Keyspaces, the following environment variables need to be set:

- `AWS_KEYSPACE` - Your AWS Keyspace name.
- `CASSANDRA_HOST` - Cassandra host (e.g. cassandra.us-west-2.amazonaws.com).
- `CASSANDRA_PORT` - Cassandra port (e.g. 9142).
- `CASSANDRA_USERNAME` - Cassandra service user.
- `CASSANDRA_PASSWORD` - Cassandra service password.
- `SSL_CERTFILE` - The path to your SSL certificate for AWS Keyspaces.

> 🗒️ **Note 1:** For convenience, an SSL certificate is provided in this repository and can be found at [/uptime_service_validation/database/aws_keyspaces/cert/sf-class2-root.crt](/uptime_service_validation/database/aws_keyspaces/cert/sf-class2-root.crt). Alternatively, the certificate can also be downloaded directly from AWS. Detailed instructions for obtaining the certificate are available in the AWS Keyspaces documentation, which you can access [here](https://docs.aws.amazon.com/keyspaces/latest/devguide/using_python_driver.html#using_python_driver.BeforeYouBegin).

> 🗒️ **Note 2:** Docker image already includes cert and has `AWS_SSL_CERTIFICATE_PATH` set up, however it can be overriden by providing this env variable to docker.

### Postgres Database Configuration

The database needs to be set up. (see `createDB.sql`)

### Docker

Program is also shipped as a docker image.

### Building Docker

```sh
docker build -t uptime-service-validation .
```

### Running Docker

When running pass all relevant env variables to the docker (see `.env`), e.g.:

```sh
docker run -e SURVEY_INTERVAL_MINUTES=20 \
           -e POSTGRES_HOST=your_postgres_host \
           -e POSTGRES_PORT=your_postgres_port \
           -e POSTGRES_DB=your_postgres_db \
           -e POSTGRES_USER=your_postgres_user \
           -e POSTGRES_PASSWORD=your_postgres_password \
           -e MINI_BATCH_NUMBER=2 \
           -e UPTIME_DAYS_FOR_SCORE=your_uptime_days_for_score \
           -e WORKER_TAG=your_worker_tag \
           -e WORKER_IMAGE=your_worker_image \
           -e AWS_KEYSPACE=your_aws_keyspace \
           -e CASSANDRA_HOST=cassandra.us-west-2.amazonaws.com \
           -e CASSANDRA_PORT=9142 \
           -e CASSANDRA_USERNAME=cassandra_service_user \
           -e CASSANDRA_PASSWORD=cassandra_service_pass \
           -e SSL_CERTFILE=your_aws_ssl_certificate_path \
           uptime-service-validation
```

## Deployment

The program needs:
* The tables set up (see `createDB.sql`)
* `.env` file set up

Once done, one can launch the program with the following command:

```sh
poetry run start
```

Or run docker.
