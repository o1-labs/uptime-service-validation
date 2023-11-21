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
- `AWS_REGION` - The AWS region.
- `AWS_ACCESS_KEY_ID` - Your AWS Access Key ID.
- `AWS_SECRET_ACCESS_KEY` - Your AWS Secret Access Key.
- `AWS_SSL_CERTIFICATE_PATH` - The path to your SSL certificate for AWS Keyspaces.

> 🗒️ **Note:** For convenience, an SSL certificate is provided in this repository and can be found at [/uptime_service_validation/database/aws_keyspaces/cert/sf-class2-root.crt](/uptime_service_validation/database/aws_keyspaces/cert/sf-class2-root.crt). Alternatively, the certificate can also be downloaded directly from AWS. Detailed instructions for obtaining the certificate are available in the AWS Keyspaces documentation, which you can access [here](https://docs.aws.amazon.com/keyspaces/latest/devguide/using_python_driver.html#using_python_driver.BeforeYouBegin).

### Deployment

The program needs:
* The tables set up (see createDB.sql)
* The .env file set up
* Install dependencies (poetry install)
* Execute coordinator.py