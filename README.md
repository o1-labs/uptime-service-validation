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

