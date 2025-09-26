# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the Validator/Coordinator component for Mina Delegation Program. It validates submissions from block producers using the stateless-verification-tool and maintains uptime scores in a PostgreSQL database. The system processes submissions in time-based batches and awards points to block producers for valid submissions.

## Architecture

- **Entry Point**: `uptime_service_validation/coordinator/coordinator.py` - Main coordinator script that manages validator processes
- **Core Components**:
  - `coordinator.py` - Main orchestrator that processes batches, manages validators, and updates scores
  - `server.py` - Sets up Kubernetes pods or subprocesses for stateless verification
  - `helper.py` - Database operations, graph algorithms for block validation, utility functions
  - `config.py` - Configuration management from environment variables
  - `aws_keyspaces_client.py` - Cassandra/AWS Keyspaces database client

## Development Commands

### Dependencies and Environment
```bash
# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

### Testing
```bash
# Run all tests
poetry run pytest

# Run tests with verbose output
poetry run pytest -v
```

### Linting
```bash
# Run flake8 linting (errors only)
poetry run flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics

# Run flake8 with complexity checks
poetry run flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
```

### Running the Application
```bash
# Start the coordinator
poetry run start

# For local development with podman-compose
podman-compose --env-file .env.example.test -f podman-compose.yaml up --build
```

### Database Management (using invoke tasks)
```bash
# Create database and tables
invoke create-database

# Initialize bot_logs table (required for first run)
invoke init-database

# Initialize with specific timestamp
invoke init-database --batch-end-epoch "2024-06-05 00:00:00"

# Create read-only user
invoke create-ro-user

# Drop database
invoke drop-database
```

## Configuration

The application is heavily configured via environment variables. Key variables include:

### Core Configuration
- `POSTGRES_*` - Database connection settings
  - `POSTGRES_SSLMODE` - SSL mode for PostgreSQL connections (default: "require")
- `WORKER_IMAGE`, `WORKER_TAG` - Docker image for stateless verifier
- `SURVEY_INTERVAL_MINUTES` - Batch processing interval (default: 20)
- `MINI_BATCH_NUMBER` - Number of mini-batches per main batch (default: 5)
- `TEST_ENV=1` - Run validators as subprocesses instead of Kubernetes pods

### Worker Job Configuration
- `WORKER_SERVICE_ACCOUNT_NAME` - Kubernetes service account for worker jobs (default: "delegation-verify")
- `WORKER_CONFIGMAP_NAME` - ConfigMap name containing worker entrypoint script
- `WORKER_CPU_REQUEST`, `WORKER_MEMORY_REQUEST` - Resource requests for worker pods
- `WORKER_CPU_LIMIT`, `WORKER_MEMORY_LIMIT` - Resource limits for worker pods
- `WORKER_TTL_SECONDS_AFTER_FINISHED` - Job cleanup timeout

### Google Sheets Integration
- `SPREADSHEET_NAME` - Name of Google Sheets document for block producer contact details
- `SPREADSHEET_CREDENTIALS_JSON` - Path to service account credentials file
- `IGNORE_APPLICATION_STATUS` - Skip Google Sheets integration if set

## Key Data Flow

1. **Batch Processing**: System processes submissions in time-based batches determined by `SURVEY_INTERVAL_MINUTES`
2. **Validation**: Each submission is validated using the stateless-verification-tool in separate Kubernetes pods
3. **Scoring**: Valid submissions earn points, with one point per batch regardless of submission count
4. **Database Updates**: Results stored in PostgreSQL with tables: `submissions`, `points`, `points_summary`, `scoreboard`

## Important Database Concepts

- **points vs points_summary**: `points` table has one row per valid submission, `points_summary` has one row per batch per block producer (crucial for correct scoring)
- **Batch System**: Uses `bot_logs` table to track batch boundaries and processing state
- **Scoreboard**: Updated based on `UPTIME_DAYS_FOR_SCORE` (default: 90 days)

## Deployment and Troubleshooting

### Common Issues and Solutions

#### Google Sheets Integration
- **Service Account Access**: Ensure the Google Sheets document is shared with the service account email found in `SPREADSHEET_CREDENTIALS_JSON`
- **API Rate Limits**: The system includes exponential backoff retry logic for temporary Google Sheets API failures
- **Missing Columns**: The system validates spreadsheet structure and handles malformed sheets gracefully

#### Kubernetes Worker Jobs
- **Service Account**: Use `WORKER_SERVICE_ACCOUNT_NAME` to specify correct Kubernetes service account
- **ConfigMap**: Ensure worker entrypoint ConfigMap exists with name matching `WORKER_CONFIGMAP_NAME`
- **Database SSL**: Set `POSTGRES_SSLMODE=disable` if PostgreSQL doesn't support SSL connections

#### S3 Integration
- **Region Configuration**: Ensure `AWS_REGION` matches the S3 bucket's actual region
- **Credentials**: Worker jobs inherit AWS credentials from coordinator environment variables
- **Bucket Access**: Verify IAM permissions include S3:GetObject for the specified bucket

### Error Recovery
- Google Sheets failures are non-blocking - core validation continues even if contact updates fail
- Invalid blocks are properly marked when S3 data is unavailable
- Jobs include automatic cleanup via `WORKER_TTL_SECONDS_AFTER_FINISHED`

## Local Development with Podman Compose

The project includes `podman-compose.yaml` for streamlined local development and testing:

### Quick Start
```bash
# Copy and configure environment file
cp .env.example .env.example.test
# Edit .env.example.test with your local settings

# Build and run the service
podman-compose --env-file .env.example.test -f podman-compose.yaml up --build
```

### Key Features
- **Automatic Test Mode**: Sets `TEST_ENV=1` to run validators as subprocesses instead of Kubernetes pods
- **Host Networking**: Simplified database connectivity for local development
- **Pre-configured Environment**: Sensible defaults for all required environment variables
- **Volume Mounting**: Automatic mounting of Google Cloud credentials for Google Sheets integration
- **Health Checks**: Built-in container health monitoring
- **Debugging Support**: Easy container debugging with sleep infinity entrypoint option

### Environment Configuration
The compose file supports all standard environment variables through `.env.example.test`:
- Database configuration (PostgreSQL connection settings)
- AWS configuration (S3 bucket, region, credentials)
- Google Sheets integration settings
- Worker and batch processing configuration

This setup allows developers to quickly test the coordinator locally without needing a full Kubernetes environment.