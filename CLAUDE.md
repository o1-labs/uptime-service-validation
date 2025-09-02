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
- `POSTGRES_*` - Database connection settings
- `WORKER_IMAGE`, `WORKER_TAG` - Docker image for stateless verifier
- `SURVEY_INTERVAL_MINUTES` - Batch processing interval (default: 20)
- `MINI_BATCH_NUMBER` - Number of mini-batches per main batch (default: 5)
- `TEST_ENV=1` - Run validators as subprocesses instead of Kubernetes pods

## Key Data Flow

1. **Batch Processing**: System processes submissions in time-based batches determined by `SURVEY_INTERVAL_MINUTES`
2. **Validation**: Each submission is validated using the stateless-verification-tool in separate Kubernetes pods
3. **Scoring**: Valid submissions earn points, with one point per batch regardless of submission count
4. **Database Updates**: Results stored in PostgreSQL with tables: `submissions`, `points`, `points_summary`, `scoreboard`

## Important Database Concepts

- **points vs points_summary**: `points` table has one row per valid submission, `points_summary` has one row per batch per block producer (crucial for correct scoring)
- **Batch System**: Uses `bot_logs` table to track batch boundaries and processing state
- **Scoreboard**: Updated based on `UPTIME_DAYS_FOR_SCORE` (default: 90 days)