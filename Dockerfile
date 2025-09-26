# Use an official Python runtime as a parent image
FROM python:3.12.11-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Install system dependencies required for psycopg2 or other PostgreSQL interactions
RUN apt-get update && apt-get install -y \
    libpq-dev \
    libpq5 \
    build-essential \
    podman \
    && rm -rf /var/lib/apt/lists/*

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Install any needed packages specified in pyproject.toml and poetry.lock
RUN pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --only main

# Output logs in real time
ENV PYTHONUNBUFFERED 1
# Set cert path for AWS Keyspaces
ENV SSL_CERTFILE /usr/src/app/uptime_service_validation/database/aws_keyspaces/cert/sf-class2-root.crt

# Run the application
CMD ["poetry", "run", "start"]
