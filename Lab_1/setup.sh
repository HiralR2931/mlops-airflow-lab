#!/bin/bash
set -e

# Remove existing .env file if it exists
rm -f .env
rm -rf ./logs ./plugins ./config

# Stop and remove containers, networks, and volumes
docker compose down -v

# Create required Airflow directories
mkdir -p ./logs ./plugins ./config

# Write the current user's UID into .env
echo "AIRFLOW_UID=$(id -u)" > .env

# Run airflow CLI to show current config
docker compose run --rm airflow-cli airflow config list

# Generate a Fernet key (encrypts connections/variables) so Airflow doesn't warn
echo "FERNET_KEY=$(openssl rand -base64 32 | tr '+/' '-_')" >> .env
