#!/usr/bin/env bash

# Check if the project name is passed as an argument
if [ $# -eq 0 ]; then
    echo "Error: No project name provided."
    echo "Usage: $0 <project-name>"
    exit 1
fi

# Assign the first argument to a variable
PROJECT_NAME="$1"

# Build, tag, and push the Docker image with the specified project name
docker build -t oaebu-workflows .
docker tag oaebu-workflows us-docker.pkg.dev/${PROJECT_NAME}/oaebu-workflows/oaebu-workflows
docker push us-docker.pkg.dev/${PROJECT_NAME}/oaebu-workflows/oaebu-workflows

# Deploy using Astro
astro deploy -i oaebu-workflows -f