#!/usr/bin/env bash

# Check if the project name is passed as an argument
if [ $# -ne 1 ]; then
    echo "Usage: $0 <deployment-id>"
    exit 1
fi

# Assign the arguments to variables
DEPLOYMENT_ID="$1"

# Build, tag, and push the Docker image with the specified project name
docker build --no-cache -t oaebu-workflows .

# Deploy using Astro
astro workspace switch Book\ Analytics\ Dashboard
astro deploy -i oaebu-workflows -f ${DEPLOYMENT_ID}
