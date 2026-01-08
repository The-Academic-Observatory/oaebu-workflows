#!/usr/bin/env bash
set -euo pipefail

# Default values
DEPLOYMENT_ID=""
IS_LOCAL=false
NO_BUILD=false

usage() {
    echo "Usage: $0 --deployment-id <ID> [--local]"
    echo ""
    echo "Options:"
    echo "  -d, --deployment-id ID    (Required) The ID for this deployment"
    echo "  -l, --local               (Optional) Enable local mode"
    echo "  -x, --no-build            (Optional) Don't rebuild the image"
    echo "  -h, --help                Display this help message"
    exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -d|--deployment-id)
            DEPLOYMENT_ID="$2"
            shift 2
            ;;
        -l|--local)
            IS_LOCAL=true
            shift
            ;;
        -x|--no-build)
            NO_BUILD=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Error: Invalid option '$1'"
            usage
            ;;
    esac
done

# --- Validation Logic ---
if [ "$IS_LOCAL" = false ] && [ -z "$DEPLOYMENT_ID" ]; then
    echo "Error: The --deployment-id argument is required."
    usage
fi  

# Build, tag, and push the Docker image with the specified project name
if [ "$NO_BUILD" = false ] ; then
    docker build --no-cache -t oaebu-workflows:dev .
fi

# Deploy using Astro
if [ "$IS_LOCAL" = false ] ; then
    astro workspace switch Book\ Analytics\ Dashboard
    astro deploy -i oaebu-workflows:dev -f "${DEPLOYMENT_ID}"
else
    astro dev start oaebu-workflows:dev
fi
