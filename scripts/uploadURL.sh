#!/bin/bash

# uploadURL.sh url subfolder name env threads

set -e

if ! command -v curl &> /dev/null
then
    echo "curl could not be found but is a pre-requisite for this script"
    exit
fi

GATEWAY_HOST=${GATEWAY_HOST:-localhost}
GATEWAY_PORT=${GATEWAY_PORT:-80}

curl -X POST http://"${GATEWAY_HOST}":"${GATEWAY_PORT}"/system/uploadURL --data "{\"name\": \"$3\", \"env\": \"$4\",\"replicas\": $5,\"url\": \"$1\",\"subfolder_path\": \"$2\"}"
