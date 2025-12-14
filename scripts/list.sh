#!/bin/bash

set -e

if ! command -v curl &> /dev/null
then
    echo "curl could not be found but is a pre-requisite for this script"
    exit
fi

GATEWAY_HOST=${GATEWAY_HOST:-localhost}
GATEWAY_PORT=${GATEWAY_PORT:-80}

curl -X GET http://${GATEWAY_HOST}:${GATEWAY_PORT}/system/list
