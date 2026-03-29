#!/bin/bash

# upload.sh folder name env replicas

set -e

if ! command -v curl &> /dev/null
then
    echo "curl could not be found but is a pre-requisite for this script"
    exit
fi

if ! command -v zip &> /dev/null
then
    echo "zip could not be found but is a pre-requisite for this script"
    exit
fi

if ! command -v uuidgen &> /dev/null
then
    echo "uuidgen could not be found but is a pre-requisite for this script"
    exit
fi

GATEWAY_HOST=${GATEWAY_HOST:-localhost}
GATEWAY_PORT=${GATEWAY_PORT:-80}
TMP_ROOT=${TMPDIR:-/tmp/}
ARCHIVE_DIR=""

while [ -z "${ARCHIVE_DIR}" ]
do
    CANDIDATE_DIR="${TMP_ROOT%/}/tinyfaas-upload-$(uuidgen)"
    if mkdir "${CANDIDATE_DIR}" 2>/dev/null
    then
        ARCHIVE_DIR="${CANDIDATE_DIR}"
    fi
done

ARCHIVE_FILE="${ARCHIVE_DIR}/function.zip"

cleanup() {
    rm -rf "${ARCHIVE_DIR}"
}

trap cleanup EXIT

pushd "$1" >/dev/null || exit
zip -qr "${ARCHIVE_FILE}" .
popd >/dev/null || exit

curl -X POST http://"${GATEWAY_HOST}":"${GATEWAY_PORT}"/system/upload \
    -F "metadata={\"name\":\"$2\",\"env\":\"$3\",\"replicas\":$4};type=application/json" \
    -F "zip=@${ARCHIVE_FILE};type=application/zip"
