#!/bin/bash

# Make sure the good container version runs before running sse migration tests

set -o pipefail;

CONTAINER=$1
EXPECTED_VERSION=$2

# run jq outside container as some container might not have it
VERSION=$(docker compose exec $1 cat package.json | jq -r .version)
if [[ "$VERSION" != "$EXPECTED_VERSION" ]]; then
    echo "bad version of container $CONTAINER. Should be $EXPECTED_VERSION. Was $VERSION" >&2
    exit 1
else
    echo OK $VERSION
fi
