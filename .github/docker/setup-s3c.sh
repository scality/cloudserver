#!/bin/bash

# setup S3C environment just like
# https://github.com/scality/Integration/blob/development/9.5/tests/setup-environment/index.js

set -e -o pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

VAULTCLIENT=$SCRIPT_DIR/../../node_modules/vaultclient/bin/vaultclient
CONFIG=$SCRIPT_DIR/admin.json

echo "Deleting and setting up S3C accounts like Integration (follow conf/authdata.json)"

$VAULTCLIENT --config $CONFIG delete-account --name Bart || true
$VAULTCLIENT --config $CONFIG create-account \
    --name Bart \
    --email sampleaccount1@sampling.com \
    --accountid 123456789012 \
    --canonicalid 79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be
$VAULTCLIENT --config $CONFIG generate-account-access-key \
    --name Bart \
    --accesskey ACC1AK00000000000000 \
    --secretkey ACC1SK0000000000000000000000000000000000

$VAULTCLIENT --config $CONFIG delete-account --name Lisa || true
$VAULTCLIENT --config $CONFIG create-account \
    --name Lisa \
    --email sampleaccount2@sampling.com \
    --accountid 123456789013 \
    --canonicalid 79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2bf
$VAULTCLIENT --config $CONFIG generate-account-access-key \
    --name Lisa \
    --accesskey ACC2AK00000000000000 \
    --secretkey ACC2SK0000000000000000000000000000000000

# Replication account for backbeat replication tests
$VAULTCLIENT --config $CONFIG delete-account --name Replication || true
# Cannot use url as canonicalid for service account
$VAULTCLIENT --config $CONFIG create-account \
    --name Replication \
    --email inspector@replication.info \
    --accountid 123456789015 \
    --canonicalid 79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2ba
$VAULTCLIENT --config $CONFIG generate-account-access-key \
    --name Replication \
    --accesskey ACCREPAK000000000000 \
    --secretkey ACCREPSK00000000000000000000000000000000

echo "Copying s3c credentials to mem credentials"
cp \
    $SCRIPT_DIR/../../tests/functional/aws-node-sdk/lib/json/s3c_credentials.json \
    $SCRIPT_DIR/../../tests/functional/aws-node-sdk/lib/json/mem_credentials.json

echo "Update conf/authdata.json account Replication canonicalID"
REP_CANONICAL_ID=$(
    $VAULTCLIENT --config $CONFIG get-account --account-name Replication | jq -r .canonicalId
)

# might need to undo changes if script was already ran before (manuallyoutside CI)
git checkout -- $SCRIPT_DIR/../../conf/authdata.json || true
sed -i "s/http:\/\/acs.zenko.io\/accounts\/service\/replication/$REP_CANONICAL_ID/g" $SCRIPT_DIR/../../conf/authdata.json
