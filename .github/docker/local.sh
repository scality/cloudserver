#!/bin/bash
set -e -o pipefail
#in .github/docker

export S3BACKEND=file
export S3METADATA=scality
export S3VAULT=scality
export CLOUDSERVER_IMAGE_BEFORE_SSE_MIGRATION=ghcr.io/scality/cloudserver:7.70.21-11
export CLOUDSERVER_IMAGE_ORIGINAL=ghcr.io/scality/cloudserver:50db1ada69a394cf877bd3486d4d0e318158e338
export MPU_TESTING="yes"
export JOB_NAME=sse-kms-migration-tests-show-arn
export kmsHideScalityArn=showArn

export VAULT_IMAGE_BEFORE_SSE_MIGRATION=ghcr.io/scality/vault:7.70.15-5
export VAULT_IMAGE_ORIGINAL=ghcr.io/scality/vault:e8c0fa2890c131581efd13ad3fd1ade7dcbd0968
export KMS_IMAGE=nsmithuk/local-kms:3.11.7

# IMAGE IS HARDCODED FOR OKMS TO HIDE
export JOB_NAME=sse-kms-migration-tests-hide-arn
export kmsHideScalityArn=hideArn
# export JOB_NAME=sse-kms-migration-tests-show-arn
# export kmsHideScalityArn=showArn

mkdir -p /tmp/artifacts/$JOB_NAME

export CLOUDSERVER_IMAGE=$CLOUDSERVER_IMAGE_BEFORE_SSE_MIGRATION
export VAULT_IMAGE=$VAULT_IMAGE_BEFORE_SSE_MIGRATION
export SSE_CONF=before

export KMS_AWS_SECRET_ACCESS_KEY=123
export KMS_AWS_ACCESS_KEY_ID=456

# START KMS
docker run -d -p 8080:8080 $KMS_IMAGE || true

    echo "waiting for local AWS KMS service on port 8080 to be available."

    timeout 300 bash -c 'until curl -sS 0:8080 > /dev/null; do 
        echo "service not ready on port 8080. Retrying in 2 seconds."
        sleep 2
    done'
    echo "local AWS KMS service is up and running on port 8080."

    AWS_ENDPOINT_URL=http://0:8080 AWS_DEFAULT_REGION=us-east-1 AWS_ACCESS_KEY_ID=456 AWS_SECRET_ACCESS_KEY=123 aws kms list-keys --max-items 1
# END KMS

# Start all before migration
docker compose up -d
        bash ../../wait_for_local_port.bash 8500 40
        bash ../../wait_for_local_port.bash 8000 40
# HAVE vaultclient bin in your PATH or an alias
alias vaultclient="~/scality/vaultclient/bin/vaultclient"
export PATH="$PATH:~/scality/vaultclient/bin/"
vaultclient --config admin.json delete-account --name mick || true
vaultclient --config admin.json create-account --name mick --email mick@mick.mick
vaultclient --config admin.json generate-account-access-key --name mick --accesskey SCUBAINTERNAL0000000 --secretkey SCUBAINTERNAL000000000000000000000000000
vaultclient --config admin.json get-account --account-name mick

cd ../..

echo ===== RUN BEFORE MIGRATION =====
export S3_CONFIG_FILE=config.before.json

        set -o pipefail;


        echo Ensures the expected version of cloudserver is old one:
        VERSION=$(docker compose -f .github/docker/docker-compose.yaml \
            exec cloudserver cat package.json | jq -r .version)
        if [[ "$VERSION" != "7.70.21-11" ]]; then
          echo "bad version of container. Should be 7.70.21-11. Was $VERSION" >&2
          exit 1
        else
          echo OK $VERSION
        fi

        yarn run ft_sse_before_migration | tee /tmp/artifacts/$JOB_NAME/beforeMigration.log

# RUN latest images
cd .github/docker
export SSE_CONF=sseMigration.$kmsHideScalityArn
export CLOUDSERVER_IMAGE=$CLOUDSERVER_IMAGE_ORIGINAL
export VAULT_IMAGE=$VAULT_IMAGE_ORIGINAL

docker compose down cloudserver vault && docker compose up -d vault # cloudserver-sse-migration

echo ==== RUN MIGRATION ====
cd ../..
yarn start_migration > s3.log &
export S3_CONFIG_FILE=config.sseMigration.$kmsHideScalityArn.json
export S3KMS=aws

        set -o pipefail;
        bash wait_for_local_port.bash 8500 40
        bash wait_for_local_port.bash 8000 40

        # echo Ensures the expected version of cloudserver is NOT old one
        # VERSION=$(docker compose -f .github/docker/docker-compose.yaml \
        #     exec cloudserver-sse-migration cat package.json | jq -r .version)
        # if [[ "$VERSION" == "7.70.21-11" ]]; then
        #   echo "bad version of container. Should NOT be 7.70.21-11. Was $VERSION" >&2
        #   exit 1
        # else
        #   echo OK $VERSION
        # fi

        yarn run ft_sse_migration # | tee /tmp/artifacts/$JOB_NAME/migration.log
        sleep 10
        yarn run ft_sse_arn # | tee /tmp/artifacts/$JOB_NAME/migration.log

