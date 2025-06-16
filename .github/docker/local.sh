#!/bin/bash
set -e -o pipefail

# run kms migration tests locally
# in .github/docker

export S3BACKEND=file
export S3METADATA=file
export S3VAULT=scality
export MPU_TESTING="yes"

export CLOUDSERVER_IMAGE_BEFORE_SSE_MIGRATION=ghcr.io/scality/cloudserver:7.70.66
export CLOUDSERVER_IMAGE_ORIGINAL=ghcr.io/scality/cloudserver:7.70.70

export VAULT_IMAGE_BEFORE_SSE_MIGRATION=ghcr.io/scality/vault:7.70.31
export VAULT_IMAGE_ORIGINAL=ghcr.io/scality/vault:7.70.32
export KMS_IMAGE=nsmithuk/local-kms:3.11.7

export S3_CONFIG_FILE=config.json

export kmsContainer=localkms
export kmsProvider=aws
export kmsPort=8080

# export kmsContainer=pykmip
# export kmsProvider=kmip
# export kmsPort=5696

export kmsHideScalityArn=true
export globalEncryptionEnabled=true

export JOB_NAME=sse-kms-migration-tests-$kmsHideScalityArn-$kmsProvider

mkdir -p /tmp/artifacts/$JOB_NAME
mkdir -p /tmp/ssl-kmip

export CLOUDSERVER_IMAGE=$CLOUDSERVER_IMAGE_BEFORE_SSE_MIGRATION
export VAULT_IMAGE=$VAULT_IMAGE_BEFORE_SSE_MIGRATION

export KMS_AWS_SECRET_ACCESS_KEY=123
export KMS_AWS_ACCESS_KEY_ID=456

export COMPOSE_FILE=docker-compose.yaml:docker-compose.sse.yaml

function stop_all() {
  docker compose -p docker down
}

function rm_all() {
  sudo rm -rf ./vault-db/
  sudo rm -rf ../../localData/*
  sudo rm -rf ../../localMetadata/*
}

function start_all_before_migration() {
  cd ../../tests/functional/sse-kms-migration
  pwd
  cp configs/base.json config.json
  cd ../../../.github/docker

  docker compose up -d redis vault-sse-before-migration cloudserver-sse-before-migration
  bash ../../wait_for_local_port.bash 8500 40
  bash ../../wait_for_local_port.bash 8000 40
  # HAVE vaultclient bin in your PATH or an alias
  # alias vaultclient="~/scality/vaultclient/bin/vaultclient"
  export PATH="$PATH:~/scality/vaultclient/bin/"
  vaultclient --config admin.json delete-account --name test || true
  vaultclient --config admin.json create-account --name test --email test@scality.com
  vaultclient --config admin.json generate-account-access-key --name test --accesskey TESTAK00000000000000 --secretkey TESTSK0000000000000000000000000000000000
  vaultclient --config admin.json get-account --account-name test
}

function run_before_migration() {
  echo ===== RUN BEFORE MIGRATION =====
  cd ../..

  AWS_ENDPOINT_URL=http://0:8000 AWS_DEFAULT_REGION=us-east-1 AWS_ACCESS_KEY_ID=TESTAK00000000000000 AWS_SECRET_ACCESS_KEY=TESTSK0000000000000000000000000000000000 aws s3 ls
  yarn run ft_sse_before_migration | tee /tmp/artifacts/$JOB_NAME/beforeMigration.log
}

function run_latest_images() {
  export CLOUDSERVER_IMAGE=$CLOUDSERVER_IMAGE_ORIGINAL
  export VAULT_IMAGE=$VAULT_IMAGE_ORIGINAL
  export S3KMS=$kmsProvider # S3
  export KMS_BACKEND=$([[ "$kmsProvider" == "aws" ]] && echo "aws") # vault only aws is supported

  cd tests/functional/sse-kms-migration
  jq -s "
    .[0] * .[1] * .[2] *
    { kmsHideScalityArn: $kmsHideScalityArn } *
    { globalEncryptionEnabled: $globalEncryptionEnabled }
  " \
    configs/base.json \
    configs/$kmsProvider.json \
    configs/sseMigration.json \
    > config.json
  cd ../../../
  cd .github/docker
  # copy kmip certs
  sudo cp -r ../pykmip/certs/* /tmp/ssl-kmip

  docker compose down cloudserver-sse-before-migration vault-sse-before-migration
  docker compose up -d $kmsContainer vault-sse-migration cloudserver-sse-migration

  bash ../../wait_for_local_port.bash $kmsPort 40
  bash ../../wait_for_local_port.bash 8500 40
  bash ../../wait_for_local_port.bash 8000 40
}

function run_migration() {
  echo ==== RUN MIGRATION ====
  cd ../..
  export S3KMS=$kmsProvider # S3
  export KMS_BACKEND=$([[ "$kmsProvider" == "aws" ]] && echo "aws") # vault only aws is supported
  yarn run ft_sse_migration | tee /tmp/artifacts/$JOB_NAME/migration.log
}

function run_after_migration() {
  sleep 1
  export S3KMS=$kmsProvider # S3
  export KMS_BACKEND=$([[ "$kmsProvider" == "aws" ]] && echo "aws") # vault only aws is supported
  yarn run ft_sse_arn | tee /tmp/artifacts/$JOB_NAME/arnPrefix.log
}

stop_all
rm_all
start_all_before_migration
run_before_migration
run_latest_images
run_migration
run_after_migration
