#!/bin/bash -x
set -x #echo on
set -e #exit at the first error

mkdir -p ~/.aws
cat >>/root/.aws/exports <<EOF
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export b2backend_B2_ACCOUNT_ID="$b2backend_B2_ACCOUNT_ID"
export b2backend_B2_STORAGE_ACCESS_KEY="$b2backend_B2_STORAGE_ACCESS_KEY"
export b2backend_B2_STORAGE_ENDPOINT="$b2backend_B2_STORAGE_ENDPOINT"
export gcpbackend2_GCP_SERVICE_EMAIL="$gcpbackend2_GCP_SERVICE_EMAIL"
export gcpbackend2_GCP_SERVICE_KEY="$gcpbackend2_GCP_SERVICE_KEY"
export gcpbackend2_GCP_SERVICE_KEYFILE="$gcpbackend2_GCP_SERVICE_KEYFILE"
export gcpbackend_GCP_SERVICE_EMAIL="$gcpbackend_GCP_SERVICE_EMAIL"
export gcpbackend_GCP_SERVICE_KEY="$gcpbackend_GCP_SERVICE_KEY"
export gcpbackendmismatch_GCP_SERVICE_EMAIL="$gcpbackendmismatch_GCP_SERVICE_EMAIL"
export gcpbackendmismatch_GCP_SERVICE_KEY="$gcpbackendmismatch_GCP_SERVICE_KEY"
export gcpbackend_GCP_SERVICE_KEYFILE="$gcpbackend_GCP_SERVICE_KEYFILE"
export gcpbackendmismatch_GCP_SERVICE_KEYFILE="$gcpbackendmismatch_GCP_SERVICE_KEYFILE"
export gcpbackendnoproxy_GCP_SERVICE_KEYFILE="$gcpbackendnoproxy_GCP_SERVICE_KEYFILE"
export gcpbackendproxy_GCP_SERVICE_KEYFILE="$gcpbackendproxy_GCP_SERVICE_KEYFILE"
export GOOGLE_SERVICE_EMAIL="$GCP_BACKEND_SERVICE_EMAIL"
export GOOGLE_SERVICE_KEY="$GCP_BACKEND_SERVICE_KEY"
export azurebackend2_AZURE_STORAGE_ACCESS_KEY="$AZURE_BACKEND_ACCESS_KEY_2"
export azurebackend2_AZURE_STORAGE_ACCOUNT_NAME="$AZURE_BACKEND_ACCOUNT_NAME_2"
export azurebackend2_AZURE_STORAGE_ENDPOINT="$AZURE_BACKEND_ENDPOINT_2"
export azurebackend_AZURE_STORAGE_ACCESS_KEY="$AZURE_BACKEND_ACCESS_KEY"
export azurebackend_AZURE_STORAGE_ACCOUNT_NAME="$AZURE_BACKEND_ACCOUNT_NAME"
export azurebackend_AZURE_STORAGE_ENDPOINT="$AZURE_BACKEND_ENDPOINT"
export azurebackendmismatch_AZURE_STORAGE_ACCESS_KEY="$AZURE_BACKEND_ACCESS_KEY"
export azurebackendmismatch_AZURE_STORAGE_ACCOUNT_NAME="$AZURE_BACKEND_ACCOUNT_NAME"
export azurebackendmismatch_AZURE_STORAGE_ENDPOINT="$AZURE_BACKEND_ENDPOINT"
export azurenonexistcontainer_AZURE_STORAGE_ACCESS_KEY="$AZURE_BACKEND_ACCESS_KEY"
export azurenonexistcontainer_AZURE_STORAGE_ACCOUNT_NAME="$AZURE_BACKEND_ACCOUNT_NAME"
export azurenonexistcontainer_AZURE_STORAGE_ENDPOINT="$AZURE_BACKEND_ENDPOINT"
export azuretest_AZURE_BLOB_ENDPOINT="$AZURE_BACKEND_ENDPOINT"
EOF

source /root/.aws/exports &> /dev/null
cat >>/root/.aws/credentials <<EOF
[default]
aws_access_key_id = $AWS_S3_BACKEND_ACCESS_KEY
aws_secret_access_key = $AWS_S3_BACKEND_SECRET_KEY
[default_2]
aws_access_key_id = $AWS_S3_BACKEND_ACCESS_KEY_2
aws_secret_access_key = $AWS_S3_BACKEND_SECRET_KEY_2
[google]
aws_access_key_id = $AWS_GCP_BACKEND_ACCESS_KEY
aws_secret_access_key = $AWS_GCP_BACKEND_SECRET_KEY
[google_2]
aws_access_key_id = $AWS_GCP_BACKEND_ACCESS_KEY_2
aws_secret_access_key = $AWS_GCP_BACKEND_SECRET_KEY_2
EOF
