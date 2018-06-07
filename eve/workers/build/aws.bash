#!/bin/bash -x
set -x #echo on
set -e #exit at the first error

cat >>/root/.aws/exports <<EOF
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
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
mkdir -p ~/.aws
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

