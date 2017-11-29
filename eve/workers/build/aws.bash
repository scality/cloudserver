#!/bin/bash -x
set -x #echo on
set -e #exit at the first error
mkdir -p ~/.aws
cat >>~/.aws/credentials <<EOF
[default]
aws_access_key_id = $AWS_S3_BACKEND_ACCESS_KEY
aws_secret_access_key = $AWS_S3_BACKEND_SECRET_KEY
[default_2]
aws_access_key_id = $AWS_S3_BACKEND_ACCESS_KEY_2
aws_secret_access_key = $AWS_S3_BACKEND_SECRET_KEY_2
EOF
