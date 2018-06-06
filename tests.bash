#!/bin/bash -x
set -x #echo on
set -e #exit at the first error

source /home/eve/.bashrc &> /dev/null
source ./eve/workers/build/bash_profile &> /dev/null

MYPWD=$(pwd)

killandsleep () {
  kill -9 $(lsof -t -i:$1) || true
  sleep 10
}

export REMOTE_MANAGEMENT_DISABLE=1

if [ $CIRCLE_NODE_INDEX -eq 0 ]
then

#  npm run --silent lint -- --max-warnings 0
#
#  npm run --silent lint_md
#
#  flake8 $(git ls-files "*.py")
#
#  yamllint $(git ls-files "*.yml")
#
#  mkdir -p $CIRCLE_TEST_REPORTS/unit
#
#  npm run unit_coverage
#
#  npm run unit_coverage_legacy_location

  npm run start_dmd &
  bash wait_for_local_port.bash 9990 40 &&
  npm run multiple_backend_test
  killandsleep 9990

  # Run S3 with multiple data backends ; run ft_tests
  S3BACKEND=mem S3DATA=multiple npm start > $CIRCLE_ARTIFACTS/server_multiple_java.txt &
  bash wait_for_local_port.bash 8000 40
  pushd ./tests/functional/jaws
  mvn test

  killandsleep 8000
  cd $MYPWD

  S3BACKEND=mem S3DATA=multiple npm start > $CIRCLE_ARTIFACTS/server_multiple_fog.txt &
  bash wait_for_local_port.bash 8000 40
  pushd tests/functional/fog
  rspec tests.rb

  cd $MYPWD
  killandsleep 8000

  S3BACKEND=mem MPU_TESTING=yes S3DATA=multiple npm start > $CIRCLE_ARTIFACTS/server_multiple_awssdk.txt &
  bash wait_for_local_port.bash 8000 40
  S3DATA=multiple npm run ft_awssdk

  cd $MYPWD
  killandsleep 8000

  # Run external backend tests with proxy ; run ft_awssdk_external_backends

  S3BACKEND=mem MPU_TESTING=yes S3DATA=multiple CI_PROXY=true npm start > $CIRCLE_ARTIFACTS/server_external_backends_proxy_awssdk.txt &
  bash wait_for_local_port.bash 8000 40
  S3DATA=multiple CI_PROXY=true npm run ft_awssdk_external_backends

  killandsleep 8000

fi

if [ $CIRCLE_NODE_INDEX -eq 1 ]
then

  # Run S3 with multiple data backends + KMS Encryption; run ft_awssdk

  S3BACKEND=mem MPU_TESTING=yes S3DATA=multiple npm start > $CIRCLE_ARTIFACTS/server_multiple_kms_awssdk.txt & bash wait_for_local_port.bash 8000 40 && S3DATA=multiple ENABLE_KMS_ENCRYPTION=true npm run ft_awssdk

  killandsleep 8000

  # Run S3 with mem Backend ; run ft_tests

  S3BACKEND=mem npm start > $CIRCLE_ARTIFACTS/server_mem_java.txt & bash wait_for_local_port.bash 8000 40 && cd ./tests/functional/jaws && mvn test

  cd $MYPWD
  killandsleep 8000

  S3BACKEND=mem npm start > $CIRCLE_ARTIFACTS/server_mem_fog.txt & bash wait_for_local_port.bash 8000 40 && cd tests/functional/fog && rspec tests.rb

  cd $MYPWD
  killandsleep 8000

fi

if [ $CIRCLE_NODE_INDEX -eq 2 ]
then

  S3BACKEND=mem MPU_TESTING=yes npm start > $CIRCLE_ARTIFACTS/server_mem_awssdk.txt & bash wait_for_local_port.bash 8000 40 && S3DATA=file npm run ft_awssdk

  killandsleep 8000

  S3BACKEND=mem npm start > $CIRCLE_ARTIFACTS/server_mem_s3cmd.txt & bash wait_for_local_port.bash 8000 40 && S3DATA=file npm run ft_s3cmd

  killandsleep 8000

  S3BACKEND=mem npm start > $CIRCLE_ARTIFACTS/server_mem_s3curl.txt & bash wait_for_local_port.bash 8000 40 && S3DATA=file npm run ft_s3curl

  killandsleep 8000

  S3BACKEND=mem npm start > $CIRCLE_ARTIFACTS/server_mem_rawnode.txt & bash wait_for_local_port.bash 8000 40 && S3DATA=file npm run ft_node

  killandsleep 8000

  # Run S3 with mem Backend + KMS Encryption ; run ft_tests

  S3BACKEND=mem MPU_TESTING=yes npm start > $CIRCLE_ARTIFACTS/server_mem_kms_awssdk.txt & bash wait_for_local_port.bash 8000 40 && ENABLE_KMS_ENCRYPTION=true S3DATA=file npm run ft_awssdk

  killandsleep 8000

  S3BACKEND=mem npm start > $CIRCLE_ARTIFACTS/server_mem_kms_s3cmd.txt & bash wait_for_local_port.bash 8000 40 && ENABLE_KMS_ENCRYPTION=true S3DATA=file npm run ft_s3cmd

  killandsleep 8000

  S3BACKEND=mem npm start > $CIRCLE_ARTIFACTS/server_mem_kms_s3curl.txt & bash wait_for_local_port.bash 8000 40 && ENABLE_KMS_ENCRYPTION=true S3DATA=file npm run ft_s3curl

  killandsleep 8000

  S3BACKEND=mem npm start > $CIRCLE_ARTIFACTS/server_mem_kms_rawnode.txt & bash wait_for_local_port.bash 8000 40 && ENABLE_KMS_ENCRYPTION=true S3DATA=file npm run ft_node

  killandsleep 8000

  # Run with mongdb backend ; run ft_tests

  S3BACKEND=mem MPU_TESTING=yes S3METADATA=mongodb npm start > $CIRCLE_ARTIFACTS/server_mongodb_awssdk.txt & bash wait_for_local_port.bash 8000 40 && S3DATA=file S3METADATA=mongodb npm run ft_awssdk

  killandsleep 8000

  S3BACKEND=mem MPU_TESTING=yes S3METADATA=mongodb npm start > $CIRCLE_ARTIFACTS/server_mongodb_s3cmd.txt & bash wait_for_local_port.bash 8000 40 && S3DATA=file S3METADATA=mongodb npm run ft_s3cmd

  killandsleep 8000

  S3BACKEND=mem MPU_TESTING=yes S3METADATA=mongodb npm start > $CIRCLE_ARTIFACTS/server_mongodb_s3curl.txt & bash wait_for_local_port.bash 8000 40 && S3DATA=file S3METADATA=mongodb npm run ft_s3curl

  killandsleep 8000

  S3BACKEND=mem MPU_TESTING=yes S3METADATA=mongodb npm start > $CIRCLE_ARTIFACTS/server_mongodb_healthchecks.txt & bash wait_for_local_port.bash 8000 40 && S3DATA=file S3METADATA=mongodb npm run ft_healthchecks

  killandsleep 8000

  S3BACKEND=mem MPU_TESTING=yes S3METADATA=mongodb npm start > $CIRCLE_ARTIFACTS/server_mongodb_management.txt & bash wait_for_local_port.bash 8000 40 && S3METADATA=mongodb npm run ft_management

  killandsleep 8000

fi

if [ $CIRCLE_NODE_INDEX -eq 3 ]
then

  # Run S3 with file Backend ; run ft_tests

  S3BACKEND=file S3VAULT=mem MPU_TESTING=yes npm start > $CIRCLE_ARTIFACTS/server_file_awssdk.txt & bash wait_for_local_port.bash 8000 40 && S3DATA=file npm run ft_awssdk

  killandsleep 8000

  S3BACKEND=file S3VAULT=mem npm start > $CIRCLE_ARTIFACTS/server_file_management.txt & bash wait_for_local_port.bash 8000 40 && npm run ft_management

  killandsleep 8000

  S3BACKEND=file S3VAULT=mem npm start > $CIRCLE_ARTIFACTS/server_file_s3cmd.txt & bash wait_for_local_port.bash 8000 40 && S3DATA=file npm run ft_s3cmd

  killandsleep 8000

  S3BACKEND=file S3VAULT=mem npm start > $CIRCLE_ARTIFACTS/server_file_s3curl.txt & bash wait_for_local_port.bash 8000 40 && S3DATA=file npm run ft_s3curl

  killandsleep 8000

  S3BACKEND=file S3VAULT=mem npm start > $CIRCLE_ARTIFACTS/server_file_rawnode.txt & bash wait_for_local_port.bash 8000 40 && S3DATA=file npm run ft_node

  killandsleep 8000

  # Run S3 with file Backend + KMS Encryption ; run ft_tests

  S3BACKEND=file S3VAULT=mem MPU_TESTING=yes npm start > $CIRCLE_ARTIFACTS/server_file_kms_awssdk.txt & bash wait_for_local_port.bash 8000 40 && ENABLE_KMS_ENCRYPTION=true S3DATA=file npm run ft_awssdk

  killandsleep 8000

  S3BACKEND=file S3VAULT=mem npm start > $CIRCLE_ARTIFACTS/server_file_kms_s3cmd.txt & bash wait_for_local_port.bash 8000 40 && ENABLE_KMS_ENCRYPTION=true S3DATA=file npm run ft_s3cmd

  killandsleep 8000

  S3BACKEND=file S3VAULT=mem npm start > $CIRCLE_ARTIFACTS/server_file_kms_s3curl.txt & bash wait_for_local_port.bash 8000 40 && ENABLE_KMS_ENCRYPTION=true S3DATA=file npm run ft_s3curl

  killandsleep 8000

  S3BACKEND=file S3VAULT=mem npm start > $CIRCLE_ARTIFACTS/server_file_kms_rawnode.txt & bash wait_for_local_port.bash 8000 40 && ENABLE_KMS_ENCRYPTION=true S3DATA=file npm run ft_node

  killandsleep 8000

  S3BACKEND=file S3VAULT=mem npm start > $CIRCLE_ARTIFACTS/server_file_kms_management.txt & bash wait_for_local_port.bash 8000 40 && ENABLE_KMS_ENCRYPTION=true npm run ft_management

  killandsleep 8000

  S3BACKEND=mem ENABLE_LOCAL_CACHE=true npm start > $CIRCLE_ARTIFACTS/server_mem_healthchecks.txt & bash wait_for_local_port.bash 8000 40 && ENABLE_LOCAL_CACHE=true S3DATA=file npm run ft_healthchecks

  killandsleep 8000

fi

exit $?
