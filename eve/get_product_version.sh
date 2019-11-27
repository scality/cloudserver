#!/bin/bash

script_full_path=$(readlink -f "$0")
file_dir=$(dirname "$script_full_path")/..

PACKAGE_VERSION=$(cat $file_dir/package.json \
  | grep version \
  | head -1 \
  | awk -F: '{ print $2 }' \
  | sed 's/[",]//g' \
  | tr -d '[[:space:]]')

echo $PACKAGE_VERSION
