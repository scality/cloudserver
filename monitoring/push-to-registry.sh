#!/bin/bash

# push a file to a registry as an OCI image
#
# $1: name:tag name + tag of the file to push
# $2: input_file file to push
# $3: mime_type mime type of the file

set -e

if [[ $# -ne 3 ]]
then
    echo "usage: $0 <name:tag> <input_file> <mime_type>"
    exit 1
fi

ORAS=$(which oras)

if [[ ${ORAS} == "" ]]
then
    echo "ERR: package 'oras' not found, please install it first"
    echo "using: https://oras.land/cli/"
    exit 1
fi

NAME_TAG=$1
INPUT_FILE=$2
MIME_TYPE=$3

if [[ -z ${NAME_TAG} ]]
then
    echo "empty name:tag"
    exit 1
fi

if [[ ! -f ${INPUT_FILE} ]]
then
    echo "input file '${INPUT_FILE}' not found"
    exit 1
fi

if [[ -z ${MIME_TYPE} ]]
then
    echo "empty mime type"
    exit 1
fi

REGISTRY=${REGISTRY:-"registry.scality.com"}
PROJECT=${PROJECT:-"cloudserver-dev"}

set -x
${ORAS} push "${REGISTRY}/${PROJECT}/${NAME_TAG}" "${INPUT_FILE}:${MIME_TYPE}"
