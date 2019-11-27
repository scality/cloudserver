#!/bin/bash

LOCAL_BRANCH=$(git branch | grep \* | cut -d ' ' -f2)
BRANCHES=(development q stabilization)

for branch in ${BRANCHES[@]}; do
    if echo "${LOCAL_BRANCH}\/" | grep -q ^${branch} ; then
        cat .git/HEAD | sed 's/.*\///'
        exit 0
    fi
done

echo 0.0.0
