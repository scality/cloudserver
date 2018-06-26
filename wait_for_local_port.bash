#!/usr/bin/env bash
set -e
wait_for_local_port() {
    local port=$1
    local timeout=$2
    local count=0
    local ret=1
    echo "waiting for S3:$port"
    while [[ "$ret" -eq "10" && "$count" -lt "$timeout" ]] ; do
        nc -z -w 1 localhost $port
        ret=$?
        if [ ! "$ret" -eq "0" ]; then
            echo -n .
            sleep 1
            count=$(($count+1))
        fi
    done

    echo ""

    if [[ "$count" -eq "$timeout" ]]; then
        echo "Server did not start in less than $timeout seconds. Exiting..."
        exit 1
    fi

    echo "Server got ready in ~${count} seconds. Starting test now..."
}

wait_for_local_port $1 $2
