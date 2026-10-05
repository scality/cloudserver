#!/bin/sh

# This script is needed because RADOS Gateway
# will open the port before beginning to serve traffic
# causing wait_for_local_port.bash to exit immediately

TIMEOUT=600

echo 'Waiting for ceph'
elapsed=0
while [ -z "$(curl 127.0.0.1:8001 2>/dev/null)" ]; do
    sleep 1
    echo -n "."
    elapsed=$((elapsed + 1))
    if [ "$elapsed" -ge "$TIMEOUT" ]; then
        echo
        echo "Ceph not ready after ${TIMEOUT}s"
        echo "List of containers:"
        docker ps -a
        echo "Logs of ceph container:"
        docker logs docker-ceph-1
        echo "Host network:"
        cat /proc/net/dev
        ip -4 -o addr
        exit 1
    fi
done
