#!/bin/sh

# This script is needed because RADOS Gateway
# will open the port before beginning to serve traffic
# causing wait_for_local_port.bash to exit immediately

echo 'Waiting for ceph'
while [ -z "$(curl 127.0.0.1:8001 2>/dev/null)" ]; do
    sleep 1
    echo -n "."
done
