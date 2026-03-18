#!/bin/bash
set -exo pipefail

init_RS() {
  sleep 5
  mongo --port 27018 /conf/initReplicaSet.js
}
init_RS &

# Pre-create log file as world-readable; mongod (logAppend: true) opens it without resetting permissions
# Otherwise mongod create the file with 600 permissions, preventing CI artifact upload
touch /logs/mongod.log && chmod 644 /logs/mongod.log

mongod --bind_ip_all --config=/conf/mongod.conf
