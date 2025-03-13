#!/bin/bash
set -exo pipefail

init_RS() {
  sleep 5
  mongosh --port 27018 /conf/initReplicaSet.js
}
init_RS &

mongod --bind_ip_all --config=/conf/mongod.conf
