#!/usr/bin/env bash
# Wait until the mongo replica set has elected a primary.
#
# mongo-run.sh starts mongod immediately but backgrounds rs.initiate() behind a
# `sleep 5`, so the port answers well before the set exists. A TCP check
# therefore lets tests start against a mongo with no primary, and cloudserver,
# which blocks on its metadata backend during start-up, then never binds 8000.
#
# usage: wait_for_mongo_primary.bash [timeout_seconds]
set -uo pipefail

timeout=${1:-120}
count=0

# compose was brought up from .github/docker, so resolve the project from there
cd "$(dirname "$0")/.github/docker" || exit 1

mongo_state() {
    docker compose exec -T mongo \
        mongo --port 27018 --quiet --eval 'rs.status().myState' 2>/dev/null \
        | tr -dc '0-9'
}

echo "waiting for mongo replica set primary"
while [ "$count" -lt "$timeout" ]; do
    # myState 1 is PRIMARY; anything else, or a failed exec, means not ready
    if [ "$(mongo_state)" = "1" ]; then
        echo ""
        echo "Mongo primary ready in ~${count} seconds. Starting test now..."
        exit 0
    fi
    echo -n .
    sleep 1
    count=$((count + 1))
done

echo ""
echo "Mongo replica set had no primary after ${timeout} seconds. Exiting..."
docker compose exec -T mongo \
    mongo --port 27018 --quiet --eval 'rs.status()' 2>&1 | tail -30 || true
exit 1
