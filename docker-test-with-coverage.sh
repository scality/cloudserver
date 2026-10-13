#!/bin/bash

c8 --reporter=none yarn start > /artifacts/s3.log 2> /artifacts/s3-stderr.log &

PID=$!

generate_coverage() {
    echo 'Stopping NodeJS processes...'
    kill -TERM $PID 2>/dev/null || true
    wait $PID
    echo 'Generating coverage report...'
    c8 report --reports-dir /coverage/test --reporter=lcov --reporter=text-summary
}

trap generate_coverage SIGTERM
wait $PID
