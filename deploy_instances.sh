#!/bin/bash
for i in $(seq 1 $1);
do
    node ./portConfig.js
    npm run antidote_backend &
    sleep 3
done
