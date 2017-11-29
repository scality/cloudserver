#!/bin/bash -x
set -x #echo on
set -e #exit at the first error

killandsleep () {
  kill -9 $(lsof -t -i:$1) || true
  sleep 10
}
