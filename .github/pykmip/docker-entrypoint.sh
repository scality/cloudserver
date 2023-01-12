#!/bin/sh

python3 /usr/local/bin/run_server.py 2>&1 | tee -a /artifacts/pykmip.log
