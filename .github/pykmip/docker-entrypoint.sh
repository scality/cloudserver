#!/bin/sh

# Keep using run_server.py instead of pykmip_server to have DEBUG logging

python3 /usr/local/bin/run_server.py 2>&1 | tee -a /artifacts/pykmip.log
