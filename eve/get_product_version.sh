#!/bin/sh

cat .git/HEAD | sed 's/.*\///'
