#!/bin/sh
// 2>/dev/null ; exec "$(which nodejs || which node)" "$0" "$@"
'use strict';

require('../lib/kms/utilities.js').createEncryptedBucket();
