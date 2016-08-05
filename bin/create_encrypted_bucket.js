#!/bin/sh
// 2>/dev/null ; exec "$(which nodejs || which node)" "$0" "$@"
'use strict'; // eslint-disable-line strict

require('babel-core/register');
require('../lib/kms/utilities.js').createEncryptedBucket();
