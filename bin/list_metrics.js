#!/usr/bin/env node
'use strict'; // eslint-disable-line strict

require('babel-core/register');
require('../lib/utapi/utilities.js').listMetrics();
