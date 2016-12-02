'use strict';

require('babel-core/register');
require('./bitmapd-utils.js').default.connectToS3('127.0.0.1', 7000);
require('./bitmapd-utils.js').default.opendb();
