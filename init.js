'use strict'; // eslint-disable-line strict
require('babel-core/register');

const assert = require('assert');
const fs = require('fs');
const os = require('os');

const async = require('async');

const constants = require('./constants').default;
const config = require('./lib/Config.js').default;
const logger = require('./lib/utilities/logger.js').logger;
const storageUtils = require('arsenal').storage.utils;

// If neither data nor metadata is using the file backend,
// there is no need to init
if (config.backends.data !== 'file' && config.backends.data !== 'multiple' &&
    config.backends.metadata !== 'file') {
    logger.info('No init required. Go forth and store data.');
    process.exit(0);
}

const dataPath = config.filePaths.dataPath;

fs.accessSync(dataPath, fs.F_OK | fs.R_OK | fs.W_OK);
const warning = 'WARNING: Synchronization directory updates are not ' +
    'supported on this platform. Newly written data could be lost ' +
    'if your system crashes before the operating system is able to ' +
    'write directory updates.';
if (os.type() === 'Linux' && os.endianness() === 'LE') {
    try {
        storageUtils.setDirSyncFlag(dataPath);
    } catch (err) {
        logger.warn(warning, { error: err.stack });
    }
} else {
    logger.warn(warning);
}

// Create 3511 subdirectories for the data file backend
const subDirs = Array.from({ length: constants.folderHash },
    (v, k) => (k).toString());
async.eachSeries(subDirs, (subDirName, next) => {
    fs.mkdir(`${dataPath}/${subDirName}`, err => {
        // If already exists, move on
        if (err && err.code !== 'EEXIST') {
            return next(err);
        }
        return next();
    });
},
 err => {
     assert.strictEqual(err, null, `Error creating data files ${err}`);
     logger.info('Init complete.  Go forth and store data.');
 });
