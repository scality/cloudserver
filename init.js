'use strict'; // eslint-disable-line strict
require('babel-core/register');

const assert = require('assert');
const async = require('async');
const fs = require('fs');
const constants = require('./constants').default;
const config = require('./lib/Config.js').default;

if (config.backends.data !== 'file' && config.backends.metadata !== 'file') {
    process.stdout.write('No init required.' + '\n');
    process.exit(0);
}

const dataPath = config.filePaths.dataPath;
const metadataPath = config.filePaths.metadataPath;

fs.accessSync(dataPath, fs.F_OK | fs.R_OK | fs.W_OK);
fs.accessSync(metadataPath, fs.F_OK | fs.R_OK | fs.W_OK);

// TODO: ioctl on the data and metadata directories fd,
// with params FS_IOC_SETFLAGS and FS_DIRSYNC_FL


// Create 3511 subfolders for the data file backend
const arr = Array.from({ length: constants.folderHash },
    (v, k) => (k + 1).toString());
async.eachSeries(arr, (fileName, next) => {
    fs.mkdir(`${dataPath}/${fileName}`, err => {
        // If already exists, move on
        if (err && err.errno !== -17) {
            return next(err);
        }
        return next(null);
    });
},
 err => {
     assert.strictEqual(err, null, `Error creating data files ${err}`);
     process.stdout.write('Init complete.  Go forth and store data.' + '\n');
 });
