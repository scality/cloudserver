'use strict'; // eslint-disable-line strict
require('babel-core/register');

const assert = require('assert');
const fs = require('fs');
const os = require('os');

const async = require('async');
const constants = require('./constants').default;
const config = require('./lib/Config.js').default;
const logger = require('./lib/utilities/logger.js').logger;

let ioctl;
try {
    ioctl = require('ioctl');
} catch (err) {
    logger.warn('ioctl dependency is unavailable. skipping...');
}

function _setDirSyncFlag(path) {
    const GETFLAGS = 2148034049;
    const SETFLAGS = 1074292226;
    const FS_DIRSYNC_FL = 65536;
    const buffer = new Buffer(8).fill(0);
    const pathFD = fs.openSync(path, 'r');
    const status = ioctl(pathFD, GETFLAGS, buffer);
    assert.strictEqual(status, 0);
    const currentFlags = buffer.readUIntLE(0, 8);
    const flags = currentFlags | FS_DIRSYNC_FL;
    buffer.writeUIntLE(flags, 0, 8);
    const status2 = ioctl(pathFD, SETFLAGS, buffer);
    assert.strictEqual(status2, 0);
    fs.closeSync(pathFD);
    const pathFD2 = fs.openSync(path, 'r');
    const confirmBuffer = new Buffer(8).fill(0);
    ioctl(pathFD2, GETFLAGS, confirmBuffer);
    assert.strictEqual(confirmBuffer.readUIntLE(0, 8),
        currentFlags | FS_DIRSYNC_FL, 'FS_DIRSYNC_FL not set');
    logger.info('FS_DIRSYNC_FL set');
    fs.closeSync(pathFD2);
}

if (config.backends.data !== 'file' && config.backends.metadata !== 'file') {
    logger.info('No init required. Go forth and store data.');
    process.exit(0);
}

const dataPath = config.filePaths.dataPath;
const metadataPath = config.filePaths.metadataPath;

fs.accessSync(dataPath, fs.F_OK | fs.R_OK | fs.W_OK);
fs.accessSync(metadataPath, fs.F_OK | fs.R_OK | fs.W_OK);

if (os.type() === 'Linux' && os.endianness() === 'LE' && ioctl) {
    _setDirSyncFlag(dataPath);
    _setDirSyncFlag(metadataPath);
} else {
    logger.warn('WARNING: Synchronization directory updates are not ' +
        'supported on this platform. Newly written data could be lost ' +
        'if your system crashes before the operating system is able to ' +
        'write directory updates.');
}

// Create 3511 subdirectories for the data file backend
const subDirs = Array.from({ length: constants.folderHash },
    (v, k) => (k).toString());
async.eachSeries(subDirs, (subDirName, next) => {
    fs.mkdir(`${dataPath}/${subDirName}`, err => {
        // If already exists, move on
        if (err && err.errno !== -17) {
            return next(err);
        }
        return next();
    });
},
 err => {
     assert.strictEqual(err, null, `Error creating data files ${err}`);
     logger.info('Init complete.  Go forth and store data.');
 });
