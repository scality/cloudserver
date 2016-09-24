'use strict'; // eslint-disable-line strict
require('babel-core/register');

const assert = require('assert');
const fs = require('fs');
const os = require('os');

const async = require('async');
const constants = require('./constants').default;
const config = require('./lib/Config.js').default;
const logger = require('./lib/utilities/logger.js').logger;

const genTopo = require('./lib/data/dpa/topology').default;

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
    const buffer = Buffer.alloc(8, 0);
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
    const confirmBuffer = Buffer.alloc(8, 0);
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
const warning = 'WARNING: Synchronization directory updates are not ' +
    'supported on this platform. Newly written data could be lost ' +
    'if your system crashes before the operating system is able to ' +
    'write directory updates.';
if (os.type() === 'Linux' && os.endianness() === 'LE' && ioctl) {
    try {
        _setDirSyncFlag(dataPath);
        _setDirSyncFlag(metadataPath);
    } catch (err) {
        logger.warn(warning, { error: err });
    }
} else {
    logger.warn(warning);
}

function createDirsTopo(topo, dataPath, callback) {
    // Create dirsNb subdirectories for the data file backend
    async.eachSeries(Object.keys(topo), (key, next) => {
        if (topo[key].constructor !== Object) {
            return next();
        }
        const path = `${dataPath}/${key}`;
        return fs.mkdir(path, err => {
            // If already exists, move on
            if (err && err.errno !== -17) {
                return next(err);
            }
            if (topo[key].leaf) {
                return next();
            }
            return createDirsTopo(topo[key], path, next);
        });
    }, callback);
}

if ((process.env.ENABLE_DP !== 'true') || !constants.topoMD) {
    // Create 3511 subdirectories for the data file backend
    const subDirs = Array.from({ length: constants.folderHash },
        (v, k) => k.toString());
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
} else {
    // if topology does not exist, create it
    // otherwise, import it
    const file = `${__dirname}/${constants.topoFile}.json`;
    let topology;
    try {
        fs.statSync(file);
        // import topology
        try {
            const topo = JSON.parse(fs.readFileSync(file));
            // update weigth distribution
            topology = genTopo.update(topo);
        } catch (error) {
            logger.warn('Failed to import topology', { file, error });
        }
    } catch (error) {
        logger.debug('Not found topology file. Create it');
    }

    if (!topology) {
        // create topology
        topology = genTopo.init(constants.topoMD);
    }
    // write/update topology into file
    fs.writeFileSync(file, JSON.stringify(topology, null, 4), 'utf8');

    // create directories according to the topology
    createDirsTopo(topology, dataPath, err => {
        assert.strictEqual(err, null, `Error creating data files ${err}`);
        logger.info('Init complete.  Go forth and store data.');
    });
}
