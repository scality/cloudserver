import { errors } from 'arsenal';
import crypto from 'crypto';
import fs from 'fs';
import config from '../../Config';
import Logger from 'werelogs';

const logger = new Logger('FileDataBackend', {
    logLevel: config.log.logLevel,
    dumpLevel: config.log.dumpLevel,
});
const STORAGE_PATH = '/data';
const FOLDER_HASH = 3511; // Prime number

function createLogger(reqUids) {
    return reqUids ?
        logger.newRequestLoggerFromSerializedUids(reqUids) :
        logger.newRequestLogger();
}

/*
* Each object/part becomes a file and the files are stored
* in a directory hash structure
* under STORAGE_PATH
*/

function hashCode(key) {
    let hash = 0;
    for (let i = 0; i < key.length; i++) {
        const char = key.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convert to 32bit integer
    }
    return (hash < 0) ? -hash : hash;
}

function getFilePath(key) {
    const hash = hashCode(key);
    const folderHashPath = ((hash % FOLDER_HASH) + 1).toString();
    return `${STORAGE_PATH}/${folderHashPath}/${key}`;
}

export const backend = {
    put: function putFile(request, keyContext, reqUids, callback) {
        const log = createLogger(reqUids);
        // Consider making async
        const key = crypto.randomBytes(20).toString('hex');
        const filePath = getFilePath(key);
        const hash = crypto.createHash('md5');

        request.pause();

        fs.open(filePath, 'w', (err, fd) => {
            if (err) {
                log.error('error opening filePath', { error: err });
                return callback(errors.InternalError);
            }
            request.resume();
            request.on('data', data => {
                // Disable data events as we need to wait for fs.write callback
                request.pause();
                hash.update(data);
                return fs.write(fd, data, 0, data.length,
                    err => {
                        if (err) {
                            log.error('error writing data', { error: err });
                            return callback(errors.InternalError);
                        }
                        request.resume(); // Allow data events again
                    });
            });
            request.on('error', () => {
                log.error('error streaming data from request', { error: err });
                return callback(errors.InternalError);
            });
            request.on('end', () => {
                request.calculatedHash = hash.digest('hex');
                log.debug('finished writing data', { key,
                    calculatedHash: request.calculatedHash });
                fs.close(fd);
                return callback(null, key);
            });
        });
    },

    get: function getFile(key, range, reqUids, callback) {
        const log = createLogger(reqUids);
        const filePath = getFilePath(key);
        log.debug('opening readStream to get data', { filePath });
        const readStreamOptions = {
            flags: 'r',
            encoding: null,
            fd: null,
            autoClose: true,
        };
        if (range) {
            readStreamOptions.start = range[0];
            readStreamOptions.end = range[1];
        }
        const rs = fs.createReadStream(filePath, readStreamOptions);
        return callback(null, rs);
    },

    delete: function delFile(key, reqUids, callback) {
        const log = createLogger(reqUids);
        const filePath = getFilePath(key);
        log.debug('deleting file', { filePath });
        return fs.unlink(filePath, callback);
    },
};

export default backend;
