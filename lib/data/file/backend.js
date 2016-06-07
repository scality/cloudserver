import { errors, stringHash } from 'arsenal';
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


function getFilePath(key) {
    const hash = stringHash(key);
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
            return request.on('data', data => {
                // Disable data events as we need to wait for fs.write callback
                request.pause();
                hash.update(data);
                return fs.write(fd, data, 0, data.length,
                    err => {
                        if (err) {
                            log.error('error writing data', { error: err });
                            fs.close(fd);
                            return callback(errors.InternalError);
                        }
                        return request.resume(); // Allow data events again
                    });
            })
            .on('error', err => {
                log.error('error streaming data from request', { error: err });
                fs.close(fd);
                return callback(errors.InternalError);
            })
            .on('end', () => {
                // eslint-disable-next-line no-param-reassign
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
        const rs = fs.createReadStream(filePath, readStreamOptions)
            .on('error', err => {
                log.error('error retrieving file', { error: err });
                return callback(errors.InternalError);
            })
            .on('open', () => { callback(null, rs); });
    },

    delete: function delFile(key, reqUids, callback) {
        const log = createLogger(reqUids);
        const filePath = getFilePath(key);
        log.debug('deleting file', { filePath });
        return fs.unlink(filePath, callback);
    },
};

export default backend;
