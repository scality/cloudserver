import async from 'async';
import { errors } from 'arsenal';

import file from './file/backend';
import inMemory from './in_memory/backend';
import multipleBackendGateway from './multipleBackendGateway';
import config from '../Config';
import MD5Sum from '../utilities/MD5Sum';
import assert from 'assert';
import kms from '../kms/wrapper';

let client;
let implName;

if (config.backends.data === 'mem') {
    client = inMemory;
    implName = 'mem';
} else if (config.backends.data === 'file') {
    client = file;
    implName = 'file';
} else if (config.backends.data === 'multiple') {
    client = multipleBackendGateway;
    implName = 'multipleBackends';
}

/**
 * _retryDelete - Attempt to delete key again if it failed previously
 * @param { string | object } objectGetInfo - either string location of object
 *      to delete or object containing info of object to delete
 * @param {object} log - Werelogs request logger
 * @param {number} count - keeps count of number of times function has been run
 * @param {function} cb - callback
 * @returns undefined and calls callback
 */
const MAX_RETRY = 2;

function _retryDelete(objectGetInfo, log, count, cb) {
    if (count > MAX_RETRY) {
        return cb(errors.InternalError);
    }
    return client.delete(objectGetInfo, log.getSerializedUids(), err => {
        if (err) {
            return _retryDelete(objectGetInfo, log, count + 1, cb);
        }
        return cb();
    });
}

const data = {
    put: (cipherBundle, value, valueSize, keyContext, backendInfo, log, cb) => {
        assert.strictEqual(typeof valueSize, 'number');
        log.debug('sending put to datastore', { implName, keyContext,
                                                method: 'put' });
        const hashedStream = new MD5Sum();
        value.pipe(hashedStream);

        let writeStream = hashedStream;
        if (cipherBundle && cipherBundle.cipher) {
            writeStream = cipherBundle.cipher;
            hashedStream.pipe(writeStream);
        }
        if (implName === 'multipleBackends') {
            // Need to send backendInfo to client.put and
            // client.put will provide dataRetrievalInfo so no
            // need to construct here
            return client.put(writeStream, valueSize, keyContext, backendInfo,
                log.getSerializedUids(), (err, dataRetrievalInfo) => {
                    if (err) {
                        log.error('error from datastore',
                                 { error: err, implName });
                        return cb(errors.InternalError);
                    }
                    return cb(null, dataRetrievalInfo, hashedStream);
                });
        }
        return client.put(writeStream, valueSize, keyContext,
            log.getSerializedUids(), (err, key) => {
                if (err) {
                    log.error('error from datastore',
                             { error: err, implName });
                    return cb(errors.InternalError);
                }
                const dataRetrievalInfo = {
                    key,
                    dataStoreName: implName,
                };
                return cb(null, dataRetrievalInfo, hashedStream);
            });
    },

    get: (objectGetInfo, log, cb) => {
        // If objectGetInfo.key exists the md-model-version is 2 or greater.
        // Otherwise, the objectGetInfo is just the key string.
        const objGetInfo = (implName === 'sproxyd') ?
            objectGetInfo.key : objectGetInfo;
        const range = objectGetInfo.range;
        log.debug('sending get to datastore', { implName,
            key: objectGetInfo.key, range, method: 'get' });
        client.get(objGetInfo, range, log.getSerializedUids(),
            (err, stream) => {
                if (err) {
                    log.error('error from datastore', { error: err, implName });
                    return cb(errors.InternalError);
                }
                if (objectGetInfo.cipheredDataKey) {
                    const serverSideEncryption = {
                        cryptoScheme: objectGetInfo.cryptoScheme,
                        masterKeyId: objectGetInfo.masterKeyId,
                        cipheredDataKey: Buffer.from(
                            objectGetInfo.cipheredDataKey, 'base64'),
                    };
                    const offset = objectGetInfo.range ?
                        objectGetInfo.range[0] : 0;
                    return kms.createDecipherBundle(
                        serverSideEncryption, offset, log,
                        (err, decipherBundle) => {
                            if (err) {
                                log.error('cannot get decipher bundle ' +
                                    'from kms', {
                                        method: 'data.wrapper.data.get',
                                    });
                                return cb(err);
                            }
                            stream.pipe(decipherBundle.decipher);
                            return cb(null, decipherBundle.decipher);
                        });
                }
                return cb(null, stream);
            });
    },

    delete: (objectGetInfo, log, cb) => {
        const callback = cb || log.end;
        // If objectGetInfo.key exists the md-model-version is 2 or greater.
        // Otherwise, the objectGetInfo is just the key string.
        const objGetInfo = (implName === 'sproxyd') ?
            objectGetInfo.key : objectGetInfo;
        log.trace('sending delete to datastore', {
            implName,
            key: objectGetInfo.key,
            method: 'delete',
        });
        _retryDelete(objGetInfo, log, 0, err => {
            if (err) {
                log.error('error deleting object from datastore',
                    { error: err, key: objectGetInfo.key });
            }
            return callback(err);
        });
    },

    // It would be preferable to have an sproxyd batch delete route to
    // replace this
    batchDelete: (locations, log) => {
        // TODO: The method of persistence of sproxy delete key will
        // be finalized; refer Issue #312 for the discussion. In the
        // meantime, we at least log the location of the data we are
        // about to delete before attempting its deletion.
        log.trace('initiating batch delete', {
            keys: locations,
            implName,
            method: 'batchDelete',
        });
        async.eachLimit(locations, 5, (loc, next) => {
            data.delete(loc, log, next);
        },
        err => {
            if (err) {
                log.error('batch delete failed', { error: err });
            } else {
                log.trace('batch delete successfully completed');
            }
            log.end();
        });
    },

    switch: newClient => {
        client = newClient;
        return client;
    },

    checkHealth: (log, cb) => {
        if (!client.healthcheck) {
            const defResp = {};
            defResp[implName] = { code: 200, message: 'OK' };
            return cb(null, defResp);
        }
        return client.healthcheck(log, (err, result) => {
            let respBody = {};
            if (err) {
                log.error(`error from ${implName}`, { error: err });
                respBody[implName] = {
                    error: err,
                };
                // error returned as null so async parallel doesn't return
                // before all backends are checked
                return cb(null, respBody);
            }
            if (implName === 'multipleBackends') {
                respBody = result;
                return cb(null, respBody);
            }
            respBody[implName] = {
                code: result.statusCode,
                message: result.statusMessage,
            };
            return cb(null, respBody);
        });
    },
};

export default data;
