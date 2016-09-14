import async from 'async';
import { errors } from 'arsenal';
import Sproxy from 'sproxydclient';
import file from './file/backend';
import inMemory from './in_memory/backend';
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
} else if (config.backends.data === 'scality') {
    client = new Sproxy({
        bootstrap: config.sproxyd.bootstrap,
        log: config.log,
        chordCos: config.sproxyd.chordCos,
    });
    implName = 'sproxyd';
}

const data = {
    put: (cipherBundle, value, valueSize, keyContext, log, cb) => {
        assert.strictEqual(typeof valueSize, 'number');
        log.debug('sending put to datastore', { implName, keyContext,
                                                method: 'put' });
        // The callback in the MD5Sum constructor need to do synchronous
        // operations and will be called before the client.put() callback
        let calculatedHash;
        const hashedStream = new MD5Sum(hash => {
            calculatedHash = hash;
        });
        value.pipe(hashedStream);

        let writeStream = hashedStream;
        if (cipherBundle && cipherBundle.cipher) {
            writeStream = cipherBundle.cipher;
            hashedStream.pipe(writeStream);
        }

        client.put(writeStream, valueSize, keyContext, log.getSerializedUids(),
           (err, key) => {
               if (err) {
                   log.error('error from datastore',
                             { error: err, implName });
                   return cb(errors.InternalError);
               }
               const dataRetrievalInfo = {
                   key,
                   dataStoreName: implName,
               };
               return cb(null, dataRetrievalInfo, calculatedHash);
           });
    },

    get: (objectGetInfo, log, cb) => {
        // If objectGetInfo.key exists the md-model-version is 2 or greater.
        // Otherwise, the objectGetInfo is just the key string.
        const key = objectGetInfo.key ? objectGetInfo.key : objectGetInfo;
        const range = objectGetInfo.range;
        log.debug('sending get to datastore', { implName, key,
            range, method: 'get' });
        client.get(key, range, log.getSerializedUids(), (err, stream) => {
            if (err) {
                log.error('error from sproxyd', { error: err });
                return cb(errors.InternalError);
            }
            if (objectGetInfo.cipheredDataKey) {
                const serverSideEncryption = {
                    cryptoScheme: objectGetInfo.cryptoScheme,
                    masterKeyId: objectGetInfo.masterKeyId,
                    cipheredDataKey: new Buffer(objectGetInfo.cipheredDataKey,
                                                'base64'),
                };
                const offset = objectGetInfo.range ? objectGetInfo.range[0] : 0;
                return kms.createDecipherBundle(
                    serverSideEncryption, offset, log,
                    (err, decipherBundle) => {
                        if (err) {
                            log.error('cannot get decipher bundle from kms', {
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
        // If objectGetInfo.key exists the md-model-version is 2 or greater.
        // Otherwise, the objectGetInfo is just the key string.
        const key = objectGetInfo.key ? objectGetInfo.key : objectGetInfo;
        log.debug('sending delete to datastore', { implName, key,
            method: 'delete' });
        client.delete(key, log.getSerializedUids(), err => {
            if (err) {
                log.error('error from sproxyd', { error: err });
                return cb(errors.InternalError);
            }
            return cb();
        });
    },

    // It would be preferable to have an sproxyd batch delete route to
    // replace this
    batchDelete: (locations, log) => {
        async.eachLimit(locations, 5, (loc, next) => {
            data.delete(loc, log, err => {
                if (err) {
                    log.error('one part of a batch delete failed',
                    { location: loc });
                }
                return next();
            });
        },
        () => {
            log.end();
        });
    },

    switch: (newClient) => {
        client = newClient;
        return client;
    },
};

export default data;
