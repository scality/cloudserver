import async from 'async';
import { errors } from 'arsenal';
import Sproxy from 'sproxydclient';

import inMemory from './in_memory/backend';
import Config from '../Config';

let client;
let implName;

if ((process.env.S3BACKEND && process.env.S3BACKEND === 'mem')
    || (process.env.S3SPROXYD && process.env.S3SPROXYD === 'mem')) {
    client = inMemory;
    implName = 'mem';
} else {
    const config = new Config();
    client = new Sproxy({
        bootstrap: config.sproxyd.bootstrap,
        log: config.log,
        chordCos: config.sproxyd.chordCos,
    });
    implName = 'sproxyd';
}

const data = {
    put: (value, keyContext, log, cb) => {
        log.debug('sending put to datastore', { implName, keyContext,
            method: 'put' });
        client.put(value, keyContext, log.getSerializedUids(), (err, key) => {
            if (err) {
                log.error('error from sproxyd', { error: err });
                return cb(errors.InternalError);
            }
            const dataRetrievalInfo = {
                key,
                dataStoreName: implName,
            };
            return cb(null, dataRetrievalInfo);
        });
    },

    get: (objectGetInfo, log, cb) => {
        const key = objectGetInfo.key;
        const range = objectGetInfo.range;
        log.debug('sending get to datastore', { implName, key,
            range, method: 'get' });
        client.get(key, range, log.getSerializedUids(), (err, val) => {
            if (err) {
                log.error('error from sproxyd', { error: err });
                return cb(errors.InternalError);
            }
            return cb(null, val);
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
