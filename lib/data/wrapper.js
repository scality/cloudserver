import assert from 'assert';
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
    assert(config.sproxyd.bootstrap.length > 0,
           'sproxyd bootstrap list is empty');
    client = new Sproxy({
        bootstrap: config.sproxyd.bootstrap,
        log: config.log,
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
            return cb(null, key);
        });
    },

    get: (key, log, cb) => {
        log.debug('sending get to datastore', { implName, key, method: 'get' });
        client.get(key, log.getSerializedUids(), (err, val) => {
            if (err) {
                log.error('error from sproxyd', { error: err });
                return cb(errors.InternalError);
            }
            return cb(null, val);
        });
    },

    delete: (key, log, cb) => {
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

    switch: (newClient) => {
        client = newClient;
        return client;
    },
};

export default data;
