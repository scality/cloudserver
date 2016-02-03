import assert from 'assert';

import Sproxy from 'sproxydclient';

import inMemory from './in_memory/backend';
import Config from '../Config';

let client;
let implName;

if (process.env.S3BACKEND && process.env.S3BACKEND === 'mem') {
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
        client.put(value, keyContext, log.getSerializedUids(), cb);
    },

    get: (key, log, cb) => {
        log.debug('sending get to datastore', { implName, key, method: 'get' });
        client.get(key, log.getSerializedUids(), cb);
    },

    delete: (key, log, cb) => {
        log.debug('sending delete to datastore', { implName, key,
            method: 'delete' });
        client.delete(key, log.getSerializedUids(), cb);
    },

    switch: (newClient) => {
        client = newClient;
        return client;
    },
};

export default data;
