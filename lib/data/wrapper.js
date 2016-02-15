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
        log.debug(`PUT data(${implName})`, { keyContext });
        client.put(value, keyContext, log.getSerializedUids(), cb);
    },

    get: (key, log, cb) => {
        log.debug(`GET data(${implName})`, { key });
        client.get(key, log.getSerializedUids(), cb);
    },

    delete: (key, log, cb) => {
        log.debug(`DELETE data(${implName})`, { key });
        client.delete(key, log.getSerializedUids(), cb);
    },

    switch: (newClient) => {
        client = newClient;
        return client;
    },
};

export default data;
