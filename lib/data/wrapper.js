import assert from 'assert';

import Sproxy from 'sproxydclient';

import inMemory from './in_memory/backend';
import Config from '../Config';

let client;

if (process.env.S3BACKEND && process.env.S3BACKEND === 'mem') {
    client = inMemory;
} else {
    const config = new Config();
    assert(config.sproxyd.bootstrap.length > 0,
           'sproxyd bootstrap list is empty');
    client = new Sproxy({ bootstrap: config.sproxyd.bootstrap });
}

const data = {
    put: (value, keyContext, cb) => {
        client.put(value, keyContext, cb);
    },

    get: (keys, cb) => {
        client.get(keys, cb);
    },

    delete: (keys, cb) => {
        client.delete(keys, cb);
    },

    switch: (newClient) => {
        client = newClient;
        return client;
    },
};

export default data;
