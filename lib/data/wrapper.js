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
        const keyStr = JSON.stringify(keyContext);
        log.debug(`data(${implName}): sending PUT keyContext=${keyStr}`);
        client.put(value, keyContext, log.getSerializedUids(), cb);
    },

    get: (keys, log, cb) => {
        log.debug(`data(${implName}): sending GET keys=${keys.join(',')}`);
        client.get(keys, log.getSerializedUids(), cb);
    },

    delete: (keys, log, cb) => {
        log.debug(`data(${implName}): sending DELETE keys=${keys.join(',')}`);
        client.delete(keys, log.getSerializedUids(), cb);
    },

    switch: (newClient) => {
        client = newClient;
        return client;
    },
};

export default data;
