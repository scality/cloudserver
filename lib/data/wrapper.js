import inMemory from './in_memory/backend';
import Sproxy from 'sproxydclient';

let client;

if (process.env.S3BACKEND && process.env.S3BACKEND === 'mem') {
    client = inMemory;
} else {
    client = new Sproxy();
}

const data = {
    put: (value, cb) => {
        client.put(value, cb);
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
