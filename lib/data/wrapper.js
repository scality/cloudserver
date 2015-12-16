import backend from './in_memory/backend';
// import Backend from 'sproxydclient';

let client = backend;
// let client = new Backend();

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
    },
};

export default data;
