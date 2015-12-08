import backend from './in_memory/backend';

let client = backend;

const metadata = {
    getBucket: (bucketName, cb) => {
        client.getBucketAttributes(bucketName, cb);
    },

    switch: (newClient) => {
        client = newClient;
    },
};

export default metadata;
