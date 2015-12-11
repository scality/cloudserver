import backend from './in_memory/backend';

let client = backend;

const metadata = {
    createBucket: (bucketName, bucketMD, cb) => {
        client.createBucket(bucketName, bucketMD, cb);
    },

    getBucket: (bucketName, cb) => {
        client.getBucketAttributes(bucketName, cb);
    },

    deleteBucket: (bucketName, cb) => {
        client.deleteBucket(bucketName, cb);
    },

    putObjectMD: (bucketName, objName, objVal, cb) => {
        client.putObject(bucketName, objName, objVal, cb);
    },

    switch: (newClient) => {
        client = newClient;
    },
};

export default metadata;
