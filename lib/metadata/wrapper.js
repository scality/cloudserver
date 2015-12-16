import bucketclient from 'bucketclient';
import inMemory from './in_memory/backend';

let client;

if (process.argv.length > 2
    && (process.argv[2] === 'mem' || process.argv[2] === '--compilers')) {
    client = inMemory;
} else {
    client = new bucketclient.RESTClient();
}


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

    getObjectMD: (bucketName, objName, cb) => {
        client.getObject(bucketName, objName, cb);
    },

    deleteObjectMD: (bucketName, objName, cb) => {
        client.deleteObject(bucketName, objName, cb);
    },

    listObject: (bucketName, prefix, marker, delimiter, maxKeys, cb) => {
        client
        .listObject(bucketName, { prefix, marker, maxKeys, delimiter, }, cb);
    },

    switch: (newClient) => {
        client = newClient;
    },
};

export default metadata;
