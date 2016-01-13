import BucketClientInterface from './bucketclient/backend';
import inMemory from './in_memory/backend';

let client;

if (process.env.S3BACKEND && process.env.S3BACKEND === 'mem') {
    client = inMemory;
} else {
    client = new BucketClientInterface();
}

function errorMap(mdError) {
    const map = {
        NoSuchBucket: 'NoSuchBucket',
        BucketAlreadyExists: 'BucketAlreadyExists',
        NoSuchKey: 'NoSuchKey',
        DBNotFound: 'NoSuchBucket',
        DBAlreadyExists: 'BucketAlreadyExists',
        ObjNotFound: 'NoSuchKey',
        NotImplemented: 'NotImplemented',
    };
    return map[mdError] ? map[mdError] : 'InternalError';
}


const metadata = {
    createBucket: (bucketName, bucketMD, log, cb) => {
        client.createBucket(bucketName, bucketMD, err => {
            if (err) {
                return cb(errorMap(err));
            }
            return cb(err);
        });
    },

    updateBucket: (bucketName, bucketMD, log, cb) => {
        client.putBucketAttributes(bucketName, bucketMD, err => {
            if (err) {
                return cb(errorMap(err));
            }
            return cb(err);
        });
    },

    getBucket: (bucketName, log, cb) => {
        client.getBucketAttributes(bucketName, (err, data) => {
            if (err) {
                return cb(errorMap(err));
            }
            return cb(err, data);
        });
    },

    deleteBucket: (bucketName, log, cb) => {
        client.deleteBucket(bucketName, err => {
            if (err) {
                return cb(errorMap(err));
            }
            return cb(err);
        });
    },

    putObjectMD: (bucketName, objName, objVal, log, cb) => {
        client.putObject(bucketName, objName, objVal, err => {
            if (err) {
                return cb(errorMap(err));
            }
            return cb(err);
        });
    },

    getObjectMD: (bucketName, objName, log, cb) => {
        client.getObject(bucketName, objName, (err, data) => {
            if (err) {
                return cb(errorMap(err));
            }
            return cb(err, data);
        });
    },

    deleteObjectMD: (bucketName, objName, log, cb) => {
        client.deleteObject(bucketName, objName, err => {
            if (err) {
                return cb(errorMap(err));
            }
            return cb(err);
        });
    },

    listObject: (bucketName, prefix, marker, delimiter, maxKeys, log, cb) => {
        client
        .listObject(bucketName, { prefix, marker, maxKeys, delimiter, },
                    (err, data) => {
                        if (err) {
                            return cb(errorMap(err));
                        }
                        return cb(err, data);
                    });
    },

    switch: (newClient) => {
        client = newClient;
    },
};

export default metadata;
