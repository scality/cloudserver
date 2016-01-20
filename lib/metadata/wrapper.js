import BucketClientInterface from './bucketclient/backend';
import inMemory from './in_memory/backend';

let client;
let implName;

if (process.env.S3BACKEND && process.env.S3BACKEND === 'mem') {
    client = inMemory;
    implName = "memorybucket";
} else {
    client = new BucketClientInterface();
    implName = "bucketclient";
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
        client.createBucket(bucketName, bucketMD, log, err => {
            if (err) {
                log.error(`${implName}: createBucket: ${bucketName}: ${err}`);
                return cb(errorMap(err));
            }
            return cb(err);
        });
    },

    updateBucket: (bucketName, bucketMD, log, cb) => {
        client.putBucketAttributes(bucketName, bucketMD, log, err => {
            if (err) {
                log.error(`${implName}: updateBucket: ${bucketName}: ${err}`);
                return cb(errorMap(err));
            }
            return cb(err);
        });
    },

    getBucket: (bucketName, log, cb) => {
        client.getBucketAttributes(bucketName, log, (err, data) => {
            if (err) {
                log.error(`${implName}: getBucket: ${bucketName}: ${err}`);
                return cb(errorMap(err));
            }
            return cb(err, data);
        });
    },

    deleteBucket: (bucketName, log, cb) => {
        client.deleteBucket(bucketName, log, err => {
            if (err) {
                log.error(`${implName}: deleteBucket: ${bucketName}: ${err}`);
                return cb(errorMap(err));
            }
            return cb(err);
        });
    },

    putObjectMD: (bucketName, objName, objVal, log, cb) => {
        client.putObject(bucketName, objName, objVal, log, err => {
            if (err) {
                log.error(`${implName}: putObjectMD: ${bucketName}: ${err}`);
                return cb(errorMap(err));
            }
            return cb(err);
        });
    },

    getObjectMD: (bucketName, objName, log, cb) => {
        client.getObject(bucketName, objName, log, (err, data) => {
            if (err) {
                log.error(`${implName}: getObjectMD: ${bucketName}: ${err}`);
                return cb(errorMap(err));
            }
            return cb(err, data);
        });
    },

    deleteObjectMD: (bucketName, objName, log, cb) => {
        client.deleteObject(bucketName, objName, log, err => {
            if (err) {
                log.error(`${implName}: deleteObjectMD: ${bucketName}: ${err}`);
                return cb(errorMap(err));
            }
            return cb(err);
        });
    },

    listObject: (bucketName, prefix, marker, delimiter, maxKeys, log, cb) => {
        client
            .listObject(bucketName, { prefix, marker, maxKeys, delimiter, },
                    log, (err, data) => {
                        if (err) {
                            log.error(`${implName}: listObject: ` +
                                `${bucketName}: ${err}`);
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
