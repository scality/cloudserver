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
        log.debug('Creating bucket in Metadata');
        client.createBucket(bucketName, bucketMD, log, err => {
            if (err) {
                log.warn(`${implName}: createBucket: ${bucketName}: ${err}`);
                return cb(errorMap(err));
            }
            log.debug('Bucket created in Metadata');
            return cb(err);
        });
    },

    updateBucket: (bucketName, bucketMD, log, cb) => {
        log.debug('Updating bucket in Metadata');
        client.putBucketAttributes(bucketName, bucketMD, log, err => {
            if (err) {
                log.warn(`${implName}: updateBucket: ${bucketName}: ${err}`);
                return cb(errorMap(err));
            }
            log.debug('Bucket updated in Metadata');
            return cb(err);
        });
    },

    getBucket: (bucketName, log, cb) => {
        log.debug('Getting bucket from Metadata');
        client.getBucketAttributes(bucketName, log, (err, data) => {
            if (err) {
                log.warn(`${implName}: getBucket: ${bucketName}: ${err}`);
                return cb(errorMap(err));
            }
            log.debug('Bucket retrieved from Metadata');
            return cb(err, data);
        });
    },

    deleteBucket: (bucketName, log, cb) => {
        log.debug('Deleting bucket from Metadata');
        client.deleteBucket(bucketName, log, err => {
            if (err) {
                log.warn(`${implName}: deleteBucket: ${bucketName}: ${err}`);
                return cb(errorMap(err));
            }
            log.debug('Deleted bucket from Metadata');
            return cb(err);
        });
    },

    putObjectMD: (bucketName, objName, objVal, log, cb) => {
        log.debug('Putting Object in Metdata');
        client.putObject(bucketName, objName, objVal, log, err => {
            if (err) {
                log.warn(`${implName}: putObjectMD: ${bucketName}: ${err}`);
                return cb(errorMap(err));
            }
            log.debug('Object successfully put in Metadata');
            return cb(err);
        });
    },

    getObjectMD: (bucketName, objName, log, cb) => {
        log.debug('Getting object from metadata');
        client.getObject(bucketName, objName, log, (err, data) => {
            if (err) {
                log.warn(`${implName}: getObjectMD: ${bucketName}: ${err}`);
                return cb(errorMap(err));
            }
            log.debug('Object retrieved from Metadata');
            return cb(err, data);
        });
    },

    deleteObjectMD: (bucketName, objName, log, cb) => {
        log.debug('Deleting object from metadata');
        client.deleteObject(bucketName, objName, log, err => {
            if (err) {
                log.warn(`${implName}: deleteObjectMD: ${bucketName}: ${err}`);
                return cb(errorMap(err));
            }
            log.debug('Object deleted from Metadata');
            return cb(err);
        });
    },

    listObject: (bucketName, prefix, marker, delimiter, maxKeys, log, cb) => {
        client
            .listObject(bucketName, { prefix, marker, maxKeys, delimiter, },
                    log, (err, data) => {
                        log.debug('Getting Object Listing from Metadata');
                        if (err) {
                            log.warn(`${implName}: listObject: ` +
                                `${bucketName}: ${err}`);
                            return cb(errorMap(err));
                        }
                        log.debug('Object listing retrieved from Metadata');
                        return cb(err, data);
                    });
    },

    switch: (newClient) => {
        client = newClient;
    },
};

export default metadata;
