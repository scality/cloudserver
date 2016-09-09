import BucketClientInterface from './bucketclient/backend';
import BucketFileInterface from './bucketfile/backend';
import BucketInfo from './BucketInfo';
import inMemory from './in_memory/backend';
import config from '../Config';

let client;
let implName;

if (config.backends.metadata === 'mem') {
    client = inMemory;
    implName = 'memorybucket';
} else if (config.backends.metadata === 'file') {
    client = new BucketFileInterface();
    implName = 'bucketfile';
} else if (config.backends.metadata === 'scality') {
    client = new BucketClientInterface();
    implName = 'bucketclient';
}

const metadata = {
    createBucket: (bucketName, bucketMD, log, cb) => {
        log.debug('creating bucket in metadata');
        client.createBucket(bucketName, bucketMD, log, err => {
            if (err) {
                log.warn('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.trace('bucket created in metadata');
            return cb(err);
        });
    },

    updateBucket: (bucketName, bucketMD, log, cb) => {
        log.debug('updating bucket in metadata');
        client.putBucketAttributes(bucketName, bucketMD, log, err => {
            if (err) {
                log.warn('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.trace('bucket updated in metadata');
            return cb(err);
        });
    },

    getBucket: (bucketName, log, cb) => {
        log.debug('getting bucket from metadata');
        client.getBucketAttributes(bucketName, log, (err, data) => {
            if (err) {
                log.debug('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.trace('bucket retrieved from metadata');
            return cb(err, BucketInfo.fromObj(data));
        });
    },

    deleteBucket: (bucketName, log, cb) => {
        log.debug('deleting bucket from metadata');
        client.deleteBucket(bucketName, log, err => {
            if (err) {
                log.debug('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.debug('Deleted bucket from Metadata');
            return cb(err);
        });
    },

    putObjectMD: (bucketName, objName, objVal, log, cb, params) => {
        log.debug('putting object in metdata');
        client.putObject(bucketName, objName, objVal, log, err => {
            if (err) {
                log.warn('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.debug('object successfully put in metadata');
            return cb(err);
        }, params);
    },

    getBucketAndObjectMD: (bucketName, objName, log, cb, params) => {
        log.debug('getting bucket and object from metadata',
                  { database: bucketName, object: objName });
        client.getBucketAndObject(bucketName, objName, log, (err, data) => {
            if (err) {
                log.debug('error from metadata', { implName, err });
                return cb(err);
            }
            log.debug('bucket and object retrieved from metadata',
                      { database: bucketName, object: objName });
            return cb(err, data);
        }, params);
    },

    getObjectMD: (bucketName, objName, log, cb, params) => {
        log.debug('getting object from metadata');
        client.getObject(bucketName, objName, log, (err, data) => {
            if (err) {
                log.warn('error from metadata', { implName, err });
                return cb(err);
            }
            log.debug('object retrieved from metadata');
            return cb(err, data);
        }, params);
    },

    deleteObjectMD: (bucketName, objName, log, cb, params) => {
        log.debug('deleting object from metadata');
        client.deleteObject(bucketName, objName, log, err => {
            if (err) {
                log.warn('error from metadata', { implName, err });
                return cb(err);
            }
            log.debug('object deleted from metadata');
            return cb(err);
        }, params);
    },

    listObject: (bucketName, params, log, cb) => {
        client.listObject(bucketName, params, log, (err, data) => {
            log.debug('getting object listing from metadata');
            if (err) {
                log.warn('error from metadata', { implName, err });
                return cb(err);
            }
            log.debug('object listing retrieved from metadata');
            return cb(err, data);
        });
    },

    listMultipartUploads: (bucketName, listingParams, log, cb) => {
        client.listMultipartUploads(bucketName, listingParams, log,
            (err, data) => {
                log.debug('getting mpu listing from metadata');
                if (err) {
                    log.warn('error from metadata', { implName, err });
                    return cb(err);
                }
                log.debug('mpu listing retrieved from metadata');
                return cb(err, data);
            });
    },

    switch: newClient => {
        client = newClient;
    },
};

export default metadata;
