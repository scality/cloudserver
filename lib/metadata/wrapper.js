import BucketClientInterface from './bucketclient/backend';
import BucketFileInterface from './bucketfile/backend';
import BucketInfo from './BucketInfo';
import inMemory from './in_memory/backend';
import config from '../Config';
import index from '../index/utils';
import indexd from '../index/bitmapd-utils'

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
            if (config.userMetaIndexing || config.systemMetaIndexing) {
                indexd.write(`0#${bucketName}||`, null);
            }
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
                log.warn('error from metadata', { implName, error: err });
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
                log.warn('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.debug('Deleted bucket from Metadata');
            return cb(err);
        });
    },

    putObjectMD: (bucketName, objName, objVal, log, cb) => {
        log.debug('putting object in metdata');
        client.putObject(bucketName, objName, objVal, log, err => {
            if (err) {
                log.warn('error from metadata', { implName, error: err });
                return cb(err);
            }
            log.debug('object successfully put in metadata');
            if (config.userMetaIndexing || config.systemMetaIndexing) {
                if (objName.indexOf('..|..') !== -1) {
                    return cb(err);
                }
                let msg = `1#${bucketName}#${objName}#${objVal['content-length']}#${objVal['content-type']}#${objVal['last-modified']}#${JSON.stringify(objVal.acl)}`;
                Object.keys(objVal).forEach(key => {
                    if (key.indexOf('x-amz-meta') !== -1 && key !== 'x-amz-meta-s3cmd-attrs') {
                        msg = msg + `#` + key;
                        msg = msg + `#` + objVal[key];
                    }
                });
                msg = msg + `||`;
                indexd.write(msg, null);
            }
            return cb(err);
        });
    },

    getBucketAndObjectMD: (bucketName, objName, log, cb) => {
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
        });
    },

    getObjectMD: (bucketName, objName, log, cb) => {
        log.debug('getting object from metadata');
        client.getObject(bucketName, objName, log, (err, data) => {
            if (err) {
                log.warn('error from metadata', { implName, err });
                return cb(err);
            }
            log.debug('object retrieved from metadata');
            return cb(err, data);
        });
    },

    deleteObjectMD: (bucketName, objName, log, cb) => {
        log.debug('deleting object from metadata');
        client.deleteObject(bucketName, objName, log, err => {
            if (err) {
                log.warn('error from metadata', { implName, err });
                return cb(err);
            }
            log.debug('object deleted from metadata');
            if (config.userMetaIndexing || config.systemMetaIndexing) {
                indexd.write(`3#${bucketName}#${objName}||`, null);
            }
            return cb(err);
        });
    },

    listObject: (bucketName, query, prefix, marker, delimiter, maxKeys,
    log, cb) => {
        if (query) {
            let msg = `2#${bucketName}#${prefix}#${marker}#${maxKeys}#${delimiter}`
            for (var i=0; i<query.length; i++) {
                msg = msg + '#' + query[i];
            }
            indexd.write(msg, cb);
        }
        else {
            client
                .listObject(bucketName, { prefix, marker, maxKeys, delimiter },
                        log, (err, data) => {
                            log.debug('getting object listing from metadata');
                            if (err) {
                                log.warn('error from metadata', { implName, err });
                                return cb(err);
                            }
                            log.debug('object listing retrieved from metadata');
                            return cb(err, data);
                        });
        }
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

    switch: (newClient) => {
        client = newClient;
    },
};

export default metadata;
