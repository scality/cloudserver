import BucketClientInterface from './bucketclient/backend';
import BucketFileInterface from './bucketfile/backend';
import BucketInfo from './BucketInfo';
import inMemory from './in_memory/backend';
import AntidoteInterface from './antidote/backend';
import config from '../Config';
import indexClient from '../indexClient/indexClient'
import async from 'async'

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
} else if (config.backends.metadata === 'antidote') {
    client = new AntidoteInterface();
    implName = 'antidote';
}

function getQueryResults(params, objName, callback) {
    let { bucketName, prefix, marker, maxKeys, delimiter, log, cb} = params;
    client.getObject(bucketName, objName, log, (err, data) => {
        if (err) {
            callback(err, null);
        }
        callback(null, {key: objName, value: {
            LastModified: data['last-modified'],
            ETag: data['content-md5'],
            StorageClass: data['x-amz-storage-class'],
            Owner: {
                ID: data['owner-id'],
                DisplayName: data['owner-display-name']
            },
            Size: data['content-length'],
            Initiated: undefined,
            Initiator: undefined,
            EventualStorageBucket: undefined,
            partLocations: undefined,
            creationDate: undefined
        }});
    });
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
                indexClient.putObjectMD(bucketName, objName, objVal);
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
                indexClient.deleteObjectMD(bucketName, objName);
            }
            return cb(err);
        });
    },

    listObject: (bucketName, query, prefix, marker, delimiter, maxKeys,
    log, cb) => {
        if (query) {
            indexClient.listObject(bucketName, query, prefix, marker, delimiter, maxKeys, cb);
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

    respondQueryGetMD: (result, params) => {
        async.map(result, getQueryResults.bind(null, params), function(err, res) {
            const response = {
                IsTruncated: false,
                NextMarker: params.marker,
                CommonPrefixes: [],
                MaxKeys: 10,
                Contents: res
            }
            return params.cb(err, response);
        });
    },

    respondQueryFilter: (result, params) => {
        let { bucketName, prefix, marker, maxKeys, delimiter, log, cb} = params;
        client.listObject(bucketName, { prefix, marker, maxKeys, delimiter },
            log, (err, data) => {
                if (err) {
                    return cb(err);
                }
                data.Contents = data.Contents.filter(elem => {
                    return result.indexOf(elem.key) !== -1;
                });
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

    switch: (newClient) => {
        client = newClient;
    },
};

export default metadata;
