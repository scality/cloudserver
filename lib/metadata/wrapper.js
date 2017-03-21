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
                log.debug('error from metadata', { implName, error: err });
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
                log.debug('error from metadata', { implName, error: err });
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

    putObjectMD: (bucketName, objName, objVal, params, log, cb) => {
        log.debug('putting object in metdata');
        client.putObject(bucketName, objName, objVal, params, log,
        (err, data) => {
            if (err) {
                log.debug('error from metadata', { implName, error: err });
                return cb(err);
            }
            if (data) {
                log.debug('object version successfully put in metadata',
                { version: data });
            } else {
                log.debug('object successfully put in metadata');
            }
            return cb(err, data);
        });
    },

    getBucketAndObjectMD: (bucketName, objName, params, log, cb) => {
        log.debug('getting bucket and object from metadata',
                  { database: bucketName, object: objName });
        client.getBucketAndObject(bucketName, objName, params, log,
            (err, data) => {
                if (err) {
                    log.debug('error from metadata', { implName, err });
                    return cb(err);
                }
                log.debug('bucket and object retrieved from metadata',
                { database: bucketName, object: objName });
                return cb(err, data);
            });
    },

    getObjectMD: (bucketName, objName, params, log, cb) => {
        log.debug('getting object from metadata');
        client.getObject(bucketName, objName, params, log, (err, data) => {
            if (err) {
                log.debug('error from metadata', { implName, err });
                return cb(err);
            }
            log.debug('object retrieved from metadata');
            return cb(err, data);
        });
    },

    deleteObjectMD: (bucketName, objName, params, log, cb) => {
        log.debug('deleting object from metadata');
        client.deleteObject(bucketName, objName, params, log, err => {
            if (err) {
                log.debug('error from metadata', { implName, err });
                return cb(err);
            }
            log.debug('object deleted from metadata');
            return cb(err);
        });
    },

    listObject: (bucketName, listingParams, log, cb) => {
        if (listingParams.listingType === undefined) {
            // eslint-disable-next-line
            listingParams.listingType = 'Delimiter';
        }
        client.listObject(bucketName, listingParams, log, (err, data) => {
            log.debug('getting object listing from metadata');
            if (err) {
                log.debug('error from metadata', { implName, err });
                return cb(err);
            }
            log.debug('object listing retrieved from metadata');
            if (listingParams.listingType === 'DelimiterVersions') {
                return cb(err, data);
            }
            // eslint-disable-next-line
            data.Contents = data.Contents.map(entry => {
                const tmp = JSON.parse(entry.value);
                return {
                    key: entry.key,
                    value: {
                        Size: tmp['content-length'],
                        ETag: tmp['content-md5'],
                        VersionId: tmp.versionId,
                        IsNull: tmp.isNull,
                        IsDeleteMarker: tmp.isDeleteMarker,
                        LastModified: tmp['last-modified'],
                        Owner: {
                            DisplayName: tmp['owner-display-name'],
                            ID: tmp['owner-id'],
                        },
                        StorageClass: tmp['x-amz-storage-class'],
                        Initiated: tmp.initiated,
                        Initiator: tmp.initiator,
                        EventualStorageBucket: tmp.eventualStorageBucket,
                        partLocations: tmp.partLocations,
                        creationDate: tmp.creationDate,
                    },
                };
            });
            return cb(err, data);
        });
    },

    listMultipartUploads: (bucketName, listingParams, log, cb) => {
        client.listMultipartUploads(bucketName, listingParams, log,
            (err, data) => {
                log.debug('getting mpu listing from metadata');
                if (err) {
                    log.debug('error from metadata', { implName, err });
                    return cb(err);
                }
                log.debug('mpu listing retrieved from metadata');
                return cb(err, data);
            });
    },

    switch: newClient => {
        client = newClient;
    },

    checkHealth: (log, cb) => {
        if (!client.checkHealth) {
            const defResp = {};
            defResp[implName] = { code: 200, message: 'OK' };
            return cb(null, defResp);
        }
        return client.checkHealth(implName, log, cb);
    },
};

export default metadata;
