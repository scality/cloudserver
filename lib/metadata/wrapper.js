const errors = require('arsenal').errors;

const BucketClientInterface = require('./bucketclient/backend');
const BucketFileInterface = require('./bucketfile/backend');
const BucketInfo = require('arsenal').models.BucketInfo;
const inMemory = require('./in_memory/backend');
const { config } = require('../Config');

let CdmiMetadata;
try {
    CdmiMetadata = require('cdmiclient').CdmiMetadata;
} catch (err) {
    CdmiMetadata = null;
}

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
} else if (config.backends.metadata === 'cdmi') {
    if (!CdmiMetadata) {
        throw new Error('Unauthorized backend');
    }

    client = new CdmiMetadata({
        path: config.cdmi.path,
        host: config.cdmi.host,
        port: config.cdmi.port,
    });
    implName = 'cdmi';
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
        log.debug('putting object in metadata');
        const value = typeof objVal.getValue === 'function' ?
            objVal.getValue() : objVal;
        client.putObject(bucketName, objName, value, params, log,
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
        const metadataUtils = require('./metadataUtils');
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
                // eslint-disable-next-line
                data.Versions = metadataUtils.parseListEntries(data.Versions);
                if (data.Versions instanceof Error) {
                    log.error('error parsing metadata listing', {
                        error: data.Versions,
                        listingType: listingParams.listingType,
                        method: 'listObject',
                    });
                    return cb(errors.InternalError);
                }
                return cb(null, data);
            }
            // eslint-disable-next-line
            data.Contents = metadataUtils.parseListEntries(data.Contents);
            if (data.Contents instanceof Error) {
                log.error('error parsing metadata listing', {
                    error: data.Contents,
                    listingType: listingParams.listingType,
                    method: 'listObject',
                });
                return cb(errors.InternalError);
            }
            return cb(null, data);
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

    getUUID: (log, cb) => {
        if (!client.getUUID) {
            log.debug('returning empty uuid as fallback', { implName });
            return cb(null, '');
        }
        return client.getUUID(log, cb);
    },

    getDiskUsage: (log, cb) => {
        if (!client.getDiskUsage) {
            log.debug('returning empty disk usage as fallback', { implName });
            return cb(null, {});
        }
        return client.getDiskUsage(cb);
    },

    countItems: (log, cb) => {
        if (!client.countItems) {
            log.debug('returning zero item counts as fallback', { implName });
            return cb(null, {
                buckets: 0,
                objects: 0,
                versions: 0,
            });
        }
        return client.countItems(log, cb);
    },
};

module.exports = metadata;
