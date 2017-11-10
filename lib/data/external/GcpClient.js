const { errors } = require('arsenal');
const GCP = require('google-cloud');
const createLogger = require('../multipleBackendLogger');
const logHelper = require('./utils').logHelper;

class GcpClient {
    constructor(config) {
        this._gcpEndpoint = config.gcpEndpoint;
        this._gcpParams = config.gcpParams;
        this._gcpBucketName = config.gcpBucketName;
        this._bucketMatch = config.bucketMatch;
        this._dataStoreName = config.dataStoreName;
        this._client = GCP.storage(this._gcpParams);
    }

    _createGcpKey(requestBucketName, requestObjectKey, bucketMatch) {
        if (bucketMatch) {
            return requestObjectKey;
        }
        return `${requestBucketName}/${requestObjectKey}`;
    }

    _translateMetaHeaders(metaHeaders, tags) {
        const translatedMetaHeaders = {};
        if (tags) {
            const tagObj = {};
            const tagArr = tags.split('&');
            tagArr.forEach(keypair => {
                const equalIndex = keypair.indexOf('=');
                const key = keypair.substring(0, equalIndex);
                tagObj[key] = keypair.substring(equalIndex + 1);
            });
            Object.keys(tagObj).forEach(tagName => {
                translatedMetaHeaders[tagName] = tagObj[tagName];
            });
        }
        Object.keys(metaHeaders).forEach(headerName => {
            const translated = headerName.replace('x-amz-meta-', '');
            translatedMetaHeaders[translated] = metaHeaders[headerName];
        });
        return translatedMetaHeaders;
    }

    _getMetaHeaders(objectMD) {
        const metaHeaders = {};
        Object.keys(objectMD).forEach(mdKey => {
            const isMetaHeader = mdKey.startsWith('x-amz-meta-');
            if (isMetaHeader) {
                metaHeaders[mdKey] = objectMD[mdKey];
            }
        });
        return this._translateMetaHeaders(metaHeaders);
    }

    _setMetaHeaders(metaHeaders) {
        const translatedMetaHeaders = {};
        Object.keys(metaHeaders).forEach(headerName => {
            if (!metaHeaders[headerName] &&
                typeof(metaHeaders[headerName]) === 'string') {
                translatedMetaHeaders[headerName] = null;
            } else {
                translatedMetaHeaders[headerName] = metaHeaders[headerName];
            }
        });
        return translatedMetaHeaders;
    }

    put(stream, size, keyContext, reqUids, callback) {
        const gcpKey = this._createGcpKey(keyContext.bucketName,
            keyContext.objectKey, this._bucketMatch);
        const bucket = this._client.bucket(this._gcpBucketName);
        const file = bucket.file(gcpKey);
        const options = {
            metadata: {
                // metadata: keyContext.metaHeaders
                metadata: this._translateMetaHeaders(keyContext.metaHeaders,
                    keyContext.tagging),
            },
        };
        stream.pipe(file.createWriteStream(options))
        .on('error', err => {
            const log = createLogger(reqUids);
            logHelper(log, 'error', 'err from GCP PUT data backend',
                err, this._dataStoreName);
            return callback(errors.InternalError
                .customizeDescription('Error returned from ' +
                `GCP: ${err.message}`)
            );
        }).on('finish', () => {
            callback(null, gcpKey);
        });
    }

    get(objectGetInfo, range, reqUids, callback) {
        const key = typeof(objectGetInfo) === 'string' ? objectGetInfo :
            objectGetInfo.key;
        // const response = objectGetInfo.response;
        const bucket = this._client.bucket(this._gcpBucketName);
        const file = bucket.file(key);
        const stream = file.createReadStream().on('error', err => {
            const log = createLogger(reqUids);
            logHelper(log, 'error', 'err from GCP GET data backend',
                err, this._dataStoreName);
            return callback(errors.InternalError);
        });
        return callback(null, stream);
    }

    delete(objectGetInfo, reqUids, callback) {
        const key = typeof(objectGetInfo) === 'string' ? objectGetInfo :
            objectGetInfo.key;
        const bucket = this._client.bucket(this._gcpBucketName);
        const file = bucket.file(key);
        return file.delete(err => {
            if (err) {
                const log = createLogger(reqUids);
                logHelper(log, 'error', 'error deleting object from ' +
                'GCP datastore', err, this._dataStoreName);
                return callback(errors.InternalError
                    .customizeDescription('Error returned from ' +
                    `GCP: ${err.message}`)
                );
            }
            return callback();
        });
    }

    checkGcpHealth(location, callback) {
        const gcpResp = {};
        const bucket = this._client.bucket(this._gcpBucketName);
        this._client.exist(bucket, err => {
            if (err) {
                gcpResp[location] = {
                    error: err.message,
                };
                return callback(null, gcpResp);
            }
            gcpResp[location] = {
                message: 'Congrats! You own the bucket',
            };
            return callback(null, gcpResp);
        });
    }

    objectPutTagging(key, bucket, objectMD, log, callback) {
        const gcpKey = this._createGcpKey(bucket, key, this._bucketMatch);
        const gcpBucket = this._client.bucket(this._gcpBucketName);
        const gcpFile = gcpBucket.file(gcpKey);
        const metaHeaders = this._getMetaHeaders(objectMD);
        const gcpMD = {
            metadata: this._setMetaHeaders(metaHeaders),
        };
        gcpFile.setMetadata(gcpMD, err => {
            if (err) {
                logHelper(log, 'error', 'error from data backend on ' +
                'putObjectTagging', err, this._dataStoreName);
                return callback(errors.InternalError
                    .customizeDescription('Error returend from ' +
                    `GCP: ${err.message}`)
                );
            }
            return callback();
        });
    }

    objectDeleteTagging(key, bucket, objectMD, log, callback) {
        const gcpKey = this._createGcpKey(bucket, key, this._bucketMatch);
        const gcpBucket = this._client.bucket(this._gcpBucketName);
        const gcpFile = gcpBucket.file(gcpKey);
        const metaHeaders = this._getMetaHeaders(objectMD);
        const gcpMD = {
            metadata: this._setMetaHeaders(metaHeaders),
        };
        gcpFile.setMetadata(gcpMD, err => {
            if (err) {
                logHelper(log, 'error', 'error from data backend on ' +
                'deleteObjectTagging', err, this._dataStoreName);
                return callback(errors.InternalError
                    .customizeDescription('Error returend from ' +
                    `GCP: ${err.message}`)
                );
            }
            return callback();
        });
    }
}

module.exports = GcpClient;
