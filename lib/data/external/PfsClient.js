const arsenal = require('arsenal');
const errors = arsenal.errors;
const createLogger = require('../multipleBackendLogger');
const { logHelper } = require('./utils');

class PfsClient {
    constructor(config) {
        const { host, port } = config.endpoint;

        this.clientType = 'pfs';
        this._bucketMatch = config.bucketMatch;
        this._dataStoreName = config.dataStoreName;
        this._restClient = new arsenal.network.rest.RESTClient({
            host,
            port,
            isPassthrough: true,
        });
    }

    setup(cb) {
        return cb();
    }

    _createFsKey(requestBucketName, requestObjectKey, bucketMatch) {
        if (bucketMatch) {
            return requestObjectKey;
        }
        return `${requestBucketName}/${requestObjectKey}`;
    }

    toObjectGetInfo(objectKey, bucketName) {
        return {
            key: this._createFsKey(bucketName, objectKey, this._bucketMatch),
            dataStoreName: this._dataStoreName,
        };
    }

    put(stream, size, keyContext, reqUids, callback) {
        const log = createLogger(reqUids);
        if (keyContext.metaHeaders['x-amz-meta-mdonly'] === 'true') {
            const b64 = keyContext.metaHeaders['x-amz-meta-md5chksum'];
            let md5 = null;
            if (b64) {
                md5 = new Buffer(b64, 'base64').toString('hex');
            }
            return callback(null, keyContext.objectKey, '',
                keyContext.metaHeaders['x-amz-meta-size'],
                md5
            );
        }
        logHelper(log, 'error', 'Not implemented', errors.NotImplemented,
            this._dataStoreName, this.clientType);
        return callback(errors.NotImplemented);
    }

    get(objectGetInfo, range, reqUids, callback) {
        const log = createLogger(reqUids);
        this._restClient.get(objectGetInfo.key, range, reqUids, (err, rs) => {
            if (err) {
                logHelper(log, 'error', 'err from data backend', err,
                    this._dataStoreName, this.clientType);
                return callback(err, null);
            }
            return callback(null, rs);
        });
    }

    delete(objectGetInfo, reqUids, callback) {
        const log = createLogger(reqUids);
        const key = typeof objectGetInfo === 'string' ? objectGetInfo :
            objectGetInfo.key;
        this._restClient.delete(key, reqUids, err => {
            if (err) {
                logHelper(log, 'error', 'err from data backend', err,
                this._dataStoreName, this.clientType);
                return callback(err);
            }
            return callback();
        });
    }

    // TODO: Implement a healthcheck
    healthcheck(location, callback) {
        const fsResp = {};
        return callback(null, fsResp);
    }

    createMPU(key, metaHeaders, bucketName, websiteRedirectHeader, contentType,
            cacheControl, contentDisposition, contentEncoding, log, callback) {
        logHelper(log, 'error', 'Not implemented', errors.NotImplemented,
                this._dataStoreName, this.clientType);
        return callback(errors.NotImplemented);
    }

    uploadPart(request, streamingV4Params, stream, size, key, uploadId,
            partNumber, bucketName, log, callback) {
        logHelper(log, 'error', 'Not implemented', errors.NotImplemented,
                this._dataStoreName, this.clientType);
        return callback(errors.NotImplemented);
    }

    listParts(key, uploadId, bucketName, partNumberMarker, maxParts, log,
            callback) {
        logHelper(log, 'error', 'Not implemented', errors.NotImplemented,
                this._dataStoreName, this.clientType);
        return callback(errors.NotImplemented);
    }

    completeMPU(jsonList, mdInfo, key, uploadId, bucketName, log, callback) {
        logHelper(log, 'error', 'Not implemented', errors.NotImplemented,
                this._dataStoreName, this.clientType);
        return callback(errors.NotImplemented);
    }

    abortMPU(key, uploadId, bucketName, log, callback) {
        logHelper(log, 'error', 'Not implemented', errors.NotImplemented,
                this._dataStoreName, this.clientType);
        return callback(errors.NotImplemented);
    }

    objectPutTagging(key, bucket, objectMD, log, callback) {
        logHelper(log, 'error', 'Not implemented', errors.NotImplemented,
                this._dataStoreName, this.clientType);
        return callback(errors.NotImplemented);
    }

    objectDeleteTagging(key, bucket, objectMD, log, callback) {
        logHelper(log, 'error', 'Not implemented', errors.NotImplemented,
                this._dataStoreName, this.clientType);
        return callback(errors.NotImplemented);
    }

    copyObject(request, destLocationConstraintName, sourceKey,
            sourceLocationConstraintName, storeMetadataParams, log, callback) {
        logHelper(log, 'error', 'Not implemented', errors.NotImplemented,
                this._dataStoreName, this.clientType);
        return callback(errors.NotImplemented);
    }

    uploadPartCopy(request, awsSourceKey, sourceLocationConstraintName,
            log, callback) {
        logHelper(log, 'error', 'Not implemented', errors.NotImplemented,
                this._dataStoreName, this.clientType);
        return callback(errors.NotImplemented);
    }
}

module.exports = PfsClient;

