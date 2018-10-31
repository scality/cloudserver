const { errors, s3middleware } = require('arsenal');
const werelogs = require('werelogs');
const MD5Sum = s3middleware.MD5Sum;
const getMetaHeaders = s3middleware.userMetadata.getMetaHeaders;
const createLogger = require('../multipleBackendLogger');
const { logHelper } = require('./utils');
const { config } = require('../../Config');
const fs = require('fs');

class FsClient {
    constructor(config) {
        this.clientType = 'fs';
        this.type = 'FS';
        this._bucketName = config.bucketName;
        this._bucketMatch = config.bucketMatch;
        this._serverSideEncryption = config.serverSideEncryption;
        this._dataStoreName = config.dataStoreName;
        this._supportsVersioning = config.supportsVersioning;
        this._mountPath = config.mountPath;
        this._logger = new werelogs.Logger('FsClient');
    }

    setup(cb) {
        return cb();
    }

    _createFsKey(requestBucketName, requestObjectKey,
        bucketMatch) {
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

        if (size === 0) {
            const b64 = keyContext.metaHeaders['x-amz-meta-md5chksum'];
            let md5 = null;
            if (b64 !== null) {
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

        const filePath = this._mountPath + '/' + objectGetInfo.key;
        const readStreamOptions = {
            flags: 'r',
            encoding: null,
            fd: null,
            autoClose: false,
        };
        const rs = fs.createReadStream(filePath, readStreamOptions)
              .on('error', err => {
                  logHelper(log, 'error', 'Error reading file', err,
                            this._dataStoreName, this.clientType);
                  console.log('err', err);
              })
              .on('open', () => {
                  return callback(null, rs);
              });
    }

    delete(objectGetInfo, reqUids, callback) {
        const log = createLogger(reqUids);

        logHelper(log, 'error', 'Not implemented', errors.NotImplemented,
                  this._dataStoreName, this.clientType);
        return callback(errors.NotImplemented);
    }

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

module.exports = FsClient;
