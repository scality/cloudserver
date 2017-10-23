const { errors, s3middleware } = require('arsenal');
const azure = require('azure-storage');
const createLogger = require('../multipleBackendLogger');
const { logHelper } = require('./utils');
const { config } = require('../../Config');
const { validateAndFilterMpuParts } =
    require('../../api/apiUtils/object/processMpuParts');
const constants = require('../../../constants');
const metadata = require('../../metadata/wrapper');
const azureMpuUtils = s3middleware.azureHelper.mpuUtils;

class AzureClient {
    constructor(config) {
        this._azureBlobEndpoint = config.azureBlobEndpoint;
        this._azureBlobSAS = config.azureBlobSAS;
        this._azureContainerName = config.azureContainerName;
        this._client = azure.createBlobServiceWithSas(
          this._azureBlobEndpoint, this._azureBlobSAS);
        this._dataStoreName = config.dataStoreName;
        this._bucketMatch = config.bucketMatch;
    }

    _errorWrapper(that, s3Method, azureMethod, args, log, cb) {
        if (log) {
            log.info(`calling azure ${azureMethod}`);
        }
        try {
            that._client[azureMethod].apply(that._client, args);
        } catch (err) {
            const error = errors.InternalError;
            if (log) {
                log.error('error thrown by Azure Storage Client Library',
                    { error: err.message, stack: err.stack, s3Method,
                    azureMethod, dataStoreName: this._dataStoreName });
            }
            cb(error.customizeDescription('Error from Azure ' +
                `method: ${azureMethod} on ${s3Method} S3 call: ` +
                `${err.message}`));
        }
    }

    _createAzureKey(requestBucketName, requestObjectKey,
        bucketMatch) {
        if (bucketMatch) {
            return requestObjectKey;
        }
        return `${requestBucketName}/${requestObjectKey}`;
    }

    _translateMetaHeaders(metaHeaders, tags) {
        const translatedMetaHeaders = {};
        if (tags) {
            // tags are passed as string of format 'key1=value1&key2=value2'
            const tagObj = {};
            const tagArr = tags.split('&');
            tagArr.forEach(keypair => {
                const equalIndex = keypair.indexOf('=');
                const key = keypair.substring(0, equalIndex);
                tagObj[key] = keypair.substring(equalIndex + 1);
            });
            translatedMetaHeaders.tags = JSON.stringify(tagObj);
        }
        Object.keys(metaHeaders).forEach(headerName => {
            const translated = headerName.replace(/-/g, '_');
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

    // Before putting or deleting object on Azure, check if MPU exists with
    // same key name. If it does, do not allow put or delete because Azure
    // will delete all blocks with same key name
    protectAzureBlocks(bucketName, objectKey, dataStoreName, log, cb) {
        const mpuBucketName = `${constants.mpuBucketPrefix}${bucketName}`;
        const splitter = constants.splitter;
        const listingParams = {
            prefix: `overview${splitter}${objectKey}`,
            listingType: 'MPU',
            splitter,
            maxKeys: 1,
        };

        return metadata.listMultipartUploads(mpuBucketName, listingParams,
        log, (err, mpuList) => {
            if (err && !err.NoSuchBucket) {
                log.error('Error listing MPUs for Azure delete',
                    { error: err, dataStoreName });
                return cb(errors.InternalError);
            }
            if (mpuList && mpuList.Uploads && mpuList.Uploads.length > 0) {
                const error = errors.MPUinProgress;
                log.error('Error: cannot put/delete object to Azure with ' +
                    'same key name as ongoing MPU on Azure',
                    { error, dataStoreName });
                return cb(error);
            }
            // If listMultipartUploads returns a NoSuchBucket error or the
            // mpu list is empty, there are no conflicting MPUs, so continue
            return cb();
        });
    }

    put(stream, size, keyContext, reqUids, callback) {
        const log = createLogger(reqUids);
        // before blob is put, make sure there is no ongoing MPU with same key
        this.protectAzureBlocks(keyContext.bucketName,
        keyContext.objectKey, this._dataStoreName, log, err => {
            // if error returned, there is ongoing MPU, so do not put
            if (err) {
                return callback(err.customizeDescription(
                    `Error putting object to Azure: ${err.message}`));
            }
            const azureKey = this._createAzureKey(keyContext.bucketName,
                keyContext.objectKey, this._bucketMatch);
            const options = { metadata:
                this._translateMetaHeaders(keyContext.metaHeaders,
                keyContext.tagging) };
            if (size === 0) {
                return this._errorWrapper(this, 'put',
                    'createBlockBlobFromText', [this._azureContainerName,
                    azureKey, '', options, err => {
                        if (err) {
                            logHelper(log, 'error', 'err from Azure PUT data ' +
                                'backend', err, this._dataStoreName);
                            return callback(errors.InternalError
                                .customizeDescription('Error returned from ' +
                                `Azure: ${err.message}`));
                        }
                        return callback(null, azureKey);
                    }], log, callback);
            }
            return this._errorWrapper(this, 'put', 'createBlockBlobFromStream',
                [this._azureContainerName, azureKey, stream, size, options,
                err => {
                    if (err) {
                        logHelper(log, 'error', 'err from Azure PUT data ' +
                            'backend', err, this._dataStoreName);
                        return callback(errors.InternalError
                            .customizeDescription('Error returned from ' +
                            `Azure: ${err.message}`));
                    }
                    return callback(null, azureKey);
                }], log, callback);
        });
    }

    head(objectGetInfo, reqUids, callback) {
        const log = createLogger(reqUids);
        const { key, azureStreamingOptions } = objectGetInfo;
        return this._errorWrapper(this, 'head', 'getBlobProperties',
            [this._azureContainerName, key, azureStreamingOptions,
            err => {
                if (err) {
                    logHelper(log, 'error', 'err from Azure HEAD data backend',
                        err, this._dataStoreName);
                    if (err.code === 'NotFound') {
                        const error = errors.InternalError.customizeDescription(
                            'Unexpected error from Azure: "NotFound". Data ' +
                            'on Azure may have been altered outside of ' +
                            'CloudServer.');
                        return callback(error);
                    }
                    return callback(errors.InternalError
                        .customizeDescription('Error returned from ' +
                        `Azure: ${err.message}`));
                }
                return callback();
            }], log, callback);
    }

    get(objectGetInfo, range, reqUids, callback) {
        const log = createLogger(reqUids);
        // for backwards compatibility
        const { key, response, azureStreamingOptions } = objectGetInfo;
        this._errorWrapper(this, 'get', 'getBlobToStream',
            [this._azureContainerName, key, response, azureStreamingOptions,
            err => {
                if (err) {
                    logHelper(log, 'error', 'err from Azure GET data backend',
                        err, this._dataStoreName);
                    return callback(errors.InternalError);
                }
                return callback(null, response);
            }], log, callback);
    }

    delete(objectGetInfo, reqUids, callback) {
        const log = createLogger(reqUids);
        // for backwards compatibility
        const key = typeof objectGetInfo === 'string' ? objectGetInfo :
            objectGetInfo.key;
        return this._errorWrapper(this, 'delete', 'deleteBlobIfExists',
            [this._azureContainerName, key,
            err => {
                if (err) {
                    const log = createLogger(reqUids);
                    logHelper(log, 'error', 'error deleting object from ' +
                        'Azure datastore', err, this._dataStoreName);
                    return callback(errors.InternalError
                        .customizeDescription('Error returned from ' +
                        `Azure: ${err.message}`));
                }
                return callback();
            }], log, callback);
    }

    checkAzureHealth(location, callback) {
        const azureResp = {};
        this._errorWrapper(this, 'checkAzureHealth', 'doesContainerExist',
            [this._azureContainerName,
            err => {
                /* eslint-disable no-param-reassign */
                if (err) {
                    azureResp[location] = { error: err.message };
                    return callback(null, azureResp);
                }
                azureResp[location] = {
                    message: 'Congrats! You own the azure container',
                };
                return callback(null, azureResp);
            }], null, callback);
    }

    uploadPart(request, streamingV4Params, partStream, size, key, uploadId,
    partNumber, bucket, log, callback) {
        const azureKey = this._createAzureKey(bucket, key, this._bucketMatch);
        const params = { bucketName: this._azureContainerName,
            partNumber, size, objectKey: azureKey, uploadId };
        const stream = request || partStream;

        if (request && request.headers && request.headers['content-md5']) {
            params.contentMD5 = request.headers['content-md5'];
        }
        const dataRetrievalInfo = {
            key: azureKey,
            partNumber,
            dataStoreName: this._dataStoreName,
            dataStoreType: 'azure',
        };

        if (size === 0) {
            log.debug('0-byte part does not store data',
                { method: 'uploadPart' });
            dataRetrievalInfo.dataStoreETag = azureMpuUtils.zeroByteETag;
            dataRetrievalInfo.numberSubParts = 0;
            return callback(null, dataRetrievalInfo);
        }
        if (size <= azureMpuUtils.maxSubPartSize) {
            return azureMpuUtils.putSinglePart(this, this._errorWrapper, stream,
            params, this._dataStoreName, log,
            (err, dataStoreETag, numberSubParts) => {
                if (err) {
                    return callback(err);
                }
                dataRetrievalInfo.dataStoreETag = dataStoreETag;
                dataRetrievalInfo.numberSubParts = numberSubParts;
                return callback(null, dataRetrievalInfo);
            });
        }
        return azureMpuUtils.putSubParts(this, this._errorWrapper, stream,
        params, this._dataStoreName, log,
        (err, dataStoreETag, numberSubParts) => {
            if (err) {
                return callback(err);
            }
            dataRetrievalInfo.dataStoreETag = dataStoreETag;
            dataRetrievalInfo.numberSubParts = numberSubParts;
            return callback(null, dataRetrievalInfo);
        });
    }

    completeMPU(jsonList, mdInfo, key, uploadId, bucket, log, callback) {
        const azureKey = this._createAzureKey(bucket, key, this._bucketMatch);
        const commitList = {
            UncommittedBlocks: [],
        };

        const { storedParts, mpuOverviewKey, splitter } = mdInfo;
        const filteredPartsObj = validateAndFilterMpuParts(storedParts,
            jsonList, mpuOverviewKey, splitter, log);
        filteredPartsObj.partList.forEach(part => {
            // part.locations is always array of 1, which contains data info
            const subPartIds =
                azureMpuUtils.getSubPartIds(part.locations[0], uploadId);
            commitList.UncommittedBlocks.push(...subPartIds);
        });

        this._errorWrapper(this, 'completeMPU', 'commitBlocks',
            [this._azureContainerName, azureKey, commitList, null,
            err => {
                if (err) {
                    logHelper(log, 'error', 'err completing MPU on Azure ' +
                        'datastore', err, this._dataStoreName);
                    return callback(errors.InternalError
                        .customizeDescription('Error returned from ' +
                        `Azure: ${err.message}`));
                }
                const completeObjData = {
                    key: azureKey,
                    filteredPartsObj,
                };
                return callback(null, completeObjData);
            }], log, callback);
    }

    objectPutTagging(key, bucket, objectMD, log, callback) {
        const azureKey = this._createAzureKey(bucket, key, this._bucketMatch);
        const azureMD = this._getMetaHeaders(objectMD);
        azureMD.tags = JSON.stringify(objectMD.tags);
        this._errorWrapper(this, 'objectPutTagging', 'setBlobMetadata',
            [this._azureContainerName, azureKey, azureMD,
            err => {
                if (err) {
                    logHelper(log, 'error', 'err putting object tags to ' +
                        'Azure backend', err, this._dataStoreName);
                    return callback(errors.InternalError);
                }
                return callback();
            }], log, callback);
    }

    objectDeleteTagging(key, bucket, objectMD, log, callback) {
        const azureKey = this._createAzureKey(bucket, key, this._bucketMatch);
        const azureMD = this._getMetaHeaders(objectMD);
        this._errorWrapper(this, 'objectDeleteTagging', 'setBlobMetadata',
            [this._azureContainerName, azureKey, azureMD,
            err => {
                if (err) {
                    logHelper(log, 'error', 'err putting object tags to ' +
                        'Azure backend', err, this._dataStoreName);
                    return callback(errors.InternalError);
                }
                return callback();
            }], log, callback);
    }

    copyObject(request, sourceKey, sourceLocationConstraintName, log,
    callback) {
        const destContainerName = request.bucketName;
        const destObjectKey = request.objectKey;

        const destAzureKey = this._createAzureKey(destContainerName,
          destObjectKey, this._bucketMatch);

        const sourceContainerName =
        config.locationConstraints[sourceLocationConstraintName]
        .details.azureContainerName;

        this._errorWrapper(this, 'copyObject', 'startCopyBlob',
            [`${this._azureBlobEndpoint}/${sourceContainerName}/${sourceKey}` +
            `?${this._azureBlobSAS}`, this._azureContainerName, destAzureKey,
            (err, res) => {
                if (err) {
                    if (err.code === 'CannotVerifyCopySource') {
                        logHelper(log, 'error', 'Unable to access ' +
                        `${sourceContainerName} Azure Container`, err,
                        this._dataStoreName);
                        return callback(errors.AccessDenied
                        .customizeDescription('Error: Unable to access ' +
                        `${sourceContainerName} Azure Container`)
                        );
                    }
                    logHelper(log, 'error', 'error from data backend on ' +
                    'copyObject', err, this._dataStoreName);
                    return callback(errors.InternalError
                    .customizeDescription('Error returned from ' +
                    `AWS: ${err.message}`)
                    );
                }
                if (res.copy.status === 'pending') {
                    logHelper(log, 'error', 'Azure copy status is pending',
                    err, this._dataStoreName);
                    const copyId = res.copy.id;
                    this._client.abortCopyBlob(this._azureContainerName,
                    destAzureKey, copyId, err => {
                        if (err) {
                            logHelper(log, 'error', 'error from data backend ' +
                            'on abortCopyBlob', err, this._dataStoreName);
                            return callback(errors.InternalError
                            .customizeDescription('Error returned from ' +
                            `AWS on abortCopyBlob: ${err.message}`)
                            );
                        }
                        return callback(errors.InvalidObjectState
                        .customizeDescription('Error: Azure copy status was ' +
                        'pending. It has been aborted successfully')
                        );
                    });
                }
                return callback(null, destAzureKey);
            }], log, callback);
    }
}

module.exports = AzureClient;
