const async = require('async');
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
        this._azureStorageEndpoint = config.azureStorageEndpoint;
        this._azureStorageCredentials = config.azureStorageCredentials;
        this._azureContainerName = config.azureContainerName;
        this._client = azure.createBlobService(
            this._azureStorageCredentials.storageAccountName,
            this._azureStorageCredentials.storageAccessKey,
            this._azureStorageEndpoint);
        this._dataStoreName = config.dataStoreName;
        this._bucketMatch = config.bucketMatch;
    }

    _errorWrapper(s3Method, azureMethod, args, log, cb) {
        if (log) {
            log.info(`calling azure ${azureMethod}`);
        }
        try {
            this._client[azureMethod].apply(this._client, args);
        } catch (err) {
            const error = errors.ServiceUnavailable;
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

    createSnapshot(key, options, log, cb) {
        this._errorWrapper('put', 'createBlobSnapshot',
            [this._azureContainerName, key, options, (err, snapshot) => {
                if (err) {
                    logHelper(log, 'error', 'could not get blob snapshot', err,
                        this._dataStoreName);
                    return cb(errors.ServiceUnavailable
                        .customizeDescription('error returned from ' +
                            `Azure: ${err.message}`));
                }
                return cb(null, key, snapshot);
            }], log, cb);
    }

    deleteBlob(key, options, log, cb) {
        return this._errorWrapper('delete', 'deleteBlobIfExists',
            [this._azureContainerName, key, options, err => {
                if (err) {
                    logHelper(log, 'error', 'error deleting object from ' +
                        'Azure datastore', err, this._dataStoreName);
                    return cb(errors.ServiceUnavailable
                        .customizeDescription('Error returned from ' +
                        `Azure: ${err.message}`));
                }
                return cb(null, key);
            }], log, cb);
    }

    handleVersion(keyContext, key, options, log, cb) {
        const { bucketName, isDeleteMarker } = keyContext;
        if (isDeleteMarker) {
            return cb();
        }
        return metadata.getBucket(bucketName, log, (err, bucket) => {
            if (err) {
                return cb(err);
            }
            if (bucket.isVersioningEnabled()) {
                return this.createSnapshot(key, options, log, cb);
            }
            return cb(null, key);
        });
    }

    isLastVersion(key, params, log, cb) {
        const bucketName = this._azureContainerName;
        return metadata.listObject(bucketName, params, log, (err, data) => {
            if (err) {
                return cb(err);
            }
            if (data.Versions.some(version => version.key === key)) {
                return cb(null, false);
            }
            if (data.IsTruncated) {
                const nextParams = Object.assign({}, params, {
                    keyMarker: data.NextKeyMarker,
                    versionIdMarker: data.NextVersionIdMarker,
                });
                return this.isLastVersion(key, nextParams, log, cb);
            }
            return cb(null, true);
        });
    }

    handleVersionDelete(snapshotId, key, log, cb) {
        const params = {
            listingType: 'DelimiterVersions',
            maxKeys: 1000,
            prefix: key,
        };
        return this.isLastVersion(key, params, log, (err, isLastVersion) => {
            if (err) {
                return cb(err);
            }
            if (isLastVersion) {
                // In addition to the snapshot, also delete the blob.
                return async.each([{ snapshotId }, {}], (options, next) =>
                    this.deleteBlob(key, options, log, next), cb);
            }
            return this.deleteBlob(key, { snapshotId }, log, cb);
        });
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
                return cb(errors.ServiceUnavailable);
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
                return this._errorWrapper('put', 'createBlockBlobFromText',
                    [this._azureContainerName, azureKey, '', options,
                    err => {
                        if (err) {
                            logHelper(log, 'error', 'err from Azure PUT data ' +
                                'backend', err, this._dataStoreName);
                            return callback(errors.ServiceUnavailable
                                .customizeDescription('Error returned from ' +
                                `Azure: ${err.message}`));
                        }
                        return this.handleVersion(keyContext, azureKey, options,
                            log, callback);
                    }], log, callback);
            }
            return this._errorWrapper('put', 'createBlockBlobFromStream',
                [this._azureContainerName, azureKey, stream, size, options,
                err => {
                    if (err) {
                        logHelper(log, 'error', 'err from Azure PUT data ' +
                            'backend', err, this._dataStoreName);
                        return callback(errors.ServiceUnavailable
                            .customizeDescription('Error returned from ' +
                            `Azure: ${err.message}`));
                    }
                    return this.handleVersion(keyContext, azureKey, options,
                        log, callback);
                }], log, callback);
        });
    }

    head(objectGetInfo, reqUids, callback) {
        const log = createLogger(reqUids);
        const { key, azureStreamingOptions, dataStoreVersionId } =
            objectGetInfo;
        azureStreamingOptions.snapshotId = dataStoreVersionId;
        return this._errorWrapper('head', 'getBlobProperties',
            [this._azureContainerName, key, azureStreamingOptions,
            err => {
                if (err) {
                    logHelper(log, 'error', 'err from Azure HEAD data backend',
                        err, this._dataStoreName);
                    if (err.code === 'NotFound') {
                        const error = errors.ServiceUnavailable
                        .customizeDescription(
                            'Unexpected error from Azure: "NotFound". Data ' +
                            'on Azure may have been altered outside of ' +
                            'CloudServer.');
                        return callback(error);
                    }
                    return callback(errors.ServiceUnavailable
                        .customizeDescription('Error returned from ' +
                        `Azure: ${err.message}`));
                }
                return callback();
            }], log, callback);
    }

    get(objectGetInfo, range, reqUids, callback) {
        const log = createLogger(reqUids);
        // for backwards compatibility
        const { key, response, azureStreamingOptions,
            dataStoreVersionId } = objectGetInfo;
        azureStreamingOptions.snapshotId = dataStoreVersionId;
        this._errorWrapper('get', 'getBlobToStream',
            [this._azureContainerName, key, response, azureStreamingOptions,
            err => {
                if (err) {
                    logHelper(log, 'error', 'err from Azure GET data backend',
                        err, this._dataStoreName);
                    return callback(errors.ServiceUnavailable);
                }
                return callback(null, response);
            }], log, callback);
    }

    delete(objectGetInfo, reqUids, callback) {
        const log = createLogger(reqUids);
        // for backwards compatibility
        const key = typeof objectGetInfo === 'string' ? objectGetInfo :
            objectGetInfo.key;
        if (!key) {
            return callback();
        }
        const snapshotId = typeof objectGetInfo === 'string' ? undefined :
            objectGetInfo.dataStoreVersionId;
        if (snapshotId) {
            return this.handleVersionDelete(snapshotId, key, log, callback);
        }
        return this.deleteBlob(key, {}, log, callback);
    }

    healthcheck(location, callback, flightCheckOnStartUp) {
        const azureResp = {};
        const healthCheckAction = flightCheckOnStartUp ?
            'createContainerIfNotExists' : 'doesContainerExist';
        this._errorWrapper('checkAzureHealth', healthCheckAction,
            [this._azureContainerName, err => {
                /* eslint-disable no-param-reassign */
                if (err) {
                    azureResp[location] = { error: err.message,
                        external: true };
                    return callback(null, azureResp);
                }
                azureResp[location] = {
                    message:
                    'Congrats! You can access the Azure storage account',
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
            numberSubParts: azureMpuUtils.getSubPartInfo(size)
                .expectedNumberSubParts,
        };

        if (size === 0) {
            log.debug('0-byte part does not store data',
                { method: 'uploadPart' });
            dataRetrievalInfo.dataStoreETag = azureMpuUtils.zeroByteETag;
            dataRetrievalInfo.numberSubParts = 0;
            return callback(null, dataRetrievalInfo);
        }
        if (size <= azureMpuUtils.maxSubPartSize) {
            const errorWrapperFn = this._errorWrapper.bind(this);
            return azureMpuUtils.putSinglePart(errorWrapperFn,
            stream, params, this._dataStoreName, log, (err, dataStoreETag) => {
                if (err) {
                    return callback(err);
                }
                dataRetrievalInfo.dataStoreETag = dataStoreETag;
                return callback(null, dataRetrievalInfo);
            });
        }
        const errorWrapperFn = this._errorWrapper.bind(this);
        return azureMpuUtils.putSubParts(errorWrapperFn, stream,
        params, this._dataStoreName, log, (err, dataStoreETag) => {
            if (err) {
                return callback(err);
            }
            dataRetrievalInfo.dataStoreETag = dataStoreETag;
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

        this._errorWrapper('completeMPU', 'commitBlocks',
            [this._azureContainerName, azureKey, commitList, null,
            err => {
                if (err) {
                    logHelper(log, 'error', 'err completing MPU on Azure ' +
                        'datastore', err, this._dataStoreName);
                    return callback(errors.ServiceUnavailable
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
        this._errorWrapper('objectPutTagging', 'setBlobMetadata',
            [this._azureContainerName, azureKey, azureMD,
            err => {
                if (err) {
                    logHelper(log, 'error', 'err putting object tags to ' +
                        'Azure backend', err, this._dataStoreName);
                    return callback(errors.ServiceUnavailable);
                }
                return callback();
            }], log, callback);
    }

    objectDeleteTagging(key, bucket, objectMD, log, callback) {
        const azureKey = this._createAzureKey(bucket, key, this._bucketMatch);
        const azureMD = this._getMetaHeaders(objectMD);
        this._errorWrapper('objectDeleteTagging', 'setBlobMetadata',
            [this._azureContainerName, azureKey, azureMD,
            err => {
                if (err) {
                    logHelper(log, 'error', 'err putting object tags to ' +
                        'Azure backend', err, this._dataStoreName);
                    return callback(errors.ServiceUnavailable);
                }
                return callback();
            }], log, callback);
    }

    copyObject(request, destLocationConstraintName, sourceKey,
    sourceLocationConstraintName, log, callback) {
        const destContainerName = request.bucketName;
        const destObjectKey = request.objectKey;

        const destAzureKey = this._createAzureKey(destContainerName,
          destObjectKey, this._bucketMatch);

        const sourceContainerName =
        config.locationConstraints[sourceLocationConstraintName]
        .details.azureContainerName;

        this._errorWrapper('copyObject', 'startCopyBlob',
            [`${this._azureStorageEndpoint}` +
                `${sourceContainerName}/${sourceKey}`,
                this._azureContainerName, destAzureKey,
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
                    return callback(errors.ServiceUnavailable
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
                            return callback(errors.ServiceUnavailable
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
