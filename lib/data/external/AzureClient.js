const { errors, s3middleware } = require('arsenal');
const azure = require('azure-storage');
const createLogger = require('../multipleBackendLogger');
const logHelper = require('./utils').logHelper;
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

    put(stream, size, keyContext, reqUids, callback) {
        const azureKey = this._createAzureKey(keyContext.bucketName,
            keyContext.objectKey, this._bucketMatch);
        const options = { metadata:
            this._translateMetaHeaders(keyContext.metaHeaders,
                keyContext.tagging) };
        this._client.createBlockBlobFromStream(this._azureContainerName,
          azureKey, stream, size, options, err => {
              if (err) {
                  const log = createLogger(reqUids);
                  logHelper(log, 'error', 'err from Azure PUT data backend',
                    err, this._dataStoreName);
                  return callback(errors.InternalError
                    .customizeDescription('Error returned from ' +
                    `Azure: ${err.message}`)
                  );
              }
              return callback(null, azureKey);
          });
    }

    head(objectGetInfo, reqUids, callback) {
        const { key, azureStreamingOptions } = objectGetInfo;
        return this._client.getBlobProperties(this._azureContainerName, key,
          azureStreamingOptions, err => {
              if (err) {
                  const log = createLogger(reqUids);
                  logHelper(log, 'error', 'err from Azure HEAD data backend',
                    err, this._dataStoreName);
                  if (err.code === 'NotFound') {
                      const error = errors.InternalError.customizeDescription(
                          'Unexpected error from Azure: "NotFound". Data on ' +
                          'Azure may have been altered outside of CloudServer.'
                      );
                      return callback(error);
                  }
                  return callback(errors.InternalError
                    .customizeDescription('Error returned from ' +
                    `Azure: ${err.message}`)
                  );
              }
              return callback();
          });
    }

    get(objectGetInfo, range, reqUids, callback) {
        // for backwards compatibility
        const { key, response, azureStreamingOptions } = objectGetInfo;
        this._client.getBlobToStream(this._azureContainerName, key, response,
          azureStreamingOptions, err => {
              if (err) {
                  const log = createLogger(reqUids);
                  logHelper(log, 'error', 'err from Azure GET data backend',
                    err, this._dataStoreName);
                  return callback(errors.InternalError);
              }
              return callback();
          });
    }

    delete(objectGetInfo, reqUids, callback) {
        // for backwards compatibility
        const key = typeof objectGetInfo === 'string' ? objectGetInfo :
          objectGetInfo.key;
        return this._client.deleteBlobIfExists(this._azureContainerName, key,
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
        });
    }

    checkAzureHealth(location, callback) {
        const azureResp = {};
        this._client.doesContainerExist(this._azureContainerName, err => {
            /* eslint-disable no-param-reassign */
            if (err) {
                azureResp[location] = { error: err.message };
                return callback(null, azureResp);
            }
            azureResp[location] = {
                message: 'Congrats! You own the azure container',
            };
            return callback(null, azureResp);
        });
    }

    uploadPart(request, streamingV4Params, partStream, size, key, uploadId,
    partNumber, bucket, log, callback) {
        const azureKey = this._createAzureKey(bucket, key, this._bucketMatch);
        const params = { bucketName: this._azureContainerName,
            partNumber, size, objectKey: azureKey, uploadId };

        if (request.headers['content-md5']) {
            params.contentMD5 = request.headers['content-md5'];
        }
        const dataRetrievalInfo = {
            key: partNumber,
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
            return azureMpuUtils.putSinglePart(this._client, request, params,
            this._dataStoreName, log, (err, dataStoreETag, numberSubParts) => {
                if (err) {
                    return callback(err);
                }
                dataRetrievalInfo.dataStoreETag = dataStoreETag;
                dataRetrievalInfo.numberSubParts = numberSubParts;
                return callback(null, dataRetrievalInfo);
            });
        }
        return azureMpuUtils.putSubParts(this._client, request, params,
        this._dataStoreName, log, (err, dataStoreETag, numberSubParts) => {
            if (err) {
                callback(err);
            }
            dataRetrievalInfo.dataStoreETag = dataStoreETag;
            dataRetrievalInfo.numberSubParts = numberSubParts;
            return callback(null, dataRetrievalInfo);
        });
    }

    objectPutTagging(key, bucket, objectMD, log, callback) {
        const azureKey = this._createAzureKey(bucket, key, this._bucketMatch);
        const azureMD = this._getMetaHeaders(objectMD);
        azureMD.tags = JSON.stringify(objectMD.tags);
        this._client.setBlobMetadata(this._azureContainerName, azureKey,
        azureMD, err => {
            if (err) {
                log.error('err from Azure GET data backend', {
                    error: err,
                    errorMessage: err.message,
                    errorStack: err.stack,
                    dataStoreName: this._dataStoreName,
                });
                return callback(errors.InternalError);
            }
            return callback();
        });
    }

    objectDeleteTagging(key, bucket, objectMD, log, callback) {
        const azureKey = this._createAzureKey(bucket, key, this._bucketMatch);
        const azureMD = this._getMetaHeaders(objectMD);
        this._client.setBlobMetadata(this._azureContainerName, azureKey,
        azureMD, err => {
            if (err) {
                log.error('err from Azure GET data backend', {
                    error: err,
                    errorMessage: err.message,
                    errorStack: err.stack,
                    dataStoreName: this._dataStoreName,
                });
                return callback(errors.InternalError);
            }
            return callback();
        });
    }
}

module.exports = AzureClient;
