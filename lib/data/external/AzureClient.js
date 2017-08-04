const { errors } = require('arsenal');
const azure = require('azure-storage');
const createLogger = require('../multipleBackendLogger');
const utils = require('./utils.js');
const constants = require('../../../constants');

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
        const log = createLogger(reqUids);
        const azureKey = this._createAzureKey(keyContext.bucketName,
            keyContext.objectKey, this._bucketMatch);
        const options = { metadata:
            this._translateMetaHeaders(keyContext.metaHeaders,
                keyContext.tagging) };
        this._client.createBlockBlobFromStream(this._azureContainerName,
          azureKey, stream, size, options, err => {
              if (err) {
                  log.error('err from Azure PUT data backend',
                  { error: err.message, stack: err.stack,
                    dataStoreName: this._dataStoreName });
                  return callback(errors.InternalError);
              }
              return callback(null, azureKey);
          });
    }

    get(objectGetInfo, range, reqUids, callback) {
        // for backwards compatibility
        const key = typeof(objectGetInfo) === 'string' ? objectGetInfo :
          objectGetInfo.key;
        const response = objectGetInfo.response;
        const azureStreamingOptions = objectGetInfo.azureStreamingOptions;
        this._client.getBlobToStream(this._azureContainerName, key, response,
          azureStreamingOptions, err => {
              if (err) {
                  const log = createLogger(reqUids);
                  log.error('err from Azure GET data backend',
                  { error: err.message, stack: err.stack,
                    dataStoreName: this._dataStoreName });
                  return callback(errors.InternalError);
              }
              return callback();
          });
    }

    delete(objectGetInfo, reqUids, callback) {
        // for backwards compatibility
        const key = typeof(objectGetInfo) === 'string' ? objectGetInfo :
          objectGetInfo.key;
        return this._client.deleteBlob(this._azureContainerName, key,
        err => {
            if (err) {
                const log = createLogger(reqUids);
                log.error('error deleting object from Azure datastore',
                { error: err.message, stack: err.stack,
                  dataStoreName: this._dataStoreName });
                return callback(errors.InternalError);
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

    objectPutTagging(key, bucket, objectMD, log, callback) {
        const azureKey = this._createAzureKey(bucket, key, this._bucketMatch);
        const azureMD = this._getMetaHeaders(objectMD);
        azureMD.tags = JSON.stringify(objectMD.tags);
        this._client.setBlobMetadata(this._azureContainerName, azureKey,
        azureMD, err => {
            if (err) {
                log.error('err from Azure GET data backend',
                { error: err, errorMessage: err.message, errorStack: err.stack,
                    dataStoreName: this._dataStoreName });
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
                log.error('err from Azure GET data backend',
                { error: err, errorMessage: err.message, errorStack: err.stack,
                dataStoreName: this._dataStoreName });
                return callback(errors.InternalError);
            }
            return callback();
        });
    }

    uploadPart(request, streamingV4Params, stream, size, key, uploadId,
    partNumber, bucket, log, callback) {
        const bucketName = this._azureContainerName;
        const mpuTempUploadKey = this._createAzureKey(bucket, key,
          this._bucketMatch);
        // NOTE: I do not use stamp
        const params = { bucketName, partNumber, size,
          mpuTempUploadKey };
        if (size === 0) {
            // Azure does not allow putting empty blocks; instead we will
            // just upload a summary part with content-length of 0.
            log.debug('0-byte part does not store data',
              { method: 'uploadPart' });
            const dataRetrievalInfo = {
                key,
                dataStoreName: this._dataStoreName,
                dataStoreType: 'azure',
                dataStoreETag: constants.zeroByteETag,
                numberSubParts: 0,
            };
            return process.nextTick(callback, null, dataRetrievalInfo);
        }
        if (size <= constants.maxSubPartSize) {
            return utils.putSinglePart(this._client, request, params, log,
              (err, dataStoreETag, numberSubParts) => {
                  if (err) {
                      callback(err);
                  }
                  const dataRetrievalInfo = {
                      key,
                      dataStoreName: this._dataStoreName,
                      dataStoreType: 'azure',
                      dataStoreETag,
                      numberSubParts,
                  };
                  return callback(null, dataRetrievalInfo);
              });
        }
        return utils.putSubParts(this._client, request, params, log,
        (err, dataStoreETag, numberSubParts) => {
            if (err) {
                callback(err);
            }
            const dataRetrievalInfo = {
                key,
                dataStoreName: this._dataStoreName,
                dataStoreType: 'azure',
                dataStoreETag,
                numberSubParts,
            };
            return callback(null, dataRetrievalInfo);
        });
    }
}

module.exports = AzureClient;
