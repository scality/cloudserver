const { errors } = require('arsenal');
const azure = require('azure-storage');
const createLogger = require('../multipleBackendLogger');

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

    _translateMetaHeaders(metaHeaders) {
        const translatedMetaHeaders = {};
        Object.keys(metaHeaders).forEach(headerName => {
            const translated = headerName.replace(/-/g, '_');
            translatedMetaHeaders[translated] = metaHeaders[headerName];
        });
        return translatedMetaHeaders;
    }

    put(stream, size, keyContext, reqUids, callback) {
        const azureKey = this._createAzureKey(keyContext.bucketName,
           keyContext.objectKey, this._bucketMatch);
        const options = { metadata:
          this._translateMetaHeaders(keyContext.metaHeaders) };
        this._client.createBlockBlobFromStream(this._azureContainerName,
          azureKey, stream, size, options, err => {
              if (err) {
                  const log = createLogger(reqUids);
                  log.error('err from Azure data backend',
                  { error: err.message, stack: err.stack,
                    dataStoreName: this._dataStoreName });
                  return callback(errors.InternalError);
              }
              return callback(null, azureKey);
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
}

module.exports = AzureClient;
