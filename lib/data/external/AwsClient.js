const { errors } = require('arsenal');
const AWS = require('aws-sdk');
const createLogger = require('../multipleBackendLogger');

class AwsClient {
    constructor(config) {
        this._s3Params = config.s3Params;
        this._awsBucketName = config.awsBucketName;
        this._bucketMatch = config.bucketMatch;
        this._dataStoreName = config.dataStoreName;
        this._client = new AWS.S3(this._s3Params);
    }

    _createAwsKey(requestBucketName, requestObjectKey,
        bucketMatch) {
        if (bucketMatch) {
            return requestObjectKey;
        }
        return `${requestBucketName}/${requestObjectKey}`;
    }

    put(stream, size, keyContext, reqUids, callback) {
        const awsKey = this._createAwsKey(keyContext.bucketName,
           keyContext.objectKey, this._bucketMatch);
        // TODO: if object to be encrypted, use encryption
        // on AWS
        return this._client.upload({
            Bucket: this._awsBucketName,
            Key: awsKey,
            Body: stream,
            Metadata: keyContext.metaHeaders,
            ContentLength: size,
        },
           (err, data) => {
               if (err) {
                   const log = createLogger(reqUids);
                   log.error('err from data backend',
                   { error: err, dataStoreName: this._dataStoreName });
                   return callback(errors.InternalError);
               }
               // because of encryption the ETag here could be
               // different from our metadata so let's store it
               // TODO: let AWS handle encryption
               return callback(null, awsKey, data.ETag);
           });
    }
    get(objectGetInfo, range, reqUids, callback) {
        const log = createLogger(reqUids);
        // for backwards compatibility
        const key = typeof(objectGetInfo) === 'string' ? objectGetInfo :
          objectGetInfo.key;
        const request = this._client.getObject({
            Bucket: this._awsBucketName,
            Key: key,
            Range: range,
        }).on('success', response => {
            log.trace('AWS GET request response headers',
              { responseHeaders: response.httpResponse.headers });
        });
        const stream = request.createReadStream().on('error', err => {
            log.error('error streaming data', { error: err,
                dataStoreName: this._dataStoreName });
            return callback(errors.InternalError);
        });
        return callback(null, stream);
    }
    delete(objectGetInfo, reqUids, callback) {
        // for backwards compatibility
        const key = typeof(objectGetInfo) === 'string' ? objectGetInfo :
          objectGetInfo.key;
        const params = {
            Bucket: this._awsBucketName,
            Key: key,
        };
        return this._client.deleteObject(params, err => {
            if (err) {
                const log = createLogger(reqUids);
                log.error('error deleting object from datastore',
                { error: err, implName: this._clientType });
                return callback(errors.InternalError);
            }
            return callback();
        });
    }
    checkAWSHealth(location, multBackendResp, callback) {
        this._client.headBucket({ Bucket: this._awsBucketName },
        err => {
            /* eslint-disable no-param-reassign */
            if (err) {
                multBackendResp[location] = { error: err };
                return callback(null, multBackendResp);
            }
            return this._client.getBucketVersioning({
                Bucket: this._awsBucketName },
            (err, data) => {
                if (err) {
                    multBackendResp[location] = { error: err };
                } else if (!data.Status ||
                    data.Status === 'Suspended') {
                    multBackendResp[location] = {
                        versioningStatus: data.Status,
                        error: 'Versioning must be enabled',
                    };
                } else {
                    multBackendResp[location] = {
                        versioningStatus: data.Status,
                        message: 'Congrats! You own the bucket',
                    };
                }
                return callback(null, multBackendResp);
            });
        });
    }
}

module.exports = AwsClient;
