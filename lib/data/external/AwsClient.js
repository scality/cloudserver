const { errors } = require('arsenal');
const AWS = require('aws-sdk');
const createLogger = require('../multipleBackendLogger');
const { prepareStream } = require('../../api/apiUtils/object/prepareStream');
const MD5Sum = require('../../utilities/MD5Sum');

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
        const uploadParams = {
            Bucket: this._awsBucketName,
            Key: awsKey,
            Body: stream,
            Metadata: keyContext.metaHeaders,
            ContentLength: size,
        };
        if (keyContext.cipherBundle) {
            uploadParams.ServerSideEncryption = 'aws:kms';
        }

        return this._client.upload(uploadParams,
           err => {
               if (err) {
                   const log = createLogger(reqUids);
                   log.error('err from data backend',
                   { error: err.message, dataStoreName: this._dataStoreName });
                   return callback(errors.InternalError);
               }
               return callback(null, awsKey);
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
            log.error('error streaming data from AWS', { error: err.message,
                dataStoreName: this._dataStoreName });
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
                { error: err.message, implName: this._clientType });
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
                    multBackendResp[location] = { error: err.message };
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

    createMPU(key, metaHeaders, bucket, websiteRedirectHeader, log, callback) {
        const awsBucket = this._awsBucketName;
        const awsKey = this._createAwsKey(bucket, key, this._bucketMatch);
        const params = { Bucket: awsBucket, Key: awsKey,
            WebsiteRedirectLocation: websiteRedirectHeader,
            Metadata: metaHeaders };
        return this._client.createMultipartUpload(params, (err, mpuResObj) => {
            if (err) {
                log.error('err from data backend',
                { error: err.message, dataStore: this._dataStoreName });
                return callback(errors.InternalError);
            }
            return callback(null, mpuResObj);
        });
    }

    uploadPart(request, streamingV4Params, size, key, uploadId, partNumber,
    bucket, log, callback) {
        const stream = prepareStream(request, streamingV4Params, log, callback);
        const hashedStream = new MD5Sum();
        stream.pipe(hashedStream);

        const awsBucket = this._awsBucketName;
        const awsKey = this._createAwsKey(bucket, key, this._bucketMatch);
        const params = { Bucket: awsBucket, Key: awsKey, UploadId: uploadId,
            Body: hashedStream, ContentLength: size,
            PartNumber: partNumber };
        return this._client.uploadPart(params, (err, partResObj) => {
            if (err) {
                log.error('err from data backend on uploadPart',
                { error: err.message, dataStoreName: this._dataStoreName });
                return callback(errors.InternalError);
            }
            // Because we manually add quotes to ETag later, remove quotes here
            const noQuotesETag =
                partResObj.ETag.substring(1, partResObj.ETag.length - 1);
            const dataRetrievalInfo = {
                key,
                dataStoreName: this._dataStoreName,
                dataStoreETag: noQuotesETag,
            };
            return callback(null, dataRetrievalInfo);
        });
    }

    completeMPU(partList, key, uploadId, bucket, log, callback) {
        const awsBucket = this._awsBucketName;
        const awsKey = this._createAwsKey(bucket, key, this._bucketMatch);
        const mpuError = {
            InvalidPart: true,
            InvalidPartOrder: true,
            EntityTooSmall: true,
        };
        const partArray = [];
        partList.forEach(partObj => {
            const partParams = { PartNumber: partObj.PartNumber[0],
            ETag: partObj.ETag[0] };
            partArray.push(partParams);
        });
        const mpuParams = {
            Bucket: awsBucket, Key: awsKey, UploadId: uploadId,
            MultipartUpload: {
                Parts: partArray,
            },
        };
        const completeObjData = {};
        return this._client.completeMultipartUpload(mpuParams,
        (err, completeMpuRes) => {
            if (err) {
                if (mpuError[err.code]) {
                    log.trace('err from data backend on completeMPU',
                    { error: err.message,
                        dataStoreName: this._dataStoreName });
                    return callback(errors[err.code]);
                }
                log.error('err from data backend on completeMPU',
                { error: err.message, dataStoreName: this._dataStoreName });
                return callback(errors.InternalError);
            }
            // need to get content length of new object to store
            // in our metadata
            return this._client.headObject({ Bucket: awsBucket, Key: awsKey },
            (err, objHeaders) => {
                if (err) {
                    log.trace('err from data backend on headObject',
                    { error: err.message,
                        dataStoreName: this._dataStoreName });
                    return callback(errors.InternalError);
                }
                completeObjData.eTag = completeMpuRes.ETag;
                completeObjData.contentLength = objHeaders.ContentLength;
                return callback(null, completeObjData);
            });
        });
    }

    abortMPU(key, uploadId, bucket, log, callback) {
        const awsBucket = this._awsBucketName;
        const awsKey = this._createAwsKey(bucket, key, this._bucketMatch);
        const abortParams = {
            Bucket: awsBucket, Key: awsKey, UploadId: uploadId,
        };
        return this._client.abortMultipartUpload(abortParams, err => {
            if (err) {
                log.error('There was an error aborting the MPU on AWS S3. You' +
                    ' should abort directly on AWS S3 using the same uploadId.',
                { error: err.message, dataStoreName: this._dataStoreName });
                return callback(errors.InternalError);
            }
            return callback();
        });
    }
}

module.exports = AwsClient;
