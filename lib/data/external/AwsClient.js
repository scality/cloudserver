const { errors, s3middleware } = require('arsenal');
const AWS = require('aws-sdk');
const MD5Sum = s3middleware.MD5Sum;
const getMetaHeaders = s3middleware.userMetadata.getMetaHeaders;
const createLogger = require('../multipleBackendLogger');
const { prepareStream } = require('../../api/apiUtils/object/prepareStream');
const { logHelper, removeQuotes, trimXMetaPrefix } = require('./utils');
const { config } = require('../../Config');

const missingVerIdInternalError = errors.InternalError.customizeDescription(
    'Invalid state. Please ensure versioning is enabled ' +
    'in AWS for the location constraint and try again.'
);

class AwsClient {
    constructor(config) {
        this._s3Params = config.s3Params;
        this._awsBucketName = config.awsBucketName;
        this._bucketMatch = config.bucketMatch;
        this._dataStoreName = config.dataStoreName;
        this._serverSideEncryption = config.serverSideEncryption;
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
        const metaHeaders = trimXMetaPrefix(keyContext.metaHeaders);
        const log = createLogger(reqUids);

        const putCb = (err, data) => {
            if (err) {
                logHelper(log, 'error', 'err from data backend',
                  err, this._dataStoreName);
                return callback(errors.InternalError
                  .customizeDescription('Error returned from ' +
                  `AWS: ${err.message}`)
                );
            }
            if (!data.VersionId) {
                logHelper(log, 'error', 'missing version id for data ' +
                    'backend object', missingVerIdInternalError,
                    this._dataStoreName);
                return callback(missingVerIdInternalError);
            }
            const dataStoreVersionId = data.VersionId;
            return callback(null, awsKey, dataStoreVersionId);
        };

        const params = {
            Bucket: this._awsBucketName,
            Key: awsKey,
        };
        // we call data.put to create a delete marker, but it's actually a
        // delete request in call to AWS
        if (keyContext.isDeleteMarker) {
            return this._client.deleteObject(params, putCb);
        }
        const uploadParams = params;
        uploadParams.Metadata = metaHeaders;
        uploadParams.ContentLength = size;
        if (this._serverSideEncryption) {
            uploadParams.ServerSideEncryption = 'AES256';
        }
        if (keyContext.tagging) {
            uploadParams.Tagging = keyContext.tagging;
        }

        if (!stream) {
            return this._client.putObject(uploadParams, putCb);
        }

        uploadParams.Body = stream;
        return this._client.upload(uploadParams, putCb);
    }
    head(objectGetInfo, reqUids, callback) {
        const log = createLogger(reqUids);
        const { key, dataStoreVersionId } = objectGetInfo;
        return this._client.headObject({
            Bucket: this._awsBucketName,
            Key: key,
            VersionId: dataStoreVersionId,
        }, err => {
            if (err) {
                logHelper(log, 'error', 'error heading object ' +
                'from datastore', err, this._dataStoreName);
                if (err.code === 'NotFound') {
                    const error = errors.InternalError.customizeDescription(
                        'Unexpected error from AWS: "NotFound". Data on AWS ' +
                        'may have been altered outside of CloudServer.'
                    );
                    return callback(error);
                }
                return callback(errors.InternalError
                  .customizeDescription('Error returned from ' +
                  `AWS: ${err.message}`)
                );
            }
            return callback();
        });
    }
    get(objectGetInfo, range, reqUids, callback) {
        const log = createLogger(reqUids);
        const { key, dataStoreVersionId } = objectGetInfo;
        const request = this._client.getObject({
            Bucket: this._awsBucketName,
            Key: key,
            VersionId: dataStoreVersionId,
            Range: range,
        }).on('success', response => {
            log.trace('AWS GET request response headers',
              { responseHeaders: response.httpResponse.headers });
        });
        const stream = request.createReadStream().on('error', err => {
            logHelper(log, 'error', 'error streaming data from AWS',
              err, this._dataStoreName);
            return callback(err);
        });
        return callback(null, stream);
    }
    delete(objectGetInfo, reqUids, callback) {
        const { key, dataStoreVersionId, deleteVersion } = objectGetInfo;
        const log = createLogger(reqUids);
        const params = {
            Bucket: this._awsBucketName,
            Key: key,
        };
        if (deleteVersion) {
            params.VersionId = dataStoreVersionId;
        }
        return this._client.deleteObject(params, err => {
            if (err) {
                logHelper(log, 'error', 'error deleting object from ' +
                'datastore', err, this._dataStoreName);
                if (err.code === 'NoSuchVersion') {
                    // data may have been deleted directly from the AWS backend
                    // don't want to retry the delete and errors are not
                    // sent back to client anyway, so no need to return err
                    return callback();
                }
                return callback(errors.InternalError
                  .customizeDescription('Error returned from ' +
                  `AWS: ${err.message}`)
                );
            }
            return callback();
        });
    }

    checkAWSHealth(location, callback) {
        const awsResp = {};
        this._client.headBucket({ Bucket: this._awsBucketName },
        err => {
            /* eslint-disable no-param-reassign */
            if (err) {
                awsResp[location] = { error: err };
                return callback(null, awsResp);
            }
            return this._client.getBucketVersioning({
                Bucket: this._awsBucketName },
            (err, data) => {
                if (err) {
                    awsResp[location] = { error: err };
                } else if (!data.Status ||
                    data.Status === 'Suspended') {
                    awsResp[location] = {
                        versioningStatus: data.Status,
                        error: 'Versioning must be enabled',
                    };
                } else {
                    awsResp[location] = {
                        versioningStatus: data.Status,
                        message: 'Congrats! You own the bucket',
                    };
                }
                return callback(null, awsResp);
            });
        });
    }

    createMPU(key, metaHeaders, bucketName, websiteRedirectHeader, log,
    callback) {
        const awsBucket = this._awsBucketName;
        const awsKey = this._createAwsKey(bucketName, key, this._bucketMatch);
        const params = { Bucket: awsBucket, Key: awsKey,
            WebsiteRedirectLocation: websiteRedirectHeader,
            Metadata: metaHeaders };
        return this._client.createMultipartUpload(params, (err, mpuResObj) => {
            if (err) {
                logHelper(log, 'error', 'err from data backend',
                  err, this._dataStoreName);
                return callback(errors.InternalError
                  .customizeDescription('Error returned from ' +
                  `AWS: ${err.message}`)
                );
            }
            return callback(null, mpuResObj);
        });
    }

    uploadPart(request, streamingV4Params, stream, size, key, uploadId,
    partNumber, bucketName, log, callback) {
        let hashedStream = stream;
        if (request) {
            const partStream = prepareStream(request, streamingV4Params,
                log, callback);
            hashedStream = new MD5Sum();
            partStream.pipe(hashedStream);
        }

        const awsBucket = this._awsBucketName;
        const awsKey = this._createAwsKey(bucketName, key, this._bucketMatch);
        const params = { Bucket: awsBucket, Key: awsKey, UploadId: uploadId,
            Body: hashedStream, ContentLength: size,
            PartNumber: partNumber };
        return this._client.uploadPart(params, (err, partResObj) => {
            if (err) {
                logHelper(log, 'error', 'err from data backend ' +
                  'on uploadPart', err, this._dataStoreName);
                return callback(errors.InternalError
                  .customizeDescription('Error returned from ' +
                  `AWS: ${err.message}`)
                );
            }
            // Because we manually add quotes to ETag later, remove quotes here
            const noQuotesETag =
                partResObj.ETag.substring(1, partResObj.ETag.length - 1);
            const dataRetrievalInfo = {
                key: awsKey,
                dataStoreType: 'aws_s3',
                dataStoreName: this._dataStoreName,
                dataStoreETag: noQuotesETag,
            };
            return callback(null, dataRetrievalInfo);
        });
    }

    listParts(key, uploadId, bucketName, partNumberMarker, maxParts, log,
    callback) {
        const awsBucket = this._awsBucketName;
        const awsKey = this._createAwsKey(bucketName, key, this._bucketMatch);
        const params = { Bucket: awsBucket, Key: awsKey, UploadId: uploadId,
            PartNumberMarker: partNumberMarker, MaxParts: maxParts };
        return this._client.listParts(params, (err, partList) => {
            if (err) {
                logHelper(log, 'error', 'err from data backend on listPart',
                  err, this._dataStoreName);
                return callback(errors.InternalError
                  .customizeDescription('Error returned from ' +
                  `AWS: ${err.message}`)
                );
            }
            // build storedParts object to mimic Scality S3 backend returns
            const storedParts = {};
            storedParts.IsTruncated = partList.IsTruncated;
            storedParts.Contents = [];
            storedParts.Contents = partList.Parts.map(item => {
                // We manually add quotes to ETag later, so remove quotes here
                const noQuotesETag =
                    item.ETag.substring(1, item.ETag.length - 1);
                return {
                    partNumber: item.PartNumber,
                    value: {
                        Size: item.Size,
                        ETag: noQuotesETag,
                        LastModified: item.LastModified,
                    },
                };
            });
            return callback(null, storedParts);
        });
    }

    /**
     * completeMPU - complete multipart upload on AWS backend
     * @param {object} jsonList - user-sent list of parts to include in
     *                          final mpu object
     * @param {object} mdInfo - object containing 3 keys: storedParts,
     *                          mpuOverviewKey, and splitter
     * @param {string} key - object key
     * @param {string} uploadId - multipart upload id string
     * @param {string} bucketName - name of bucket
     * @param {RequestLogger} log - logger instance
     * @param {function} callback - callback function
     * @return {(Error|object)} - return Error if complete MPU fails, otherwise
     * object containing completed object key, eTag, and contentLength
     */
    completeMPU(jsonList, mdInfo, key, uploadId, bucketName, log, callback) {
        const awsBucket = this._awsBucketName;
        const awsKey = this._createAwsKey(bucketName, key, this._bucketMatch);
        const mpuError = {
            InvalidPart: true,
            InvalidPartOrder: true,
            EntityTooSmall: true,
        };
        const partArray = [];
        const partList = jsonList.Part;
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
        const completeObjData = { key: awsKey };
        return this._client.completeMultipartUpload(mpuParams,
        (err, completeMpuRes) => {
            if (err) {
                if (mpuError[err.code]) {
                    logHelper(log, 'trace', 'err from data backend on ' +
                    'completeMPU', err, this._dataStoreName);
                    return callback(errors[err.code]);
                }
                logHelper(log, 'error', 'err from data backend on ' +
                'completeMPU', err, this._dataStoreName);
                return callback(errors.InternalError
                  .customizeDescription('Error returned from ' +
                  `AWS: ${err.message}`)
                );
            }
            if (!completeMpuRes.VersionId) {
                logHelper(log, 'error', 'missing version id for data ' +
                'backend object', missingVerIdInternalError,
                    this._dataStoreName);
                return callback(missingVerIdInternalError);
            }
            // need to get content length of new object to store
            // in our metadata
            return this._client.headObject({ Bucket: awsBucket, Key: awsKey },
            (err, objHeaders) => {
                if (err) {
                    logHelper(log, 'trace', 'err from data backend on ' +
                    'headObject', err, this._dataStoreName);
                    return callback(errors.InternalError
                      .customizeDescription('Error returned from ' +
                      `AWS: ${err.message}`)
                    );
                }
                // remove quotes from eTag because they're added later
                completeObjData.eTag = completeMpuRes.ETag
                    .substring(1, completeMpuRes.ETag.length - 1);
                completeObjData.dataStoreVersionId = completeMpuRes.VersionId;
                completeObjData.contentLength = objHeaders.ContentLength;
                return callback(null, completeObjData);
            });
        });
    }

    abortMPU(key, uploadId, bucketName, log, callback) {
        const awsBucket = this._awsBucketName;
        const awsKey = this._createAwsKey(bucketName, key, this._bucketMatch);
        const abortParams = {
            Bucket: awsBucket, Key: awsKey, UploadId: uploadId,
        };
        return this._client.abortMultipartUpload(abortParams, err => {
            if (err) {
                logHelper(log, 'error', 'There was an error aborting ' +
                'the MPU on AWS S3. You should abort directly on AWS S3 ' +
                'using the same uploadId.', err, this._dataStoreName);
                return callback(errors.InternalError
                  .customizeDescription('Error returned from ' +
                  `AWS: ${err.message}`)
                );
            }
            return callback();
        });
    }

    objectPutTagging(key, bucket, objectMD, log, callback) {
        const awsBucket = this._awsBucketName;
        const awsKey = this._createAwsKey(bucket, key, this._bucketMatch);
        const dataStoreVersionId = objectMD.location[0].dataStoreVersionId;
        const tagParams = {
            Bucket: awsBucket,
            Key: awsKey,
            VersionId: dataStoreVersionId,
        };
        const keyArray = Object.keys(objectMD.tags);
        tagParams.Tagging = {};
        tagParams.Tagging.TagSet = keyArray.map(key => {
            const value = objectMD.tags[key];
            return { Key: key, Value: value };
        });
        return this._client.putObjectTagging(tagParams, err => {
            if (err) {
                logHelper(log, 'error', 'error from data backend on ' +
                  'putObjectTagging', err, this._dataStoreName);
                return callback(errors.InternalError
                  .customizeDescription('Error returned from ' +
                  `AWS: ${err.message}`)
                );
            }
            return callback();
        });
    }

    objectDeleteTagging(key, bucket, objectMD, log, callback) {
        const awsBucket = this._awsBucketName;
        const awsKey = this._createAwsKey(bucket, key, this._bucketMatch);
        const dataStoreVersionId = objectMD.location[0].dataStoreVersionId;
        const tagParams = {
            Bucket: awsBucket,
            Key: awsKey,
            VersionId: dataStoreVersionId,
        };
        return this._client.deleteObjectTagging(tagParams, err => {
            if (err) {
                logHelper(log, 'error', 'error from data backend on ' +
                'deleteObjectTagging', err, this._dataStoreName);
                return callback(errors.InternalError
                  .customizeDescription('Error returned from ' +
                  `AWS: ${err.message}`)
                );
            }
            return callback();
        });
    }
    copyObject(request, sourceKey, sourceLocationConstraintName, log,
    callback) {
        const destBucketName = request.bucketName;
        const destObjectKey = request.objectKey;
        const destAwsKey = this._createAwsKey(destBucketName, destObjectKey,
          this._bucketMatch);

        const sourceAwsBucketName =
          config.getAwsBucketName(sourceLocationConstraintName);

        const metadataDirective = request.headers['x-amz-metadata-directive'];
        const metaHeaders = trimXMetaPrefix(getMetaHeaders(request.headers));
        this._client.copyObject({
            Bucket: this._awsBucketName,
            Key: destAwsKey,
            CopySource: `${sourceAwsBucketName}/${sourceKey}`,
            Metadata: metaHeaders,
            MetadataDirective: metadataDirective,
        }, err => {
            if (err) {
                if (err.code === 'AccessDenied') {
                    logHelper(log, 'error', 'Unable to access ' +
                    `${sourceAwsBucketName} AWS bucket`, err,
                    this._dataStoreName);
                    return callback(errors.AccessDenied
                      .customizeDescription('Error: Unable to access ' +
                      `${sourceAwsBucketName} AWS bucket`)
                    );
                }
                logHelper(log, 'error', 'error from data backend on ' +
                'copyObject', err, this._dataStoreName);
                return callback(errors.InternalError
                  .customizeDescription('Error returned from ' +
                  `AWS: ${err.message}`)
                );
            }
            return callback(null, destAwsKey);
        });
    }
    uploadPartCopy(request, awsSourceKey, sourceLocationConstraintName,
      log, callback) {
        const destBucketName = request.bucketName;
        const destObjectKey = request.objectKey;
        const destAwsKey = this._createAwsKey(destBucketName, destObjectKey,
        this._bucketMatch);

        const sourceAwsBucketName =
          config.getAwsBucketName(sourceLocationConstraintName);

        const uploadId = request.query.uploadId;
        const partNumber = request.query.partNumber;
        const copySourceRange = request.headers['x-amz-copy-source-range'];

        const params = {
            Bucket: this._awsBucketName,
            CopySource: `${sourceAwsBucketName}/${awsSourceKey}`,
            CopySourceRange: copySourceRange,
            Key: destAwsKey,
            PartNumber: partNumber,
            UploadId: uploadId,
        };
        return this._client.uploadPartCopy(params, (err, res) => {
            if (err) {
                if (err.code === 'AccessDenied') {
                    logHelper(log, 'error', 'Unable to access ' +
                    `${sourceAwsBucketName} AWS bucket`, err,
                    this._dataStoreName);
                    return callback(errors.AccessDenied
                      .customizeDescription('Error: Unable to access ' +
                      `${sourceAwsBucketName} AWS bucket`)
                    );
                }
                logHelper(log, 'error', 'error from data backend on ' +
                'uploadPartCopy', err, this._dataStoreName);
                return callback(errors.InternalError
                  .customizeDescription('Error returned from ' +
                  `AWS: ${err.message}`)
                );
            }
            const eTag = removeQuotes(res.CopyPartResult.ETag);
            return callback(null, eTag);
        });
    }
}

module.exports = AwsClient;
