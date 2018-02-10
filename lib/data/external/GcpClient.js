const async = require('async');
const { errors, s3middleware } = require('arsenal');
const MD5Sum = s3middleware.MD5Sum;

const { GCP, GcpUtils } = require('./GCP');
const { createMpuKey } = GcpUtils;
const AwsClient = require('./AwsClient');
const { prepareStream } = require('../../api/apiUtils/object/prepareStream');
const { logHelper, removeQuotes } = require('./utils');
const { config } = require('../../Config');

const missingVerIdInternalError = errors.InternalError.customizeDescription(
    'Invalid state. Please ensure versioning is enabled ' +
    'in GCP for the location constraint and try again.');

/**
 * Class representing a Google Cloud Storage backend object
 * @extends AwsClient
 */
class GcpClient extends AwsClient {
    /**
     * constructor - creates a Gcp backend client object (inherits )
     * @param {object} config - configuration object for Gcp Backend up
     * @param {object} config.s3params - S3 configuration
     * @param {string} config.bucketName - GCP bucket name
     * @param {string} config.mpuBucket - GCP mpu bucket name
     * @param {string} config.overflowBucket - GCP overflow bucket name
     * @param {boolean} config.bucketMatch - bucket match flag
     * @param {object} config.authParams - GCP service credentials
     * @param {string} config.dataStoreName - locationConstraint name
     * @param {booblean} config.serverSideEncryption - server side encryption
     * flag
     * @return {object} - returns a GcpClient object
     */
    constructor(config) {
        super(config);
        this.clientType = 'gcp';
        this.type = 'GCP';
        this._gcpBucketName = config.bucketName;
        this._mpuBucketName = config.mpuBucket;
        this._overflowBucketName = config.overflowBucket;
        this._createGcpKey = this._createAwsKey.bind(this);
        this._gcpParams = Object.assign(this._s3Params, {
            mainBucket: this._gcpBucketName,
            mpuBucket: this._mpuBucketName,
            overflowBucket: this._overflowBucketName,
            jsonEndpoint: config.jsonEndpoint,
            proxy: config.proxy,
            authParams: config.authParams,
        });
        this._client = new GCP(this._gcpParams);
        this.listParts = undefined;
    }

    /**
     * healthcheck - the gcp health requires checking multiple buckets:
     * main, mpu, and overflow buckets
     * @param {string} location - location name
     * @param {function} callback - callback function to call with the bucket
     * statuses
     * @return {undefined}
     */
    healthcheck(location, callback) {
        const checkBucketHealth = (bucket, cb) => {
            let bucketResp;
            this._client.headBucket({ Bucket: bucket }, err => {
                if (err) {
                    bucketResp = {
                        gcpBucket: bucket,
                        error: err,
                        external: true };
                    return cb(null, bucketResp);
                }
                bucketResp = {
                    gcpBucket: bucket,
                    message: 'Congrats! You own the bucket',
                };
                return cb(null, bucketResp);
            });
        };
        const bucketList = [
            this._gcpBucketName,
            this._mpuBucketName,
            this._overflowBucketName,
        ];
        async.map(bucketList, checkBucketHealth, (err, results) => {
            const gcpResp = {};
            gcpResp[location] = {
                buckets: [],
            };
            if (err) {
                // err should always be undefined
                return callback(errors.InternalFailure
                    .customizeDescription('Unable to perform health check'));
            }
            results.forEach(bucketResp => {
                if (bucketResp.error) {
                    gcpResp[location].error = true;
                }
                gcpResp[location].buckets.push(bucketResp);
            });
            return callback(null, gcpResp);
        });
    }

    createMPU(key, metaHeaders, bucketName, websiteRedirectHeader, contentType,
        cacheControl, contentDisposition, contentEncoding, log, callback) {
        const metaHeadersTrimmed = {};
        Object.keys(metaHeaders).forEach(header => {
            if (header.startsWith('x-amz-meta-')) {
                const headerKey = header.substring(11);
                metaHeadersTrimmed[headerKey] = metaHeaders[header];
            }
        });
        Object.assign(metaHeaders, metaHeadersTrimmed);
        const gcpKey = this._createGcpKey(bucketName, key, this._bucketMatch);
        const params = {
            Bucket: this._mpuBucketName,
            Key: gcpKey,
            Metadata: metaHeaders,
            ContentType: contentType,
            CacheControl: cacheControl,
            ContentDisposition: contentDisposition,
            ContentEncoding: contentEncoding,
        };
        return this._client.createMultipartUpload(params, (err, mpuResObj) => {
            if (err) {
                logHelper(log, 'error', 'err from data backend',
                  err, this._dataStoreName, this.clientType);
                return callback(errors.ServiceUnavailable
                  .customizeDescription('Error returned from ' +
                  `GCP: ${err.message}`)
                );
            }
            return callback(null, mpuResObj);
        });
    }

    completeMPU(jsonList, mdInfo, key, uploadId, bucketName, log, callback) {
        const gcpKey = this._createGcpKey(bucketName, key, this._bucketMatch);
        const partArray = [];
        const partList = jsonList.Part;

        partList.forEach(partObj => {
            const number = partObj.PartNumber[0];
            const partNumber = typeof number === 'string' ?
                parseInt(number, 10) : number;
            const partParams = {
                PartName: createMpuKey(gcpKey, uploadId, partNumber),
                PartNumber: partNumber,
                ETag: partObj.ETag[0],
            };
            partArray.push(partParams);
        });
        const mpuParams = {
            Bucket: this._gcpBucketName,
            MPU: this._mpuBucketName,
            Overflow: this._overflowBucketName,
            Key: gcpKey,
            UploadId: uploadId,
            MultipartUpload: { Parts: partArray },
        };
        const completeObjData = { key: gcpKey };
        return this._client.completeMultipartUpload(mpuParams,
        (err, completeMpuRes) => {
            if (err) {
                logHelper(log, 'error', 'err from data backend on ' +
                'completeMPU', err, this._dataStoreName, this.clientType);
                return callback(errors.ServiceUnavailable
                  .customizeDescription('Error returned from ' +
                  `GCP: ${err.message}`)
                );
            }
            if (!completeMpuRes.VersionId) {
                logHelper(log, 'error', 'missing version id for data ' +
                'backend object', missingVerIdInternalError,
                    this._dataStoreName, this.clientType);
                return callback(missingVerIdInternalError);
            }
            completeObjData.eTag = removeQuotes(completeMpuRes.ETag);
            completeObjData.dataStoreVersionId = completeMpuRes.VersionId;
            completeObjData.contentLength = completeMpuRes.ContentLength;
            return callback(null, completeObjData);
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

        const gcpKey = this._createGcpKey(bucketName, key, this._bucketMatch);
        const params = {
            Bucket: this._mpuBucketName,
            Key: gcpKey,
            UploadId: uploadId,
            Body: hashedStream,
            ContentLength: size,
            PartNumber: partNumber };
        return this._client.uploadPart(params, (err, partResObj) => {
            if (err) {
                logHelper(log, 'error', 'err from data backend ' +
                  'on uploadPart', err, this._dataStoreName, this.clientType);
                return callback(errors.ServiceUnavailable
                  .customizeDescription('Error returned from ' +
                  `GCP: ${err.message}`)
                );
            }
            const noQuotesETag = removeQuotes(partResObj.ETag);
            const dataRetrievalInfo = {
                key: gcpKey,
                dataStoreType: 'gcp',
                dataStoreName: this._dataStoreName,
                dataStoreETag: noQuotesETag,
            };
            return callback(null, dataRetrievalInfo);
        });
    }

    uploadPartCopy(request, gcpSourceKey, sourceLocationConstraintName, log,
    callback) {
        const destBucketName = request.bucketName;
        const destObjectKey = request.objectKey;
        const destGcpKey = this._createGcpKey(destBucketName, destObjectKey,
        this._bucketMatch);

        const sourceGcpBucketName =
            config.getGcpBucketNames(sourceLocationConstraintName).bucketName;

        const uploadId = request.query.uploadId;
        const partNumber = request.query.partNumber;
        const copySourceRange = request.headers['x-amz-copy-source-range'];

        if (copySourceRange) {
            return callback(errors.NotImplemented
              .customizeDescription('Error returned from ' +
                `${this.clientType}: copySourceRange not implemented`)
            );
        }

        const params = {
            Bucket: this._mpuBucketName,
            CopySource: `${sourceGcpBucketName}/${gcpSourceKey}`,
            Key: destGcpKey,
            UploadId: uploadId,
            PartNumber: partNumber,
        };
        return this._client.uploadPartCopy(params, (err, res) => {
            if (err) {
                if (err.code === 'AccesssDenied') {
                    logHelper(log, 'error', 'Unable to access ' +
                    `${sourceGcpBucketName} GCP bucket`, err,
                    this._dataStoreName, this.clientType);
                    return callback(errors.AccessDenied
                      .customizeDescription('Error: Unable to access ' +
                      `${sourceGcpBucketName} GCP bucket`)
                    );
                }
                logHelper(log, 'error', 'error from data backend on ' +
                'uploadPartCopy', err, this._dataStoreName);
                return callback(errors.ServiceUnavailable
                  .customizeDescription('Error returned from ' +
                  `GCP: ${err.message}`)
                );
            }
            const eTag = removeQuotes(res.CopyObjectResult.ETag);
            return callback(null, eTag);
        });
    }

    abortMPU(key, uploadId, bucketName, log, callback) {
        const gcpKey = this._createGcpKey(bucketName, key, this._bucketMatch);
        const getParams = {
            Bucket: this._gcpBucketName,
            MPU: this._mpuBucketName,
            Overflow: this._overflowBucketName,
            Key: gcpKey,
            UploadId: uploadId,
        };
        return this._client.abortMultipartUpload(getParams, err => {
            if (err) {
                logHelper(log, 'error', 'err from data backend ' +
                    'on abortMPU', err, this._dataStoreName, this.clientType);
                return callback(errors.ServiceUnavailable
                  .customizeDescription('Error returned from ' +
                    `GCP: ${err.message}`)
                );
            }
            return callback();
        });
    }
}

module.exports = GcpClient;
