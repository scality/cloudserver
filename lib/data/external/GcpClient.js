const async = require('async');
const { errors } = require('arsenal');

const { GCP } = require('./GCP');
const AwsClient = require('./AwsClient');

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
        this._gcpParams = Object.assign(this._s3Params, {
            mainBucket: this._gcpBucketName,
            mpuBucket: this._mpuBucketName,
            overflowBucket: this._overflowBucketName,
            jsonEndpoint: config.jsonEndpoint,
            proxy: config.proxy,
            authParams: config.authParams,
        });
        this._client = new GCP(this._gcpParams);
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
}

module.exports = GcpClient;
