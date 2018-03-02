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
                        error: err };
                } else {
                    bucketResp = {
                        gcpBucket: bucket,
                        message: 'Congrats! You own the bucket',
                    };
                }
                return cb(null, bucketResp);
            });
        };
        const gcpResp = {};
        async.parallel({
            main: done => checkBucketHealth(this._gcpBucketName, done),
            mpu: done => checkBucketHealth(this._mpuBucketName, done),
        }, (err, result) => {
            if (err) {
                return callback(errors.InternalFailure
                    .customizeDescription('Unable to perform health check'));
            }

            gcpResp[location] = result.main.error || result.mpu.error ?
                { error: true, external: true } : {};
            Object.assign(gcpResp[location], result);
            return callback(null, gcpResp);
        });
    }
}

module.exports = GcpClient;
