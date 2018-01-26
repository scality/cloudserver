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
        this._gcpBucketName = config.bucketName;
        this._mpuBucketName = config.mpuBucket;
        this._overflowBucketname = config.overflowBucket;
        this._gcpParams = Object.assign(this._s3Params, {
            mainBucket: this._gcpBucketName,
            mpuBucket: this._mpuBucketName,
            overflowBucket: this._overflowBucketname,
            authParams: config.authParams,
        });
        this._client = new GCP(this._gcpParams);
    }
}

module.exports = GcpClient;
