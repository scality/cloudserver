const { GCP } = require('./GCP');
const AwsClient = require('./AwsClient');

class GcpClient extends AwsClient {
    constructor(config) {
        super(config);
        this.clientType = 'gcp';
        this._gcpBucketName = this._awsBucketName;
        this._mpuBucketName = config.mpuBucket;
        this._overflowBucketname = config.overflowBucket;
        this._gcpParams = Object.assign(this._s3Params, {
            mainBucket: this._gcpBucketName,
            mpuBucket: this._mpuBucketName,
            overflowBucket: this._overflowBucketname,
            authParams: config.authParams,
        });
        this._client = new GCP(this._gcpParams);
        this._type = 'GCP';
    }
}

module.exports = GcpClient;
