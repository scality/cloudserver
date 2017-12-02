const GCP = require('./GCP');
const AwsClient = require('./AwsClient');

class GcpClient extends AwsClient {
    constructor(config) {
        super(config);
        this._client = new GCP(this._s3Params);
        this._type = 'GCP';
    }
}

module.exports = GcpClient;
