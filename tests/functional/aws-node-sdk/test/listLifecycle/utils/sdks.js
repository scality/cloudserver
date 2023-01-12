const S3 = require('aws-sdk').S3;
const LifecycleClient = require('./lifecycleClient');

const config = {
    sslEnabled: false,
    endpoint: 'http://127.0.0.1:8000',
    signatureCache: false,
    signatureVersion: 'v4',
    region: 'us-east-1',
    s3ForcePathStyle: true,
    accessKeyId: 'accessKey1',
    secretAccessKey: 'verySecretKey1',
};

const lifecycleClient = new LifecycleClient(config);
const s3Client = new S3(config);

module.exports = { lifecycleClient, s3Client } ;
