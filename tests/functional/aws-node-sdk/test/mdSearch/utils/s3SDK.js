const S3 = require('aws-sdk').S3;

const config = {
    sslEnabled: false,
    logger: process.stdout,
    endpoint: 'http://127.0.0.1:8000',
    apiVersions: { s3: '2006-03-01' },
    signatureCache: false,
    signatureVersion: 'v4',
    region: 'us-east-1',
    s3ForcePathStyle: true,
    accessKeyId: 'accessKey1',
    secretAccessKey: 'verySecretKey1',
};

const client = new S3(config);

module.exports = client;
