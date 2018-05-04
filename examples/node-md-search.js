const { S3 } = require('aws-sdk');
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
const s3Client = new S3(config);

const encodedSearch =
    encodeURIComponent('x-amz-meta-color="blue"');
const req = s3Client.listObjects({ Bucket: 'bucketname' });

// the build event
req.on('build', () => {
    req.httpRequest.path = `${req.httpRequest.path}?search=${encodedSearch}`;
});
req.on('success', res => {
    process.stdout.write(`Result ${res.data}`);
});
req.on('error', err => {
    process.stdout.write(`Error ${err}`);
});
req.send();
