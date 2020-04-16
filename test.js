const fs = require('fs');
const https = require('https');

const httpOptions = {
    agent: new https.Agent({
        // path on your host of the self-signed certificate
        ca: fs.readFileSync('./ca.crt', 'ascii'),
    }),
};

const s3 = new AWS.S3({
    httpOptions,
    accessKeyId: 'accessKey1',
    secretAccessKey: 'verySecretKey1',
    // The endpoint must be s3.scality.test, else SSL will not work
    endpoint: 'https://s3.scality.test:8000',
    sslEnabled: true,
    // With this setup, you must use path-style bucket access
    s3ForcePathStyle: true,
});

const bucket = 'cocoriko';

s3.createBucket({ Bucket: bucket }, err => {
    if (err) {
        return console.log('err createBucket', err);
    }
    return s3.deleteBucket({ Bucket: bucket }, err => {
        if (err) {
            return console.log('err deleteBucket', err);
        }
        return console.log('SSL is cool!');
    });
});
