const { S3 } = require('aws-sdk');
const config = {
    sslEnabled: false,
    endpoint: 'http://127.0.0.1:8000', // or 'localhost:8000'
    signatureCache: false,
    signatureVersion: 'v4',
    region: 'us-east-1',
    s3ForcePathStyle: true,
    accessKeyId: 'accessKey1',
    secretAccessKey: 'verySecretKey1',
};

const s3 = new S3(config);
console.log(`\nObject.values(s3): ,${Object.values(s3)}\n`);
s3.listBuckets(function(err, data) {
  if (err) console.log(err, err.stack); // an error occurred
  else     console.log(data);           // successful response
});

const bucket = { };
s3.createBucket()
