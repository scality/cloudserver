const { S3Client, ListObjectsCommand } = require('@aws-sdk/client-s3');

const config = {
    forcePathStyle: true,
    endpoint: 'http://127.0.0.1:8000',
    region: 'us-east-1',
    credentials: {
        accessKeyId: 'accessKey1',
        secretAccessKey: 'verySecretKey1',
    },
};
const s3Client = new S3Client(config);


// In AWS SDK v3, we need to handle the custom search parameter differently
// Since v3 doesn't have the same event system, we'll need to construct the URL manually
const listObjectsCommand = new ListObjectsCommand({ Bucket: 'bucketname' });

// For custom search functionality, you would need to handle this at the HTTP level
// or use the underlying HTTP client middleware
s3Client.send(listObjectsCommand)
    .then(res => {
        process.stdout.write(`Result ${JSON.stringify(res)}`);
    })
    .catch(err => {
        process.stdout.write(`Error ${err}`);
    });
