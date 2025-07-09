const { S3Client, ListObjectsCommand } = require('@aws-sdk/client-s3');
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
const s3Client = new S3Client(config);

const encodedSearch =
    encodeURIComponent('x-amz-meta-color="blue"');

async function main() {
    // v3 does not support request events, so we use middleware to add custom query params
    s3Client.middlewareStack.add(
        next => async args => {
            if (args.request && args.request.path) {
                // eslint-disable-next-line no-param-reassign
                args.request.path += `?search=${encodedSearch}`;
            }
            return next(args);
        },
        {
            step: 'build',
        }
    );

    try {
        const data = await s3Client.send(new ListObjectsCommand({ Bucket: 'bucketname' }));
        process.stdout.write(`Result ${JSON.stringify(data)}`);
    } catch (err) {
        process.stdout.write(`Error ${err}`);
    }
}

main();
