const { S3Client, ListObjectsCommand } = require('@aws-sdk/client-s3');
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const http = require('http');

const config = {
    endpoint: 'http://127.0.0.1:8000',
    region: 'us-east-1',
    forcePathStyle: true,
    credentials: {
        accessKeyId: 'accessKey1',
        secretAccessKey: 'verySecretKey1',
    },
    requestHandler: new NodeHttpHandler({
        httpAgent: new http.Agent({ keepAlive: false }),
    }),
};

const s3Client = new S3Client(config);

const encodedSearch = encodeURIComponent('x-amz-meta-color="blue"');

const command = new ListObjectsCommand({ Bucket: 'bucketname' });

command.middlewareStack.add(
    next => async args => {
        if (args.request && args.request.path) {
            // eslint-disable-next-line no-param-reassign
            args.request.path = `${args.request.path}?search=${encodedSearch}`;
        }
        return next(args);
    },
    {
        step: 'build',
        name: 'addSearchParameter',
        priority: 'high',
    },
);

// Send command and handle response
s3Client
    .send(command)
    .then(data => {
        process.stdout.write(`Result ${JSON.stringify(data)}`);
    })
    .catch(err => {
        process.stdout.write(`Error ${err}`);
    });
