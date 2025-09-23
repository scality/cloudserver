const assert = require('assert');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    CompleteMultipartUploadCommand,
} = require('@aws-sdk/client-s3');

const getConfig = require('../support/config');

const bucket = `bucketcompletempu-bucket-${Date.now()}`;

const parts = [];

// results in body of 1049793 bytes (1 MB = 1048576)
for (let i = 0; i < 15600; i++) {
    // an mpu of this many parts would not be allowed.
    // testing here to make sure we are not sent excess xml
    parts.push({
        ETag: 'STRING_VALUE',
        PartNumber: i + 1,
    });
}

describe('aws-node-sdk test bucket complete mpu', () => {
    let s3;

    // setup test
    before(async () => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
    });

    // delete bucket after testing
    after(async () => {
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    });

    const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;
    itSkipIfAWS('should not accept xml body larger than 1 MB', done => {
        const params = {
            Bucket: bucket,
            Key: 'STRING_VALUE',
            UploadId: 'STRING_VALUE',
            MultipartUpload: {
                Parts: parts,
            },
        };
        s3.send(new CompleteMultipartUploadCommand(params)).then(() => {
            done('accepted xml body larger than 1 MB');
        }).catch(error => {
            assert.strictEqual(error.$metadata.httpStatusCode, 400);
            assert.strictEqual(
                error.Code, 'InvalidRequest');
            done();
        });
    });
});
