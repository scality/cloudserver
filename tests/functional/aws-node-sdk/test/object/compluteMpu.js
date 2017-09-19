const assert = require('assert');
const { S3 } = require('aws-sdk');

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
    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        s3.createBucket({ Bucket: bucket }, done);
    });

    // delete bucket after testing
    after(done => s3.deleteBucket({ Bucket: bucket }, done));

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
        s3.completeMultipartUpload(params, error => {
            if (error) {
                assert.strictEqual(error.statusCode, 400);
                assert.strictEqual(
                    error.code, 'InvalidRequest');
                done();
            } else {
                done('accepted xml body larger than 1 MB');
            }
        });
    });
});
