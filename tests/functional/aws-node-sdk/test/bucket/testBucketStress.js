const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectCommand,
    DeleteObjectCommand,
} = require('@aws-sdk/client-s3');

const getConfig = require('../support/config');

const bucket = `stress-test-bucket-${Date.now()}`;
const text = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890';
const objectCount = 100;
const loopCount = 10;

async function putObjects(s3, loopId) {
    const promises = [];
    for (let i = 0; i < objectCount; i++) {
        const params = { Bucket: bucket, Key: `foo${loopId}_${i}`, Body: text };
        promises.push(s3.send(new PutObjectCommand(params)));
    }
    await Promise.all(promises);
}

async function deleteObjects(s3, loopId) {
    const promises = [];
    for (let i = 0; i < objectCount; i++) {
        const params = { Bucket: bucket, Key: `foo${loopId}_${i}` };
        promises.push(s3.send(new DeleteObjectCommand(params)));
    }
    await Promise.all(promises);
}

describe('aws-node-sdk stress test bucket', function testSuite() {
    this.timeout(120000);
    let s3;
    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
    });

    it('createBucket-putObject-deleteObject-deleteBucket loop', async () => {
        for (let loopId = 0; loopId < loopCount; loopId++) {
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            await putObjects(s3, loopId);
            await deleteObjects(s3, loopId);
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        }
    });
});
