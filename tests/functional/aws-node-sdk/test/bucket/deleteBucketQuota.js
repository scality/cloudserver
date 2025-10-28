const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand } = require('@aws-sdk/client-s3');
const assert = require('assert');
const getConfig = require('../support/config');
const sendRequest = require('../quota/tooling').sendRequest;

const bucket = 'deletequotatestbucket';
const nonExistantBucket = 'deletequotatestnonexistantbucket';

describe('Test delete bucket quota', () => {
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
    });

    beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: bucket })));

    afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

    it('should delete the bucket quota', async () => {
        try {
            await sendRequest('DELETE', '127.0.0.1:8000', `/${bucket}/?quota=true`);
        } catch (err) {
            assert.fail(`Unexpected error: ${err}`);
        }
    });

    it('should return no such bucket error', async () => {
        try {
            await sendRequest('DELETE', '127.0.0.1:8000', `/${nonExistantBucket}/?quota=true`);
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'NoSuchBucket');
        }
    });
});
