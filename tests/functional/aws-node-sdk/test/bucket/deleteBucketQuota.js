const AWS = require('aws-sdk');
const S3 = AWS.S3;
const assert = require('assert');
const getConfig = require('../support/config');
const sendRequest = require('../quota/tooling').sendRequest;

const bucket = 'deletequotatestbucket';
const nonExistantBucket = 'deletequotatestnonexistantbucket';

describe('Test delete bucket quota', () => {
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        AWS.config.update(config);
    });

    beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

    afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

    it('should delete the bucket quota', async () => {
        try {
            await sendRequest('DELETE', '127.0.0.1:8000', `/${bucket}/?quota=true`);
            assert.ok(true);
        } catch (err) {
            assert.fail(`Expected no error, but got ${err}`);
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
