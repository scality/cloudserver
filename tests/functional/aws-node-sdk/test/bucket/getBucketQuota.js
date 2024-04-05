const AWS = require('aws-sdk');
const S3 = AWS.S3;
const assert = require('assert');
const getConfig = require('../support/config');
const sendRequest = require('../quota/tooling').sendRequest;

const bucket = 'getquotatestbucket';
const quota = { quota: 1000 };

describe('Test get bucket quota', () => {
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        AWS.config.update(config);
    });

    beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

    afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

    it('should return the quota', async () => {
        try {
            await sendRequest('PUT', '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota));
            const data = await sendRequest('GET', '127.0.0.1:8000', `/${bucket}/?quota=true`);
            assert.strictEqual(data.GetBucketQuota.Name[0], bucket);
            assert.strictEqual(data.GetBucketQuota.Quota[0], '1000');
        } catch (err) {
            assert.fail(`Expected no error, but got ${err}`);
        }
    });

    it('should return no such bucket error', async () => {
        try {
            await sendRequest('GET', '127.0.0.1:8000', '/test/?quota=true');
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'NoSuchBucket');
        }
    });

    it('should return no such bucket quota', async () => {
        try {
            await sendRequest('DELETE', '127.0.0.1:8000', `/${bucket}/?quota=true`);
            try {
                await sendRequest('GET', '127.0.0.1:8000', `/${bucket}/?quota=true`);
                assert.fail('Expected NoSuchBucketQuota error');
            } catch (err) {
                assert.strictEqual(err.Error.Code[0], 'NoSuchBucketQuota');
            }
        } catch (err) {
            assert.fail(`Expected no error, but got ${err}`);
        }
    });

    it('should return no such bucket error', async () => {
        try {
            await sendRequest('GET', '127.0.0.1:8000', '/test/?quota=true');
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'NoSuchBucket');
        }
    });

    it('should return no such bucket quota', async () => {
        try {
            await sendRequest('DELETE', '127.0.0.1:8000', `/${bucket}/?quota=true`);
            try {
                await sendRequest('GET', '127.0.0.1:8000', `/${bucket}/?quota=true`);
                assert.fail('Expected NoSuchBucketQuota error');
            } catch (err) {
                assert.strictEqual(err.Error.Code[0], 'NoSuchBucketQuota');
            }
        } catch (err) {
            assert.fail(`Expected no error, but got ${err}`);
        }
    });
});
