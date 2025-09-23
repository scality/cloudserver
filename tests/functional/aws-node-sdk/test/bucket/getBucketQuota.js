const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand } = require('@aws-sdk/client-s3');
const assert = require('assert');
const getConfig = require('../support/config');
const sendRequest = require('../quota/tooling').sendRequest;

const bucket = 'getquotatestbucket';
const quota = { quota: 1000 };

describe('Test get bucket quota', () => {
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
    });

    beforeEach(async () => {
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
    });

    afterEach(async () => {
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    });

    it('should return the quota', async () => {
        try {
            await sendRequest('PUT', '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota));
            const { result } = await sendRequest('GET', '127.0.0.1:8000', `/${bucket}/?quota=true`);
            assert.strictEqual(result.GetBucketQuota.Name[0], bucket);
            assert.strictEqual(result.GetBucketQuota.Quota[0], '1000');
        } catch (err) {
            assert.fail(`Expected no error, but got ${err}`);
        }
    });

    it('should return empty quota when not set', async () => {
        try {
            await sendRequest('GET', '127.0.0.1:8000', `/${bucket}/?quota=true`);
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'NoSuchQuota');
        }
    });

    it('should return no such bucket error', async () => {
        try {
            await sendRequest('GET', '127.0.0.1:8000', '/nobucket/?quota=true');
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'NoSuchBucket');
        }
    });

    it('should return no such bucket quota', async () => {
        try {
            await sendRequest('DELETE', '127.0.0.1:8000', `/${bucket}/?quota=true`);
            try {
                await sendRequest('GET', '127.0.0.1:8000', `/${bucket}/?quota=true`);
                assert.fail('Expected NoSuchQuota error');
        } catch (err) {
                assert.strictEqual(err.Error.Code[0], 'NoSuchQuota');
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
                assert.fail('Expected NoSuchQuota error');
            } catch (err) {
                assert.strictEqual(err.Error.Code[0], 'NoSuchQuota');
            }
        } catch (err) {
            assert.fail(`Expected no error, but got ${err}`);
        }
    });
});
