const { S3Client, CreateBucketCommand, DeleteBucketCommand } = require('@aws-sdk/client-s3');
const assert = require('assert');
const getConfig = require('../support/config');
const sendRequest = require('../quota/tooling').sendRequest;

const bucket = 'getquotatestbucket';
const quota = { quota: 1000 };

function rejectsWith(promise, code) {
    return assert.rejects(promise, err => {
        assert.strictEqual(err.Error?.Code?.[0], code);
        return true;
    });
}

describe('Test get bucket quota', () => {
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
    });

    beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: bucket })));

    afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

    it('should return the quota', async () => {
        await sendRequest('PUT', '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota));
        const { result } = await sendRequest('GET', '127.0.0.1:8000', `/${bucket}/?quota=true`);
        assert.strictEqual(result.GetBucketQuota.Name[0], bucket);
        assert.strictEqual(result.GetBucketQuota.Quota[0], '1000');
    });

    it('should return empty quota when not set', () =>
        rejectsWith(sendRequest('GET', '127.0.0.1:8000', `/${bucket}/?quota=true`), 'NoSuchQuota'));

    it('should return no such bucket error', () =>
        rejectsWith(sendRequest('GET', '127.0.0.1:8000', '/nobucket/?quota=true'), 'NoSuchBucket'));

    it('should return no such bucket quota', async () => {
        await sendRequest('DELETE', '127.0.0.1:8000', `/${bucket}/?quota=true`);
        await rejectsWith(sendRequest('GET', '127.0.0.1:8000', `/${bucket}/?quota=true`), 'NoSuchQuota');
    });

    it('should return no such bucket quota after deleting a quota that was set', async () => {
        await sendRequest('PUT', '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota));
        await sendRequest('DELETE', '127.0.0.1:8000', `/${bucket}/?quota=true`);
        await rejectsWith(sendRequest('GET', '127.0.0.1:8000', `/${bucket}/?quota=true`), 'NoSuchQuota');
    });

    it('should return no such bucket error for another nonexistent bucket', () =>
        rejectsWith(sendRequest('GET', '127.0.0.1:8000', '/test/?quota=true'), 'NoSuchBucket'));
});
