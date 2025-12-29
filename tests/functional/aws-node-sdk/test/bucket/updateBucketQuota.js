const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand } = require('@aws-sdk/client-s3');

const assert = require('assert');
const getConfig = require('../support/config');
const sendRequest = require('../quota/tooling').sendRequest;

const bucket = 'updatequotatestbucket';
const nonExistantBucket = 'updatequotatestnonexistantbucket';
const quota = { quota: 2000 };
const negativeQuota = { quota: -1000 };
const wrongquotaFromat = '1000';
const largeQuota = { quota: 1000000000000 };

describe('Test update bucket quota', () => {
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
    });

    beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: bucket })));

    afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

    it('should update the quota', () => sendRequest('PUT', 
        '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota)));

    it('should update quota with XML format', async () => {
        try {
            const xmlQuota = '<QuotaConfiguration><Quota>3000</Quota></QuotaConfiguration>';
            await sendRequest('PUT', '127.0.0.1:8000', `/${bucket}/?quota=true`, xmlQuota);
            assert.ok(true);
        } catch (err) {
            assert.fail(`Expected no error, but got ${err}`);
        }
    });

    it('should return no such bucket error', async () => {
        try {
            await sendRequest('PUT', '127.0.0.1:8000', `/${nonExistantBucket}/?quota=true`, JSON.stringify(quota));
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'NoSuchBucket');
        }
    });

    it('should return invalid request error for negative quota', async () => {
        try {
            await sendRequest('PUT', '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(negativeQuota));
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'InvalidArgument');
            assert.strictEqual(err.Error.Message[0], 'Quota value must be a positive number');
        }
    });

    it('should return invalid request error for wrong quota format', async () => {
        try {
            await sendRequest('PUT', '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(wrongquotaFromat));
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'InvalidArgument');
            assert.strictEqual(err.Error.Message[0], 'Request body must be a JSON object');
        }
    });

    it('should accept large quota', () => sendRequest('PUT', 
        '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(largeQuota)));
});
