const AWS = require('aws-sdk');
const S3 = AWS.S3;

const assert = require('assert');
const getConfig = require('../support/config');
const sendRequest = require('../quota/tooling').sendRequest;

const bucket = 'updatequotatestbucket';
const nonExistantBucket = 'updatequotatestnonexistantbucket';
const quota = { quota: 2000 };
const negativeQuota = { quota: -1000 };
const wrongquotaFromat = '1000';

describe('Test update bucket quota', () => {
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        AWS.config.update(config);
    });

    beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

    afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

    it('should update the quota', async () => {
        try {
            await sendRequest('PUT', '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota));
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

    it('should return error when quota is negative', async () => {
        try {
            await sendRequest('PUT', '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(negativeQuota));
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'InvalidArgument');
            assert.strictEqual(err.Error.Message[0], 'Quota value must be a positive number');
        }
    });

    it('should return error when quota is not in correct format', async () => {
        try {
            await sendRequest('PUT', '127.0.0.1:8000', `/${bucket}/?quota=true`, wrongquotaFromat);
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'InvalidArgument');
            assert.strictEqual(err.Error.Message[0], 'Request body must be a JSON object');
        }
    });
});
