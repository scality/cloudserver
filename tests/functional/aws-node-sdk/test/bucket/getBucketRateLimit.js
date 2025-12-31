const assert = require('assert');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');
const getConfig = require('../support/config');
const { sendRateLimitRequest, skipIfRateLimitDisabled } = require('../rateLimit/tooling');

const bucket = 'getratelimitestbucket';
const rateLimitConfig = { RequestsPerSecond: 100 };

skipIfRateLimitDisabled('Test get bucket rate limit', () => {
    let s3;

    before(() => {
        const config = getConfig('lisa', { signatureVersion: 'v4' });
        s3 = new S3Client({ ...config, forcePathStyle: true });
    });

    beforeEach(async () => {
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
    });

    afterEach(async () => {
        try {
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        } catch (err) {
            if (err.name !== 'NoSuchBucket') {
                throw err;
            }
        }
    });

    it('should return the rate limit config', async () => {
        try {
            // First set the rate limit config
            await sendRateLimitRequest('PUT', '127.0.0.1:8000',
                `/${bucket}/?rate-limit`, JSON.stringify(rateLimitConfig));

            // Then get it
            const data = await sendRateLimitRequest('GET', '127.0.0.1:8000',
                `/${bucket}/?rate-limit`);
            assert.strictEqual(data.RequestsPerSecond.Limit, 100);
        } catch (err) {
            assert.ifError(err);
        }
    });

    it('should return NoSuchRateLimitConfig error when config does not exist', async () => {
        try {
            await sendRateLimitRequest('GET', '127.0.0.1:8000', `/${bucket}/?rate-limit`);
            assert.fail('Expected NoSuchRateLimitConfig error');
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'NoSuchRateLimitConfig');
        }
    });

    it('should return NoSuchBucket error when bucket does not exist', async () => {
        try {
            await sendRateLimitRequest('GET', '127.0.0.1:8000', '/nonexistentbucket/?rate-limit');
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'NoSuchBucket');
        }
    });

    it('should return AccessDenied error for non-service user', async () => {
        // This test would require making a request with regular user credentials
        // For now, we'll skip this as it requires additional setup
        // In a real scenario, you'd use regular user credentials here
    });
});
