const AWS = require('aws-sdk');
const S3 = AWS.S3;
const assert = require('assert');
const getConfig = require('../support/config');
const { sendRateLimitRequest, skipIfRateLimitDisabled } = require('../rateLimit/tooling');

const bucket = 'deleteratelimitestbucket';
const nonExistentBucket = 'deleteratelimitestnonexistentbucket';

skipIfRateLimitDisabled('Test delete bucket rate limit', () => {
    let s3;

    before(() => {
        const config = getConfig('lisa', { signatureVersion: 'v4' });
        s3 = new S3(config);
        AWS.config.update(config);
    });

    beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

    afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

    it('should delete the bucket rate limit config', async () => {
        try {
            // First set a rate limit config
            const rateLimitConfig = { RequestsPerSecond: 150 };
            await sendRateLimitRequest('PUT', '127.0.0.1:8000',
                `/${bucket}/?rate-limit`, JSON.stringify(rateLimitConfig));

            // Then delete it
            await sendRateLimitRequest('DELETE', '127.0.0.1:8000',
                `/${bucket}/?rate-limit`);

            // Verify it's deleted
            try {
                await sendRateLimitRequest('GET', '127.0.0.1:8000',
                    `/${bucket}/?rate-limit`);
                assert.fail('Expected NoSuchRateLimitConfig error');
            } catch (err) {
                assert.strictEqual(err.Error.Code[0], 'NoSuchRateLimitConfig');
            }
        } catch (err) {
            assert.ifError(err);
        }
    });

    it('should not return an error even if no rate limit config exists', async () => {
        try {
            await sendRateLimitRequest('DELETE', '127.0.0.1:8000',
                `/${bucket}/?rate-limit`);
            assert.ok(true);
        } catch (err) {
            assert.ifError(err);
        }
    });

    it('should return NoSuchBucket error when bucket does not exist', async () => {
        try {
            await sendRateLimitRequest('DELETE', '127.0.0.1:8000',
                `/${nonExistentBucket}/?rate-limit`);
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'NoSuchBucket');
        }
    });
});
