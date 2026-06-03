const assert = require('assert');
const { S3Client, CreateBucketCommand, DeleteBucketCommand } = require('@aws-sdk/client-s3');
const getConfig = require('../support/config');
const { sendRateLimitRequest, skipIfRateLimitDisabled } = require('../rateLimit/tooling');

const bucket = 'deleteratelimitestbucket';
const nonExistentBucket = 'deleteratelimitestnonexistentbucket';

skipIfRateLimitDisabled('Test delete bucket rate limit', () => {
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

    it('should delete the bucket rate limit config', async () => {
        try {
            // First set a rate limit config
            const rateLimitConfig = { RequestsPerSecond: 150 };
            await sendRateLimitRequest(
                'PUT',
                '127.0.0.1:8000',
                `/${bucket}/?rate-limit`,
                JSON.stringify(rateLimitConfig),
            );

            // Then delete it
            await sendRateLimitRequest('DELETE', '127.0.0.1:8000', `/${bucket}/?rate-limit`);

            // Verify it's deleted
            try {
                await sendRateLimitRequest('GET', '127.0.0.1:8000', `/${bucket}/?rate-limit`);
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
            await sendRateLimitRequest('DELETE', '127.0.0.1:8000', `/${bucket}/?rate-limit`);
            assert.ok(true);
        } catch (err) {
            assert.ifError(err);
        }
    });

    it('should return NoSuchBucket error when bucket does not exist', async () => {
        try {
            await sendRateLimitRequest('DELETE', '127.0.0.1:8000', `/${nonExistentBucket}/?rate-limit`);
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'NoSuchBucket');
        }
    });
});
