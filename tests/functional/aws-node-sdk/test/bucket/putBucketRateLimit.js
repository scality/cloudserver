const AWS = require('aws-sdk');
const S3 = AWS.S3;
const assert = require('assert');
const getConfig = require('../support/config');
const { sendRateLimitRequest, skipIfRateLimitDisabled } = require('../rateLimit/tooling');
const { config } = require('../../../../../lib/Config');

const bucket = 'putratelimitestbucket';
const nonExistentBucket = 'putratelimitestnonexistentbucket';
const rateLimitConfig = { RequestsPerSecond: 200 };
const invalidConfig = { RequestsPerSecond: -100 };
const invalidConfigNotInteger = { RequestsPerSecond: 10.5 };
const missingLimitConfig = {};

skipIfRateLimitDisabled('Test put bucket rate limit', () => {
    let s3;

    before(() => {
        const config = getConfig('lisa', { signatureVersion: 'v4' });
        s3 = new S3(config);
        AWS.config.update(config);
    });

    beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

    afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

    it('should set the rate limit config', async () => {
        try {
            await sendRateLimitRequest('PUT', '127.0.0.1:8000',
                `/${bucket}/?rate-limit`, JSON.stringify(rateLimitConfig));
            assert.ok(true);
        } catch (err) {
            assert.ifError(err);
        }
    });

    it('should update existing rate limit config', async () => {
        try {
            const initialConfig = { RequestsPerSecond: 100 };
            await sendRateLimitRequest('PUT', '127.0.0.1:8000',
                `/${bucket}/?rate-limit`, JSON.stringify(initialConfig));

            await sendRateLimitRequest('PUT', '127.0.0.1:8000',
                `/${bucket}/?rate-limit`, JSON.stringify(rateLimitConfig));

            // Verify the update
            const data = await sendRateLimitRequest('GET', '127.0.0.1:8000',
                `/${bucket}/?rate-limit`);
            assert.strictEqual(data.RequestsPerSecond.Limit, 200);
        } catch (err) {
            assert.ifError(err);
        }
    });

    it('should return NoSuchBucket error when bucket does not exist', async () => {
        try {
            await sendRateLimitRequest('PUT', '127.0.0.1:8000',
                `/${nonExistentBucket}/?rate-limit`, JSON.stringify(rateLimitConfig));
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'NoSuchBucket');
        }
    });

    it('should return InvalidArgument error when RequestsPerSecond is negative', async () => {
        try {
            await sendRateLimitRequest('PUT', '127.0.0.1:8000',
                `/${bucket}/?rate-limit`, JSON.stringify(invalidConfig));
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'InvalidArgument');
        }
    });

    it('should return InvalidArgument error when RequestsPerSecond is not an integer', async () => {
        try {
            await sendRateLimitRequest('PUT', '127.0.0.1:8000',
                `/${bucket}/?rate-limit`, JSON.stringify(invalidConfigNotInteger));
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'InvalidArgument');
        }
    });

    it('should return InvalidArgument error when RequestsPerSecond is missing', async () => {
        try {
            await sendRateLimitRequest('PUT', '127.0.0.1:8000',
                `/${bucket}/?rate-limit`, JSON.stringify(missingLimitConfig));
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'InvalidArgument');
        }
    });

    it('should return InvalidArgument error when request body is invalid JSON', async () => {
        try {
            await sendRateLimitRequest('PUT', '127.0.0.1:8000',
                `/${bucket}/?rate-limit`, 'invalid json{');
        } catch (err) {
            assert.strictEqual(err.Error.Code[0], 'InvalidArgument');
        }
    });

    it('should allow zero as a valid RequestsPerSecond value', async () => {
        try {
            const zeroConfig = { RequestsPerSecond: 0 };
            await sendRateLimitRequest('PUT', '127.0.0.1:8000',
                `/${bucket}/?rate-limit`, JSON.stringify(zeroConfig));

            const data = await sendRateLimitRequest('GET', '127.0.0.1:8000',
                `/${bucket}/?rate-limit`);
            assert.deepStrictEqual(data, { RequestsPerSecond:  { Limit: 0 } });
        } catch (err) {
            assert.ifError(err);
        }
    });

    describe('validation against node and worker count', () => {
        it('should reject limits less than (nodes × workers)', async () => {
            const nodes = config.rateLimiting?.nodes || 1;
            const workers = config.clusters || 1;
            const minLimit = nodes * workers;

            try {
                const invalidConfig = { RequestsPerSecond: minLimit - 1 };
                await sendRateLimitRequest('PUT', '127.0.0.1:8000',
                    `/${bucket}/?rate-limit`, JSON.stringify(invalidConfig));
                assert.fail('Should have thrown an error');
            } catch (err) {
                assert.strictEqual(err.Error.Code[0], 'InvalidArgument');
            }
        });

        it('should accept limits equal to (nodes × workers)', async () => {
            const nodes = config.rateLimiting?.nodes || 1;
            const workers = config.clusters || 1;
            const minLimit = nodes * workers;

            try {
                const validConfig = { RequestsPerSecond: minLimit };
                await sendRateLimitRequest('PUT', '127.0.0.1:8000',
                    `/${bucket}/?rate-limit`, JSON.stringify(validConfig));

                const data = await sendRateLimitRequest('GET', '127.0.0.1:8000',
                    `/${bucket}/?rate-limit`);
                assert.strictEqual(data.RequestsPerSecond.Limit, minLimit);
            } catch (err) {
                assert.ifError(err);
            }
        });

        it('should accept limits greater than (nodes × workers)', async () => {
            const nodes = config.rateLimiting?.nodes || 1;
            const workers = config.clusters || 1;
            const minLimit = nodes * workers;

            try {
                const validConfig = { RequestsPerSecond: minLimit + 1000 };
                await sendRateLimitRequest('PUT', '127.0.0.1:8000',
                    `/${bucket}/?rate-limit`, JSON.stringify(validConfig));

                const data = await sendRateLimitRequest('GET', '127.0.0.1:8000',
                    `/${bucket}/?rate-limit`);
                assert.strictEqual(data.RequestsPerSecond.Limit, minLimit + 1000);
            } catch (err) {
                assert.ifError(err);
            }
        });
    });
});
