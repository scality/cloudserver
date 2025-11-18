const assert = require('assert');
const sinon = require('sinon');
const { bucketPut } = require('../../../lib/api/bucketPut');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const bucketPutRateLimit = require('../../../lib/api/bucketPutRateLimit');
const bucketGetRateLimit = require('../../../lib/api/bucketGetRateLimit');
const { config } = require('../../../lib/Config');
const AuthInfo = require('arsenal').auth.AuthInfo;
const metadata = require('../../../lib/metadata/wrapper');

const log = new DummyRequestLogger();
const bucketName = 'bucketname';
const serviceUserArn = 'arn:aws:iam::123456789012:user/rate-limit-service';

const rateLimitServiceAuthInfo = new AuthInfo({
    canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
    shortid: '123456789012',
    email: 'ratelimit@service.com',
    accountDisplayName: 'rateLimitServiceDisplayName',
    arn: serviceUserArn,
});

const regularAuthInfo = makeAuthInfo('accessKey1');

const bucketPutReq = {
    bucketName,
    headers: {
        host: `${bucketName}.s3.amazonaws.com`,
    },
    url: '/',
    actionImplicitDenies: false,
};

function getRateLimitPutRequest(bucketName, configJson) {
    return {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
            origin: 'http://example.com',
        },
        url: '/?rate-limit',
        method: 'PUT',
        post: JSON.stringify(configJson),
        actionImplicitDenies: false,
    };
}

describe('bucketPutRateLimit API', () => {
    let sandbox;

    beforeEach(() => {
        sandbox = sinon.createSandbox();
        sandbox.stub(config, 'rateLimiting').value({
            serviceUserArn,
        });
    });

    afterEach(() => {
        sandbox.restore();
        cleanup();
    });

    it('should return AccessDenied error if user is not a rate limit service user', done => {
        bucketPut(regularAuthInfo, bucketPutReq, log, err => {
            assert.ifError(err);
            const rateLimitConfig = { RequestsPerSecond: 100 };
            const putRequest = getRateLimitPutRequest(bucketName, rateLimitConfig);
            bucketPutRateLimit(regularAuthInfo, putRequest, log, err => {
                assert.strictEqual(err.is.AccessDenied, true);
                done();
            });
        });
    });

    it('should return InvalidArgument error if RequestsPerSecond is missing', done => {
        bucketPut(regularAuthInfo, bucketPutReq, log, err => {
            assert.ifError(err);
            const invalidConfig = {};
            const putRequest = getRateLimitPutRequest(bucketName, invalidConfig);
            bucketPutRateLimit(rateLimitServiceAuthInfo, putRequest, log, err => {
                assert.strictEqual(err.is.InvalidArgument, true);
                done();
            });
        });
    });

    it('should return InvalidArgument error if RequestsPerSecond is NaN', done => {
        bucketPut(regularAuthInfo, bucketPutReq, log, err => {
            assert.ifError(err);
            const invalidConfig = { RequestsPerSecond: NaN };
            const putRequest = getRateLimitPutRequest(bucketName, invalidConfig);
            bucketPutRateLimit(rateLimitServiceAuthInfo, putRequest, log, err => {
                assert.strictEqual(err.is.InvalidArgument, true);
                done();
            });
        });
    });

    it('should return InvalidArgument error if RequestsPerSecond is not an integer', done => {
        bucketPut(regularAuthInfo, bucketPutReq, log, err => {
            assert.ifError(err);
            const invalidConfig = { RequestsPerSecond: 10.5 };
            const putRequest = getRateLimitPutRequest(bucketName, invalidConfig);
            bucketPutRateLimit(rateLimitServiceAuthInfo, putRequest, log, err => {
                assert.strictEqual(err.is.InvalidArgument, true);
                done();
            });
        });
    });

    it('should return InvalidArgument error if RequestsPerSecond is negative', done => {
        bucketPut(regularAuthInfo, bucketPutReq, log, err => {
            assert.ifError(err);
            const invalidConfig = { RequestsPerSecond: -1 };
            const putRequest = getRateLimitPutRequest(bucketName, invalidConfig);
            bucketPutRateLimit(rateLimitServiceAuthInfo, putRequest, log, err => {
                assert.strictEqual(err.is.InvalidArgument, true);
                done();
            });
        });
    });

    it('should return InvalidArgument error if request body is invalid JSON', done => {
        bucketPut(regularAuthInfo, bucketPutReq, log, err => {
            assert.ifError(err);
            const putRequest = {
                bucketName,
                headers: {
                    host: `${bucketName}.s3.amazonaws.com`,
                },
                url: '/?rate-limit',
                method: 'PUT',
                post: 'invalid json{',
                actionImplicitDenies: false,
            };
            bucketPutRateLimit(rateLimitServiceAuthInfo, putRequest, log, err => {
                assert.strictEqual(err.is.InvalidArgument, true);
                done();
            });
        });
    });

    it('should return NoSuchBucket error if bucket does not exist', done => {
        const rateLimitConfig = { RequestsPerSecond: 100 };
        const putRequest = getRateLimitPutRequest('nonexistent-bucket', rateLimitConfig);
        bucketPutRateLimit(rateLimitServiceAuthInfo, putRequest, log, err => {
            assert(err, 'should return an error');
            assert.strictEqual(err.is.NoSuchBucket, true);
            done();
        });
    });
});

describe('bucketPutRateLimit API success cases', () => {
    let sandbox;

    beforeEach(() => {
        sandbox = sinon.createSandbox();
        sandbox.stub(config, 'rateLimiting').value({
            serviceUserArn,
        });
    });

    afterEach(() => {
        sandbox.restore();
        cleanup();
    });

    it('should set rate limit configuration on bucket', done => {
        bucketPut(regularAuthInfo, bucketPutReq, log, err => {
            assert.ifError(err);
            const rateLimitConfig = { RequestsPerSecond: 100 };
            const putRequest = getRateLimitPutRequest(bucketName, rateLimitConfig);
            bucketPutRateLimit(rateLimitServiceAuthInfo, putRequest, log, err => {
                assert.ifError(err);
                return metadata.getBucket(bucketName, log, (err, bucket) => {
                    assert.ifError(err);
                    const bucketRateLimitConfig = bucket.getRateLimitConfiguration();
                    assert(bucketRateLimitConfig, 'rate limit config should exist');
                    const configData = bucketRateLimitConfig.getData();
                    assert.strictEqual(configData.RequestsPerSecond.Limit, 100);
                    done();
                });
            });
        });
    });

    it('should update existing rate limit configuration', done => {
        bucketPut(regularAuthInfo, bucketPutReq, log, err => {
            assert.ifError(err);
            const initialConfig = { RequestsPerSecond: 100 };
            const initialPutRequest = getRateLimitPutRequest(bucketName, initialConfig);
            bucketPutRateLimit(rateLimitServiceAuthInfo, initialPutRequest, log, err => {
                assert.ifError(err);
                const updatedConfig = { RequestsPerSecond: 200 };
                const updatedPutRequest = getRateLimitPutRequest(bucketName, updatedConfig);
                bucketPutRateLimit(rateLimitServiceAuthInfo, updatedPutRequest, log, err => {
                    assert.ifError(err);
                    return metadata.getBucket(bucketName, log, (err, bucket) => {
                        assert.ifError(err);
                        const bucketRateLimitConfig = bucket.getRateLimitConfiguration();
                        const configData = bucketRateLimitConfig.getData();
                        assert.strictEqual(configData.RequestsPerSecond.Limit, 200);
                        done();
                    });
                });
            });
        });
    });

    it('should allow zero as a valid RequestsPerSecond value', done => {
        bucketPut(regularAuthInfo, bucketPutReq, log, err => {
            assert.ifError(err);
            const rateLimitConfig = { RequestsPerSecond: 0 };
            const putRequest = getRateLimitPutRequest(bucketName, rateLimitConfig);
            bucketPutRateLimit(rateLimitServiceAuthInfo, putRequest, log, err => {
                assert.ifError(err);
                return metadata.getBucket(bucketName, log, (err, bucket) => {
                    assert.ifError(err);
                    const bucketRateLimitConfig = bucket.getRateLimitConfiguration();
                    const configData = bucketRateLimitConfig.getData();
                    assert.strictEqual(configData.RequestsPerSecond.Limit, 0);
                    done();
                });
            });
        });
    });

    describe('after rate limit configuration has been set', () => {
        beforeEach(done => {
            bucketPut(regularAuthInfo, bucketPutReq, log, err => {
                assert.ifError(err);
                const rateLimitConfig = { RequestsPerSecond: 150 };
                const putRequest = getRateLimitPutRequest(bucketName, rateLimitConfig);
                bucketPutRateLimit(rateLimitServiceAuthInfo, putRequest, log, err => {
                    assert.ifError(err);
                    done();
                });
            });
        });

        it('should be retrievable via bucketGetRateLimit', done => {
            const getRequest = {
                bucketName,
                headers: {
                    host: `${bucketName}.s3.amazonaws.com`,
                },
                url: '/?rate-limit',
                method: 'GET',
                actionImplicitDenies: false,
            };
            bucketGetRateLimit(rateLimitServiceAuthInfo, getRequest, log, (err, res) => {
                assert.ifError(err);
                const rateLimitConfig = JSON.parse(res);
                assert.strictEqual(rateLimitConfig.RequestsPerSecond.Limit, 150);
                done();
            });
        });
    });
});
