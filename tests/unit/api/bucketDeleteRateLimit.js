const assert = require('assert');
const sinon = require('sinon');
const { bucketPut } = require('../../../lib/api/bucketPut');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const bucketDeleteRateLimit = require('../../../lib/api/bucketDeleteRateLimit');
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

function getRateLimitDeleteRequest(bucketName) {
    return {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
            origin: 'http://example.com',
        },
        url: '/?rate-limit',
        method: 'DELETE',
        actionImplicitDenies: false,
    };
}

function getRateLimitPutRequest(bucketName, configJson) {
    return {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
        },
        url: '/?rate-limit',
        method: 'PUT',
        post: JSON.stringify(configJson),
        actionImplicitDenies: false,
    };
}

describe('bucketDeleteRateLimit API', () => {
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
            const deleteRequest = getRateLimitDeleteRequest(bucketName);
            bucketDeleteRateLimit(regularAuthInfo, deleteRequest, log, err => {
                assert.strictEqual(err.is.AccessDenied, true);
                done();
            });
        });
    });

    it('should return NoSuchBucket error if bucket does not exist', done => {
        const deleteRequest = getRateLimitDeleteRequest('nonexistent-bucket');
        bucketDeleteRateLimit(rateLimitServiceAuthInfo, deleteRequest, log, err => {
            assert(err, 'should return an error');
            assert.strictEqual(err.is.NoSuchBucket, true);
            done();
        });
    });

    it('should not return an error even if no rate limit config exists', done => {
        bucketPut(regularAuthInfo, bucketPutReq, log, err => {
            assert.ifError(err);
            const deleteRequest = getRateLimitDeleteRequest(bucketName);
            bucketDeleteRateLimit(rateLimitServiceAuthInfo, deleteRequest, log, err => {
                assert.ifError(err);
                done();
            });
        });
    });
});

describe('bucketDeleteRateLimit API with existing rate limit config', () => {
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

    it('should delete bucket rate limit configuration', done => {
        bucketPut(regularAuthInfo, bucketPutReq, log, err => {
            assert.ifError(err);
            const rateLimitConfig = { RequestsPerSecond: 100 };
            const putRequest = getRateLimitPutRequest(bucketName, rateLimitConfig);
            bucketPutRateLimit(rateLimitServiceAuthInfo, putRequest, log, err => {
                assert.ifError(err);
                const deleteRequest = getRateLimitDeleteRequest(bucketName);
                bucketDeleteRateLimit(rateLimitServiceAuthInfo, deleteRequest, log, err => {
                    assert.ifError(err);
                    return metadata.getBucket(bucketName, log, (err, bucket) => {
                        assert.ifError(err);
                        const bucketRateLimitConfig = bucket.getRateLimitConfiguration();
                        assert.strictEqual(bucketRateLimitConfig, undefined);
                        done();
                    });
                });
            });
        });
    });

    it('should delete rate limit configuration and verify via bucketGetRateLimit', done => {
        bucketPut(regularAuthInfo, bucketPutReq, log, err => {
            assert.ifError(err);
            const rateLimitConfig = { RequestsPerSecond: 150 };
            const putRequest = getRateLimitPutRequest(bucketName, rateLimitConfig);
            bucketPutRateLimit(rateLimitServiceAuthInfo, putRequest, log, err => {
                assert.ifError(err);
                const deleteRequest = getRateLimitDeleteRequest(bucketName);
                bucketDeleteRateLimit(rateLimitServiceAuthInfo, deleteRequest, log, err => {
                    assert.ifError(err);
                    const getRequest = {
                        bucketName,
                        headers: {
                            host: `${bucketName}.s3.amazonaws.com`,
                        },
                        url: '/?rate-limit',
                        method: 'GET',
                        actionImplicitDenies: false,
                    };
                    bucketGetRateLimit(rateLimitServiceAuthInfo, getRequest, log, err => {
                        assert(err, 'should return an error');
                        assert.strictEqual(err.is.NoSuchRateLimitConfig, true);
                        done();
                    });
                });
            });
        });
    });
});
