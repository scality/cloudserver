const assert = require('assert');
const sinon = require('sinon');
const { bucketPut } = require('../../../lib/api/bucketPut');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const bucketGetRateLimit = require('../../../lib/api/bucketGetRateLimit');
const bucketPutRateLimit = require('../../../lib/api/bucketPutRateLimit');
const { config } = require('../../../lib/Config');
const AuthInfo = require('arsenal').auth.AuthInfo;

const log = new DummyRequestLogger();
const bucketName = 'bucketname';
const serviceUserArn = 'arn:aws:iam::123456789012:user/rate-limit-service';

// Create a rate limit service user authInfo
function makeRateLimitServiceUserAuthInfo() {
    return new AuthInfo({
        canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
        shortid: '123456789012',
        email: 'ratelimit@service.com',
        accountDisplayName: 'rateLimitServiceDisplayName',
        arn: serviceUserArn,
    });
}

const rateLimitServiceAuthInfo = makeRateLimitServiceUserAuthInfo();
const regularAuthInfo = makeAuthInfo('accessKey1');

const bucketPutReq = {
    bucketName,
    headers: {
        host: `${bucketName}.s3.amazonaws.com`,
    },
    url: '/',
    actionImplicitDenies: false,
};

function getRateLimitConfigRequest(bucketName) {
    return {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
        },
        url: '/?rate-limit',
        method: 'GET',
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

describe('bucketGetRateLimit API', () => {
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
            const rateLimitRequest = getRateLimitConfigRequest(bucketName);
            bucketGetRateLimit(regularAuthInfo, rateLimitRequest, log, err => {
                assert.strictEqual(err.is.AccessDenied, true);
                done();
            });
        });
    });

    it('should return NoSuchRateLimitConfig error if bucket exists but ' + 'rate limit config is not set', done => {
        bucketPut(regularAuthInfo, bucketPutReq, log, err => {
            assert.ifError(err);
            const rateLimitRequest = getRateLimitConfigRequest(bucketName);
            bucketGetRateLimit(rateLimitServiceAuthInfo, rateLimitRequest, log, err => {
                assert.strictEqual(err.is.NoSuchRateLimitConfig, true);
                done();
            });
        });
    });

    it('should return bucket not found error if bucket does not exist', done => {
        const rateLimitRequest = getRateLimitConfigRequest('nonexistent-bucket');
        bucketGetRateLimit(rateLimitServiceAuthInfo, rateLimitRequest, log, err => {
            assert(err, 'should return an error');
            assert.strictEqual(err.is.NoSuchBucket, true);
            done();
        });
    });
});

describe('bucketGetRateLimit API with rate limit config', () => {
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

    beforeEach(done => {
        bucketPut(regularAuthInfo, bucketPutReq, log, err => {
            assert.ifError(err);
            const rateLimitConfig = { RequestsPerSecond: 100 };
            const putRequest = getRateLimitPutRequest(bucketName, rateLimitConfig);
            bucketPutRateLimit(rateLimitServiceAuthInfo, putRequest, log, err => {
                assert.ifError(err);
                done();
            });
        });
    });

    it('should return rate limit configuration JSON when config exists', done => {
        const rateLimitRequest = getRateLimitConfigRequest(bucketName);
        bucketGetRateLimit(rateLimitServiceAuthInfo, rateLimitRequest, log, (err, res) => {
            assert.ifError(err);
            const rateLimitConfig = JSON.parse(res);
            assert.strictEqual(rateLimitConfig.RequestsPerSecond.Limit, 100);
            done();
        });
    });
});
