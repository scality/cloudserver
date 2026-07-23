const assert = require('assert');
const sinon = require('sinon');

const { errors, models } = require('arsenal');
const { BucketInfo } = models;
const { DummyRequestLogger, makeAuthInfo } = require('../helpers');

const creationDate = new Date().toJSON();
const authInfo = makeAuthInfo('accessKey');
const otherAuthInfo = makeAuthInfo('otherAccessKey');
const ownerCanonicalId = authInfo.getCanonicalID();

const bucket = new BucketInfo('niftyBucket', ownerCanonicalId, authInfo.getAccountDisplayName(), creationDate);
const log = new DummyRequestLogger();

const {
    validateBucket,
    metadataGetObjects,
    metadataGetObject,
    storeServerAccessLogInfo,
    standardMetadataValidateBucket,
} = require('../../../lib/metadata/metadataUtils');
const metadata = require('../../../lib/metadata/wrapper');
const { config } = require('../../../lib/Config');
const constants = require('../../../constants');
const rateLimitCache = require('../../../lib/api/apiUtils/rateLimit/cache');
const tokenBucket = require('../../../lib/api/apiUtils/rateLimit/tokenBucket');

describe('validateBucket', () => {
    it('action bucketPutPolicy by bucket owner', () => {
        const validationResult = validateBucket(
            bucket,
            {
                authInfo,
                requestType: 'bucketPutPolicy',
                request: null,
            },
            log,
            false,
        );
        assert.ifError(validationResult);
    });
    it('action bucketPutPolicy by other than bucket owner', () => {
        const validationResult = validateBucket(
            bucket,
            {
                authInfo: otherAuthInfo,
                requestType: 'bucketPutPolicy',
                request: null,
            },
            log,
            false,
        );
        assert(validationResult);
        assert(validationResult.is.MethodNotAllowed);
    });

    it('action bucketGet by bucket owner', () => {
        const validationResult = validateBucket(
            bucket,
            {
                authInfo,
                requestType: 'bucketGet',
                request: null,
            },
            log,
            false,
        );
        assert.ifError(validationResult);
    });

    it('action bucketGet by other than bucket owner', () => {
        const validationResult = validateBucket(
            bucket,
            {
                authInfo: otherAuthInfo,
                requestType: 'bucketGet',
                request: null,
            },
            log,
            false,
        );
        assert(validationResult);
        assert(validationResult.is.AccessDenied);
    });
});

describe('metadataGetObjects', () => {
    let sandbox;
    const objectsKeys = [
        { inPlay: { key: 'objectKey1' }, versionId: 'versionId1' },
        { inPlay: { key: 'objectKey2' }, versionId: 'versionId2' },
    ];

    beforeEach(() => {
        sandbox = sinon.createSandbox();
    });

    afterEach(() => {
        sandbox.restore();
    });

    it('should return error if metadata.getObjectsMD fails', done => {
        const error = new Error('Failed to get object metadata');
        sandbox.stub(metadata, 'getObjectsMD').yields(error);

        metadataGetObjects('bucketName', objectsKeys, log, err => {
            assert(err);
            assert.strictEqual(err, error);
            done();
        });
    });

    it('should return object metadata if successful', done => {
        const metadataObjs = [
            { doc: { key: 'objectKey1' }, versionId: 'versionId1' },
            { doc: { key: 'objectKey2' }, versionId: 'versionId2' },
        ];
        sandbox.stub(metadata, 'getObjectsMD').yields(null, metadataObjs);

        metadataGetObjects('bucketName', objectsKeys, log, (err, result) => {
            assert.ifError(err);
            assert(result);
            assert.strictEqual(result.objectKey1versionId1, metadataObjs[0].doc);
            assert.strictEqual(result.objectKey2versionId2, metadataObjs[1].doc);
            done();
        });
    });
});

describe('metadataGetObject', () => {
    let sandbox;
    const objectKey = { inPlay: { key: 'objectKey1' }, versionId: 'versionId1' };

    beforeEach(() => {
        sandbox = sinon.createSandbox();
    });

    afterEach(() => {
        sandbox.restore();
    });

    it('should return the cached document if provided', done => {
        const cachedDoc = {
            [objectKey.inPlay.key]: {
                key: 'objectKey1',
                versionId: 'versionId1',
            },
        };
        metadataGetObject('bucketName', objectKey.inPlay.key, objectKey.versionId, cachedDoc, log, (err, result) => {
            assert.ifError(err);
            assert.deepStrictEqual(result, cachedDoc[objectKey.inPlay.key]);
            done();
        });
    });

    it('should return error if metadata.getObjectMD fails', done => {
        const error = new Error('Failed to get object metadata');
        sandbox.stub(metadata, 'getObjectMD').yields(error);

        metadataGetObject('bucketName', objectKey.inPlay.key, objectKey.versionId, null, log, err => {
            assert(err);
            assert.strictEqual(err, error);
            done();
        });
    });

    it('should return object metadata if successful', done => {
        const metadataObj = { doc: { key: 'objectKey1', versionId: 'versionId1' } };
        sandbox.stub(metadata, 'getObjectMD').yields(null, metadataObj);

        metadataGetObject('bucketName', objectKey.inPlay.key, objectKey.versionId, null, log, (err, result) => {
            assert.ifError(err);
            assert.deepStrictEqual(result, metadataObj);
            done();
        });
    });
});

describe('storeServerAccessLogInfo - copySource aclRequired', () => {
    it('should move source aclRequired to sourceServerAccessLog and restore destination value', () => {
        // Destination auth set aclRequired='Yes', then source auth ran on the
        // same request object and did not set aclRequired (owner on source).
        const request = {
            serverAccessLog: {},
        };
        const options = {
            copySource: true,
            savedAclRequired: 'Yes',
        };
        storeServerAccessLogInfo(request, null, null, options);
        assert.strictEqual(request.sourceServerAccessLog.aclRequired, undefined);
        assert.strictEqual(request.serverAccessLog.aclRequired, 'Yes');
    });

    it('should swap aclRequired when source auth also required ACL check', () => {
        // Destination auth did not set aclRequired (owner on dest), then
        // source auth set aclRequired='Yes' on the same request object.
        const request = {
            serverAccessLog: { aclRequired: 'Yes' },
        };
        const options = {
            copySource: true,
            savedAclRequired: undefined,
        };
        storeServerAccessLogInfo(request, null, null, options);
        assert.strictEqual(request.sourceServerAccessLog.aclRequired, 'Yes');
        assert.strictEqual(request.serverAccessLog.aclRequired, undefined);
    });
});

describe('checkRateLimitIfNeeded cross-account rate limiting', () => {
    let sandbox;
    let request;

    const otherCanonicalId = otherAuthInfo.getCanonicalID();

    beforeEach(() => {
        sandbox = sinon.createSandbox();
        sandbox.stub(config, 'rateLimiting').value({
            enabled: true,
            serviceUserArn: 'arn:aws:iam::000000000000:user/rate-limit-service-user',
            nodes: 1,
            tokenBucketBufferSize: 50,
            tokenBucketRefillThreshold: 20,
            error: errors.SlowDown,
            bucket: {
                configCacheTTL: 30000,
                defaultConfig: { RequestsPerSecond: { BurstCapacity: 1 } },
            },
            account: {
                configCacheTTL: 30000,
                defaultConfig: { RequestsPerSecond: { BurstCapacity: 1 } },
            },
        });
        sandbox.stub(metadata, 'getBucket').yields(null, bucket);
        rateLimitCache.configCache.clear();
        rateLimitCache.bucketOwnerCache.clear();
        tokenBucket.getAllTokenBuckets().clear();
        request = { apiMethod: 'bucketGet', bucketName: bucket.getName() };
    });

    afterEach(() => {
        sandbox.restore();
        rateLimitCache.configCache.clear();
        rateLimitCache.bucketOwnerCache.clear();
        tokenBucket.getAllTokenBuckets().clear();
    });

    // Rate limiting runs before bucket authorization, so the requester may
    // still get AccessDenied from validateBucket afterwards; these tests
    // assert on the rate limit side effects, not the final auth outcome.
    function validateBucketRequest(requesterAuthInfo, cb) {
        standardMetadataValidateBucket(
            {
                authInfo: requesterAuthInfo,
                bucketName: bucket.getName(),
                requestType: 'bucketGet',
                request,
            },
            false,
            log,
            cb,
        );
    }

    it('should cache the bucket owner even when the bucket config was already checked', done => {
        request.rateLimitBucketAlreadyChecked = true;
        validateBucketRequest(authInfo, err => {
            assert.ifError(err);
            assert.strictEqual(rateLimitCache.getCachedBucketOwner(bucket.getName()), ownerCanonicalId);
            done();
        });
    });

    it('should key the account rate limit under the requester canonical ID by default', done => {
        request.accountLimits = { RequestsPerSecond: { Limit: 100 } };
        validateBucketRequest(authInfo, err => {
            assert.ifError(err);
            const cached = rateLimitCache.getCachedConfig(rateLimitCache.namespace.account, ownerCanonicalId);
            assert.deepStrictEqual(cached, {
                RequestsPerSecond: { BurstCapacity: 1, Limit: 100, source: 'resource' },
            });
            assert(tokenBucket.getAllTokenBuckets().has(`account:${ownerCanonicalId}:rps`));
            assert.strictEqual(request.rateLimitAccountAlreadyChecked, true);
            done();
        });
    });

    it('should key the account rate limit under the target account when the request carries one', done => {
        request.accountLimits = { RequestsPerSecond: { Limit: 100 } };
        request.rateLimitTargetAccount = ownerCanonicalId;
        validateBucketRequest(otherAuthInfo, () => {
            assert(rateLimitCache.getCachedConfig(rateLimitCache.namespace.account, ownerCanonicalId));
            assert.strictEqual(
                rateLimitCache.getCachedConfig(rateLimitCache.namespace.account, otherCanonicalId),
                undefined,
            );
            assert(tokenBucket.getAllTokenBuckets().has(`account:${ownerCanonicalId}:rps`));
            assert(!tokenBucket.getAllTokenBuckets().has(`account:${otherCanonicalId}:rps`));
            done();
        });
    });

    it('should deny a cross-account request when the target account limit is exhausted', done => {
        request.accountLimits = { RequestsPerSecond: { Limit: 100 } };
        request.rateLimitTargetAccount = ownerCanonicalId;
        const ownerBucket = tokenBucket.getTokenBucket(
            'account',
            ownerCanonicalId,
            'rps',
            { limit: 100, burstCapacity: 1000 },
            log,
        );
        ownerBucket.tokens = 0;
        validateBucketRequest(otherAuthInfo, err => {
            assert(err);
            assert(err.is.SlowDown);
            done();
        });
    });

    it('should rate limit against the requester account when no target account is present', done => {
        request.accountLimits = { RequestsPerSecond: { Limit: 100 } };
        const ownerBucket = tokenBucket.getTokenBucket(
            'account',
            ownerCanonicalId,
            'rps',
            { limit: 100, burstCapacity: 1000 },
            log,
        );
        ownerBucket.tokens = 0;
        validateBucketRequest(otherAuthInfo, err => {
            // The exhausted owner account bucket must not affect the request:
            // the requester is limited against their own account.
            assert(!err || !err.is.SlowDown);
            assert(tokenBucket.getAllTokenBuckets().has(`account:${otherCanonicalId}:rps`));
            done();
        });
    });

    it('should skip the account rate limit check for public requesters', done => {
        const publicAuthInfo = makeAuthInfo(constants.publicId);
        request.accountLimits = { RequestsPerSecond: { Limit: 100 } };
        validateBucketRequest(publicAuthInfo, () => {
            assert.strictEqual(request.rateLimitAccountAlreadyChecked, undefined);
            assert.strictEqual(
                rateLimitCache.getCachedConfig(rateLimitCache.namespace.account, constants.publicId),
                undefined,
            );
            // The bucket owner is still cached for later cross-account attribution
            assert.strictEqual(rateLimitCache.getCachedBucketOwner(bucket.getName()), ownerCanonicalId);
            done();
        });
    });

    it('should skip the account rate limit check for public requesters even with a target account', done => {
        const publicAuthInfo = makeAuthInfo(constants.publicId);
        request.accountLimits = { RequestsPerSecond: { Limit: 100 } };
        request.rateLimitTargetAccount = ownerCanonicalId;
        validateBucketRequest(publicAuthInfo, () => {
            assert.strictEqual(request.rateLimitAccountAlreadyChecked, undefined);
            assert.strictEqual(
                rateLimitCache.getCachedConfig(rateLimitCache.namespace.account, ownerCanonicalId),
                undefined,
            );
            done();
        });
    });
});
