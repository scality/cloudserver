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
const vault = require('../../../lib/auth/vault');

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
    let vaultStub;

    const otherCanonicalId = otherAuthInfo.getCanonicalID();

    beforeEach(() => {
        sandbox = sinon.createSandbox();
        // Vault is consulted for the bucket owner's limits whenever they could
        // not have been returned by the request's own authentication.
        vaultStub = sandbox.stub(vault, 'getAccountLimitsByCanonicalId').yields(null, undefined);
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

    it('should key the account rate limit under the bucket owner for a same-account request', done => {
        request.rateLimitTargetAccountLimits = { RequestsPerSecond: { Limit: 100 } };
        validateBucketRequest(authInfo, err => {
            assert.ifError(err);
            const cached = rateLimitCache.getCachedConfig(rateLimitCache.namespace.account, ownerCanonicalId);
            assert.deepStrictEqual(cached, {
                RequestsPerSecond: { BurstCapacity: 1, Limit: 100, source: 'resource' },
            });
            assert(tokenBucket.getAllTokenBuckets().has(`account:${ownerCanonicalId}:rps`));
            assert.strictEqual(request.rateLimitAccountAlreadyChecked, true);
            // The requester is the owner, so their own auth already returned the limits
            assert.strictEqual(vaultStub.called, false);
            done();
        });
    });

    it('should key the account rate limit under the target account when the request carries one', done => {
        request.rateLimitTargetAccountLimits = { RequestsPerSecond: { Limit: 100 } };
        request.rateLimitTargetAccount = ownerCanonicalId;
        validateBucketRequest(otherAuthInfo, () => {
            assert(rateLimitCache.getCachedConfig(rateLimitCache.namespace.account, ownerCanonicalId));
            assert.strictEqual(
                rateLimitCache.getCachedConfig(rateLimitCache.namespace.account, otherCanonicalId),
                undefined,
            );
            assert(tokenBucket.getAllTokenBuckets().has(`account:${ownerCanonicalId}:rps`));
            assert(!tokenBucket.getAllTokenBuckets().has(`account:${otherCanonicalId}:rps`));
            assert.strictEqual(vaultStub.called, false);
            done();
        });
    });

    it('should deny a cross-account request when the target account limit is exhausted', done => {
        request.rateLimitTargetAccountLimits = { RequestsPerSecond: { Limit: 100 } };
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

    it('should fetch the owner limits from Vault when no target account is present', done => {
        // Cross-account request with a bucket owner cache miss: the requester's
        // own auth returned their limits, not the owner's, so Vault is asked.
        vaultStub.yields(null, { RequestsPerSecond: { Limit: 100 } });
        validateBucketRequest(otherAuthInfo, () => {
            assert.strictEqual(vaultStub.calledOnce, true);
            assert.strictEqual(vaultStub.firstCall.args[0], ownerCanonicalId);
            assert.deepStrictEqual(rateLimitCache.getCachedConfig(rateLimitCache.namespace.account, ownerCanonicalId), {
                RequestsPerSecond: { BurstCapacity: 1, Limit: 100, source: 'resource' },
            });
            assert(tokenBucket.getAllTokenBuckets().has(`account:${ownerCanonicalId}:rps`));
            assert(!tokenBucket.getAllTokenBuckets().has(`account:${otherCanonicalId}:rps`));
            done();
        });
    });

    it('should rate limit a cross-account request against the owner, not the requester', done => {
        vaultStub.yields(null, { RequestsPerSecond: { Limit: 100 } });
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

    it('should rate limit public requesters against the bucket owner', done => {
        const publicAuthInfo = makeAuthInfo(constants.publicId);
        vaultStub.yields(null, { RequestsPerSecond: { Limit: 100 } });
        validateBucketRequest(publicAuthInfo, () => {
            // Anonymous requests never authenticate an account, so the owner's
            // limits always come from Vault.
            assert.strictEqual(vaultStub.calledOnce, true);
            assert.strictEqual(vaultStub.firstCall.args[0], ownerCanonicalId);
            assert.strictEqual(request.rateLimitAccountAlreadyChecked, true);
            assert.deepStrictEqual(rateLimitCache.getCachedConfig(rateLimitCache.namespace.account, ownerCanonicalId), {
                RequestsPerSecond: { BurstCapacity: 1, Limit: 100, source: 'resource' },
            });
            assert.strictEqual(
                rateLimitCache.getCachedConfig(rateLimitCache.namespace.account, constants.publicId),
                undefined,
            );
            assert(tokenBucket.getAllTokenBuckets().has(`account:${ownerCanonicalId}:rps`));
            assert(!tokenBucket.getAllTokenBuckets().has(`account:${constants.publicId}:rps`));
            assert.strictEqual(rateLimitCache.getCachedBucketOwner(bucket.getName()), ownerCanonicalId);
            done();
        });
    });

    it('should deny a public request when the bucket owner limit is exhausted', done => {
        const publicAuthInfo = makeAuthInfo(constants.publicId);
        vaultStub.yields(null, { RequestsPerSecond: { Limit: 100 } });
        const ownerBucket = tokenBucket.getTokenBucket(
            'account',
            ownerCanonicalId,
            'rps',
            { limit: 100, burstCapacity: 1000 },
            log,
        );
        ownerBucket.tokens = 0;
        validateBucketRequest(publicAuthInfo, err => {
            assert(err);
            assert(err.is.SlowDown);
            done();
        });
    });

    it('should refetch the owner limits for a public requester even with a target account', done => {
        // The cached bucket owner made doAuth return the owner's limits, but an
        // anonymous request has no account auth, so Vault is still consulted.
        const publicAuthInfo = makeAuthInfo(constants.publicId);
        request.rateLimitTargetAccount = ownerCanonicalId;
        request.rateLimitTargetAccountLimits = { RequestsPerSecond: { Limit: 5 } };
        vaultStub.yields(null, { RequestsPerSecond: { Limit: 100 } });
        validateBucketRequest(publicAuthInfo, () => {
            assert.strictEqual(vaultStub.calledOnce, true);
            assert.deepStrictEqual(rateLimitCache.getCachedConfig(rateLimitCache.namespace.account, ownerCanonicalId), {
                RequestsPerSecond: { BurstCapacity: 1, Limit: 100, source: 'resource' },
            });
            done();
        });
    });

    it('should fall back to the global account defaults when Vault returns no limits', done => {
        const publicAuthInfo = makeAuthInfo(constants.publicId);
        vaultStub.yields(null, undefined);
        validateBucketRequest(publicAuthInfo, () => {
            assert.deepStrictEqual(rateLimitCache.getCachedConfig(rateLimitCache.namespace.account, ownerCanonicalId), {
                RequestsPerSecond: { BurstCapacity: 1, source: 'global' },
            });
            // No Limit in the global defaults, so no token bucket is created
            assert(!tokenBucket.getAllTokenBuckets().has(`account:${ownerCanonicalId}:rps`));
            done();
        });
    });

    it('should propagate a Vault error to the callback', done => {
        vaultStub.yields(errors.InternalError);
        validateBucketRequest(otherAuthInfo, err => {
            assert(err);
            assert(err.is.InternalError);
            done();
        });
    });

    it('should still cache the bucket owner when the account config was already checked', done => {
        request.rateLimitAccountAlreadyChecked = true;
        validateBucketRequest(authInfo, err => {
            assert.ifError(err);
            assert.strictEqual(rateLimitCache.getCachedBucketOwner(bucket.getName()), ownerCanonicalId);
            assert.strictEqual(
                rateLimitCache.getCachedConfig(rateLimitCache.namespace.account, ownerCanonicalId),
                undefined,
            );
            done();
        });
    });
});
