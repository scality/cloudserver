const assert = require('assert');
const sinon = require('sinon');

const { models } = require('arsenal');
const { BucketInfo } = models;
const { DummyRequestLogger, makeAuthInfo } = require('../helpers');

const creationDate = new Date().toJSON();
const authInfo = makeAuthInfo('accessKey');
const otherAuthInfo = makeAuthInfo('otherAccessKey');
const ownerCanonicalId = authInfo.getCanonicalID();

const bucket = new BucketInfo('niftyBucket', ownerCanonicalId,
    authInfo.getAccountDisplayName(), creationDate);
const log = new DummyRequestLogger();

const {
    validateBucket,
    metadataGetObjects,
    metadataGetObject,
    storeServerAccessLogInfo,
} = require('../../../lib/metadata/metadataUtils');
const metadata = require('../../../lib/metadata/wrapper');

describe('validateBucket', () => {
    it('action bucketPutPolicy by bucket owner', () => {
        const validationResult = validateBucket(bucket, {
            authInfo,
            requestType: 'bucketPutPolicy',
            request: null,
        }, log, false);
        assert.ifError(validationResult);
    });
    it('action bucketPutPolicy by other than bucket owner', () => {
        const validationResult = validateBucket(bucket, {
            authInfo: otherAuthInfo,
            requestType: 'bucketPutPolicy',
            request: null,
        }, log, false);
        assert(validationResult);
        assert(validationResult.is.MethodNotAllowed);
    });

    it('action bucketGet by bucket owner', () => {
        const validationResult = validateBucket(bucket, {
            authInfo,
            requestType: 'bucketGet',
            request: null,
        }, log, false);
        assert.ifError(validationResult);
    });

    it('action bucketGet by other than bucket owner', () => {
        const validationResult = validateBucket(bucket, {
            authInfo: otherAuthInfo,
            requestType: 'bucketGet',
            request: null,
        }, log, false);
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
                key: 'objectKey1', versionId: 'versionId1',
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
