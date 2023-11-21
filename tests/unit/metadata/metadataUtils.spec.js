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

const { validateBucket, metadataGetObjects, metadataGetObject } = require('../../../lib/metadata/metadataUtils');
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
