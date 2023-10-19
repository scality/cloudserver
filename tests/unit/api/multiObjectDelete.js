const assert = require('assert');
const { errors, storage } = require('arsenal');

const { decodeObjectVersion, getObjMetadataAndDelete, initializeMultiObjectDeleteWithBatchingSupport }
    = require('../../../lib/api/multiObjectDelete');
const multiObjectDelete = require('../../../lib/api/multiObjectDelete');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const DummyRequest = require('../DummyRequest');
const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');
const log = new DummyRequestLogger();

const { metadata } = storage.metadata.inMemory.metadata;
const { ds } = storage.data.inMemory.datastore;
const metadataswitch = require('../metadataswitch');
const sinon = require('sinon');

const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = Buffer.from('I am a body', 'utf8');
const contentLength = 2 * postBody.length;
const objectKey1 = 'objectName1';
const objectKey2 = 'objectName2';
const metadataUtils = require('../../../lib/metadata/metadataUtils');
const services = require('../../../lib/services');
const testBucketPutRequest = new DummyRequest({
    bucketName,
    namespace,
    headers: {},
    url: `/${bucketName}`,
});

describe('getObjMetadataAndDelete function for multiObjectDelete', () => {
    let testPutObjectRequest1;
    let testPutObjectRequest2;
    const request = new DummyRequest({
        headers: {},
        parsedContentLength: contentLength,
    }, postBody);
    const bucket = { getVersioningConfiguration: () => null };

    beforeEach(done => {
        cleanup();
        sinon.spy(metadataswitch, 'deleteObjectMD');
        testPutObjectRequest1 = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectKey1,
            headers: {},
            url: `/${bucketName}/${objectKey1}`,
        }, postBody);
        testPutObjectRequest2 = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectKey2,
            headers: {},
            url: `/${bucketName}/${objectKey2}`,
        }, postBody);
        bucketPut(authInfo, testBucketPutRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest1,
                undefined, log, () => {
                    objectPut(authInfo, testPutObjectRequest2,
                        undefined, log, () => {
                            assert.strictEqual(metadata.keyMaps
                                .get(bucketName)
                                .has(objectKey1), true);
                            assert.strictEqual(metadata.keyMaps
                                .get(bucketName)
                                .has(objectKey2), true);
                            done();
                        });
                });
        });
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should successfully get object metadata and then ' +
        'delete metadata and data', done => {
        getObjMetadataAndDelete(authInfo, 'foo', request, bucketName, bucket,
            true, [], [{ key: objectKey1 }, { key: objectKey2 }], log,
            (err, quietSetting, errorResults, numOfObjects,
                successfullyDeleted, totalContentLengthDeleted) => {
                assert.ifError(err);
                assert.strictEqual(quietSetting, true);
                assert.deepStrictEqual(errorResults, []);
                assert.strictEqual(numOfObjects, 2);
                assert.strictEqual(totalContentLengthDeleted, contentLength);
                assert.strictEqual(metadata.keyMaps.get(bucketName)
                    .has(objectKey1), false);
                assert.strictEqual(metadata.keyMaps.get(bucketName)
                    .has(objectKey2), false);
                // call to delete data is async so wait 20 ms to check
                // that data deleted
                setTimeout(() => {
                    // eslint-disable-next-line
                    assert.deepStrictEqual(ds, [ , , , ]);
                    done();
                }, 20);
            });
    });

    it('should return success results if no such key', done => {
        getObjMetadataAndDelete(authInfo, 'foo', request, bucketName, bucket,
            true, [], [{ key: 'madeup1' }, { key: 'madeup2' }], log,
            (err, quietSetting, errorResults, numOfObjects,
                successfullyDeleted, totalContentLengthDeleted) => {
                assert.ifError(err);
                assert.strictEqual(quietSetting, true);
                assert.deepStrictEqual(errorResults, []);
                assert.strictEqual(numOfObjects, 0);
                assert.strictEqual(totalContentLengthDeleted,
                    0);
                assert.strictEqual(metadata.keyMaps.get(bucketName)
                    .has(objectKey1), true);
                assert.strictEqual(metadata.keyMaps.get(bucketName)
                    .has(objectKey2), true);
                done();
            });
    });

    it('should return error results if err from metadata getting object' +
        'is error other than NoSuchKey', done => {
        // we fake an error by calling on an imaginary bucket
        // even though the getObjMetadataAndDelete function would
        // never be called if there was no bucket (would error out earlier
        // in API)
        getObjMetadataAndDelete(authInfo, 'foo', request, 'madeupbucket',
            bucket, true, [], [{ key: objectKey1 }, { key: objectKey2 }], log,
            (err, quietSetting, errorResults, numOfObjects,
                successfullyDeleted, totalContentLengthDeleted) => {
                assert.ifError(err);
                assert.strictEqual(quietSetting, true);
                assert.deepStrictEqual(errorResults, [
                    {
                        entry: { key: objectKey1 },
                        error: errors.NoSuchBucket,
                    },
                    {
                        entry: { key: objectKey2 },
                        error: errors.NoSuchBucket,
                    },
                ]);
                assert.strictEqual(totalContentLengthDeleted,
                    0);
                assert.strictEqual(metadata.keyMaps.get(bucketName)
                    .has(objectKey1), true);
                assert.strictEqual(metadata.keyMaps.get(bucketName)
                    .has(objectKey2), true);
                done();
            });
    });

    it('should return no error or success results if no objects in play',
        done => {
            getObjMetadataAndDelete(authInfo, 'foo', request, bucketName,
                bucket, true, [], [], log,
                (err, quietSetting, errorResults, numOfObjects,
                    successfullyDeleted, totalContentLengthDeleted) => {
                    assert.ifError(err);
                    assert.strictEqual(quietSetting, true);
                    assert.deepStrictEqual(errorResults, []);
                    assert.strictEqual(numOfObjects, 0);
                    assert.strictEqual(totalContentLengthDeleted,
                        0);
                    done();
                });
        });

    it('should pass along error results', done => {
        const errorResultsSample = [
            {
                key: 'somekey1',
                error: errors.AccessDenied,
            },
            {
                key: 'somekey2',
                error: errors.AccessDenied,
            },
        ];
        getObjMetadataAndDelete(authInfo, 'foo', request, bucketName, bucket,
            true, errorResultsSample,
            [{ key: objectKey1 }, { key: objectKey2 }], log,
            (err, quietSetting, errorResults, numOfObjects,
                successfullyDeleted, totalContentLengthDeleted) => {
                assert.ifError(err);
                assert.strictEqual(quietSetting, true);
                assert.deepStrictEqual(errorResults, errorResultsSample);
                assert.strictEqual(numOfObjects, 2);
                assert.strictEqual(totalContentLengthDeleted, contentLength);
                done();
            });
    });

    it('should properly batch delete data even if there are errors in other objects', done => {
        const deleteObjectStub = sinon.stub(services, 'deleteObject');
        deleteObjectStub.onCall(0).callsArgWith(7, errors.InternalError);
        deleteObjectStub.onCall(1).callsArgWith(7, null);

        getObjMetadataAndDelete(authInfo, 'foo', request, bucketName, bucket,
        true, [], [{ key: objectKey1 }, { key: objectKey2 }], log,
        (err, quietSetting, errorResults, numOfObjects,
            successfullyDeleted, totalContentLengthDeleted) => {
            assert.ifError(err);
            assert.strictEqual(quietSetting, true);
            assert.deepStrictEqual(errorResults, [
                {
                    entry: {
                        key: objectKey1,
                    },
                    error: errors.InternalError,
                },
            ]);
            assert.strictEqual(numOfObjects, 1);
            assert.strictEqual(totalContentLengthDeleted, contentLength / 2);
            // Expect still in memory as we stubbed the function
            assert.strictEqual(metadata.keyMaps.get(bucketName).has(objectKey1), true);
            assert.strictEqual(metadata.keyMaps.get(bucketName).has(objectKey2), true);
            // ensure object 2 only is in the list of successful deletions
            assert.strictEqual(successfullyDeleted.length, 1);
            assert.deepStrictEqual(successfullyDeleted[0].entry.key, objectKey2);
            return done();
        });
    });

    it('should pass overheadField to metadata', done => {
        getObjMetadataAndDelete(authInfo, 'foo', request, bucketName, bucket,
            true, [], [{ key: objectKey1 }, { key: objectKey2 }], log,
            (err, quietSetting, errorResults, numOfObjects) => {
                assert.ifError(err);
                assert.strictEqual(numOfObjects, 2);
                sinon.assert.calledWith(
                    metadataswitch.deleteObjectMD,
                    bucketName,
                    objectKey1,
                    sinon.match({ overheadField: sinon.match.array }),
                    sinon.match.any,
                    sinon.match.any
                );
                sinon.assert.calledWith(
                    metadataswitch.deleteObjectMD,
                    bucketName,
                    objectKey2,
                    sinon.match({ overheadField: sinon.match.array }),
                    sinon.match.any,
                    sinon.match.any
                );
                done();
            });
    });
});

describe('initializeMultiObjectDeleteWithBatchingSupport', () => {
    let bucketName;
    let inPlay;
    let log;
    let callback;

    beforeEach(() => {
        bucketName = 'myBucket';
        inPlay = { one: 'object1', two: 'object2' };
        log = {};
        callback = sinon.spy();
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should not throw if the decodeObjectVersion function fails', done => {
        const metadataGetObjectsStub = sinon.stub(metadataUtils, 'metadataGetObjects').yields(null, {});
        sinon.stub(multiObjectDelete, 'decodeObjectVersion').returns([new Error('decode error')]);

        initializeMultiObjectDeleteWithBatchingSupport(bucketName, inPlay, log, callback);

        assert.strictEqual(metadataGetObjectsStub.callCount, 1);
        sinon.assert.calledOnce(callback);
        assert.strictEqual(callback.getCall(0).args[0], null);
        assert.deepStrictEqual(callback.getCall(0).args[1], {});
        done();
    });

    it('should call the batching method if the backend supports it', done => {
        const metadataGetObjectsStub = sinon.stub(metadataUtils, 'metadataGetObjects').yields(null, {});
        const objectVersion = 'someVersionId';
        sinon.stub(multiObjectDelete, 'decodeObjectVersion').returns([null, objectVersion]);

        initializeMultiObjectDeleteWithBatchingSupport(bucketName, inPlay, log, callback);

        assert.strictEqual(metadataGetObjectsStub.callCount, 1);
        sinon.assert.calledOnce(callback);
        assert.strictEqual(callback.getCall(0).args[0], null);
        done();
    });

    it('should not return an error if the metadataGetObjects function fails', done => {
        const metadataGetObjectsStub =
            sinon.stub(metadataUtils, 'metadataGetObjects').yields(new Error('metadata error'), null);
        const objectVersion = 'someVersionId';
        sinon.stub(multiObjectDelete, 'decodeObjectVersion').returns([null, objectVersion]);

        initializeMultiObjectDeleteWithBatchingSupport(bucketName, inPlay, log, callback);

        assert.strictEqual(metadataGetObjectsStub.callCount, 1);
        sinon.assert.calledOnce(callback);
        assert.strictEqual(callback.getCall(0).args[0] instanceof Error, false);
        assert.deepStrictEqual(callback.getCall(0).args[1], {});
        done();
    });

    it('should populate the cache when the backend supports it', done => {
        const expectedOutput = {
            one: {
                value: 'object1',
            },
            two: {
                value: 'object2',
            },
        };
        const metadataGetObjectsStub = sinon.stub(metadataUtils, 'metadataGetObjects').yields(null, expectedOutput);
        const objectVersion = 'someVersionId';
        sinon.stub(multiObjectDelete, 'decodeObjectVersion').returns([null, objectVersion]);

        initializeMultiObjectDeleteWithBatchingSupport(bucketName, inPlay, log, callback);

        assert.strictEqual(metadataGetObjectsStub.callCount, 1);
        sinon.assert.calledOnce(callback);
        assert.strictEqual(callback.getCall(0).args[0], null);
        assert.deepStrictEqual(callback.getCall(0).args[1], expectedOutput);
        done();
    });
});

describe('decodeObjectVersion function helper', () => {
    it('should throw error for invalid version IDs', () => {
        const ret = decodeObjectVersion({
            versionId: '\0',
        });
        assert(ret[0].is.NoSuchVersion);
    });

    it('should return "null" for null versionId', () => {
        const ret = decodeObjectVersion({
            versionId: 'null',
        });
        assert.strictEqual(ret[0], null);
        assert.strictEqual(ret[1], 'null');
    });

    it('should return null error on success', () => {
        const ret = decodeObjectVersion({});
        assert.ifError(ret[0]);
        assert.deepStrictEqual(ret[1], undefined);
    });
});
