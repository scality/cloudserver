const assert = require('assert');
const { errors } = require('arsenal');

const { getObjMetadataAndDelete }
    = require('../../../lib/api/multiObjectDelete');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const { metadata } = require('arsenal').storage.metadata.inMemory.metadata;
const { ds } = require('../../../lib/data/in_memory/backend');
const DummyRequest = require('../DummyRequest');
const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = Buffer.from('I am a body', 'utf8');
const contentLength = 2 * postBody.length;
const objectKey1 = 'objectName1';
const objectKey2 = 'objectName2';
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
                    assert.deepStrictEqual(ds, [ , , ]);
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
});
