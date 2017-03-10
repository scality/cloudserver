import assert from 'assert';
import { errors } from 'arsenal';

import { getObjMetadataAndDelete } from '../../../lib/api/multiObjectDelete';
import { cleanup, DummyRequestLogger, makeAuthInfo } from '../helpers';
import { metadata } from '../../../lib/metadata/in_memory/metadata';
import { ds } from '../../../lib/data/in_memory/backend';
import DummyRequest from '../DummyRequest';
import bucketPut from '../../../lib/api/bucketPut';
import objectPut from '../../../lib/api/objectPut';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = Buffer.from('I am a body', 'utf8');
const contentLength = 2 * postBody.length;
const objectKey1 = 'objectName1';
const objectKey2 = 'objectName2';
const locationConstraint = 'us-east-1';
const testBucketPutRequest = new DummyRequest({
    bucketName,
    namespace,
    headers: {},
    url: `/${bucketName}`,
});

describe('getObjMetadataAndDelete function for multiObjectDelete', () => {
    let testPutObjectRequest1;
    let testPutObjectRequest2;

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
        bucketPut(authInfo, testBucketPutRequest, locationConstraint,
            log, () => {
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
        getObjMetadataAndDelete(bucketName, true,
            [], [objectKey1, objectKey2], log,
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
        getObjMetadataAndDelete(bucketName, true,
            [], ['madeup1', 'madeup2'], log,
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
        getObjMetadataAndDelete('madeupbucket', true,
            [], [objectKey1, objectKey2], log,
            (err, quietSetting, errorResults, numOfObjects,
                successfullyDeleted, totalContentLengthDeleted) => {
                assert.ifError(err);
                assert.strictEqual(quietSetting, true);
                assert.deepStrictEqual(errorResults, [
                    {
                        key: objectKey1,
                        error: errors.NoSuchBucket,
                    },
                    {
                        key: objectKey2,
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
            getObjMetadataAndDelete(bucketName, true,
                [], [], log,
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
        getObjMetadataAndDelete(bucketName, true,
            errorResultsSample, [objectKey1, objectKey2], log,
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
