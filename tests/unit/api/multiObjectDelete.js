const assert = require('assert');
const { errors } = require('arsenal');

const { getObjMetadataAndDelete }
    = require('../../../lib/api/multiObjectDelete');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const { metadata } = require('../../../lib/metadata/in_memory/metadata');
const { ds } = require('../../../lib/data/in_memory/backend');
const DummyRequest = require('../DummyRequest');
const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');
const bucketPutACL = require('../../../lib/api/bucketPutACL');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const altCanonicalID = 'accessKey2';
const authInfo = makeAuthInfo(canonicalID);
const altAuthInfo = makeAuthInfo(altCanonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = Buffer.from('I am a body', 'utf8');
const contentLength = 2 * postBody.length;
const objectKey1 = 'objectName1';
const objectKey2 = 'objectName2';
const objectKey3 = 'objectName3';
const testBucketPutRequest = new DummyRequest({
    bucketName,
    namespace,
    headers: {},
    url: `/${bucketName}`,
});
const testPutObjectRequest1 = new DummyRequest({
    bucketName,
    namespace,
    objectKey: objectKey1,
    headers: {},
    url: `/${bucketName}/${objectKey1}`,
}, postBody);
const testPutObjectRequest2 = new DummyRequest({
    bucketName,
    namespace,
    objectKey: objectKey2,
    headers: {},
    url: `/${bucketName}/${objectKey2}`,
}, postBody);
const testPutObjectRequest3 = new DummyRequest({
    bucketName,
    namespace,
    objectKey: objectKey3,
    headers: {},
    url: `/${bucketName}/${objectKey3}`,
}, postBody);

describe('getObjMetadataAndDelete function for multiObjectDelete', () => {
    const request = new DummyRequest({
        headers: {},
        parsedContentLength: contentLength,
    }, postBody);
    const bucket = { getVersioningConfiguration: () => null };

    describe('bucket and objects belong to same account', () => {
        beforeEach(done => {
            cleanup();
            bucketPut(authInfo, testBucketPutRequest, log, () => {
                objectPut(authInfo, testPutObjectRequest1, undefined,
                log, () => {
                    objectPut(authInfo, testPutObjectRequest2, undefined,
                    log, () => {
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
            getObjMetadataAndDelete(authInfo, authInfo.getCanonicalID(),
            request, bucketName, bucket, true, [],
            [{ key: objectKey1 }, { key: objectKey2 }], log,
            (err, quietSetting, errorResults, deletedObjStats) => {
                assert.ifError(err);
                assert.strictEqual(quietSetting, true);
                assert.deepStrictEqual(errorResults, []);
                assert.strictEqual(deletedObjStats.requesterIsObjOwner.
                    numOfObjectsRemoved, 2);
                assert.strictEqual(deletedObjStats.requesterNotObjOwner.
                    numOfObjectsRemoved, 0);
                assert.strictEqual(deletedObjStats.requesterIsObjOwner.
                    totalContentLengthDeleted, contentLength);
                assert.strictEqual(deletedObjStats.requesterNotObjOwner.
                    totalContentLengthDeleted, 0);
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
            getObjMetadataAndDelete(authInfo, authInfo.getCanonicalID(),
            request, bucketName, bucket, true, [],
            [{ key: 'madeup1' }, { key: 'madeup2' }], log,
            (err, quietSetting, errorResults, deletedObjStats) => {
                assert.ifError(err);
                assert.strictEqual(quietSetting, true);
                assert.deepStrictEqual(errorResults, []);
                assert.strictEqual(deletedObjStats.requesterIsObjOwner.
                    numOfObjectsRemoved, 0);
                assert.strictEqual(deletedObjStats.requesterNotObjOwner.
                    numOfObjectsRemoved, 0);
                assert.strictEqual(deletedObjStats.requesterIsObjOwner.
                    totalContentLengthDeleted, 0);
                assert.strictEqual(deletedObjStats.requesterNotObjOwner.
                    totalContentLengthDeleted, 0);
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
            getObjMetadataAndDelete(authInfo, authInfo.getCanonicalID(),
            request, 'madeupbucket', bucket, true, [],
            [{ key: objectKey1 }, { key: objectKey2 }], log,
            (err, quietSetting, errorResults, deletedObjStats) => {
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
                assert.strictEqual(deletedObjStats.requesterIsObjOwner.
                    totalContentLengthDeleted, 0);
                assert.strictEqual(deletedObjStats.requesterNotObjOwner.
                    totalContentLengthDeleted, 0);
                assert.strictEqual(metadata.keyMaps.get(bucketName)
                    .has(objectKey1), true);
                assert.strictEqual(metadata.keyMaps.get(bucketName)
                    .has(objectKey2), true);
                done();
            });
        });

        it('should return no error or success results if no objects in play',
        done => {
            getObjMetadataAndDelete(authInfo, authInfo.getCanonicalID(),
            request, bucketName, bucket, true, [], [], log,
            (err, quietSetting, errorResults, deletedObjStats) => {
                assert.ifError(err);
                assert.strictEqual(quietSetting, true);
                assert.deepStrictEqual(errorResults, []);
                assert.strictEqual(deletedObjStats.requesterIsObjOwner.
                    numOfObjectsRemoved, 0);
                assert.strictEqual(deletedObjStats.requesterIsObjOwner.
                    numOfObjectsRemoved, 0);
                assert.strictEqual(deletedObjStats.requesterIsObjOwner.
                    totalContentLengthDeleted, 0);
                assert.strictEqual(deletedObjStats.requesterNotObjOwner.
                    totalContentLengthDeleted, 0);
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
            getObjMetadataAndDelete(authInfo, authInfo.getCanonicalID(),
            request, bucketName, bucket, true, errorResultsSample,
            [{ key: objectKey1 }, { key: objectKey2 }], log,
            (err, quietSetting, errorResults, deletedObjStats) => {
                assert.ifError(err);
                assert.strictEqual(quietSetting, true);
                assert.deepStrictEqual(errorResults, errorResultsSample);
                assert.strictEqual(deletedObjStats.requesterIsObjOwner.
                    numOfObjectsRemoved, 2);
                assert.strictEqual(deletedObjStats.requesterNotObjOwner.
                    numOfObjectsRemoved, 0);
                assert.strictEqual(deletedObjStats.requesterIsObjOwner.
                    totalContentLengthDeleted, contentLength);
                assert.strictEqual(deletedObjStats.requesterNotObjOwner.
                    totalContentLengthDeleted, 0);
                done();
            });
        });
    });

    describe('bucket and objects belong to different accounts', () => {
        beforeEach(done => {
            cleanup();
            bucketPut(authInfo, testBucketPutRequest, log, () => {
                const testACLRequest = {
                    bucketName,
                    namespace,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    post: '<AccessControlPolicy xmlns=' +
                            '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                          '<Owner>' +
                            `<ID>${authInfo.getCanonicalID()}</ID>` +
                            '<DisplayName>OwnerDisplayName</DisplayName>' +
                          '</Owner>' +
                          '<AccessControlList>' +
                            '<Grant>' +
                              '<Grantee xsi:type="CanonicalUser">' +
                                `<ID>${altAuthInfo.getCanonicalID()}</ID>` +
                                '<DisplayName>OwnerDisplayName</DisplayName>' +
                              '</Grantee>' +
                              '<Permission>WRITE</Permission>' +
                            '</Grant>' +
                          '</AccessControlList>' +
                        '</AccessControlPolicy>',
                    url: '/?acl',
                    query: { acl: '' },
                };
                bucketPutACL(authInfo, testACLRequest, log, () => {
                    objectPut(authInfo, testPutObjectRequest1, undefined,
                    log, () => {
                        objectPut(altAuthInfo, testPutObjectRequest2, undefined,
                        log, () => {
                            objectPut(altAuthInfo, testPutObjectRequest3,
                            undefined, log, () => {
                                assert.strictEqual(metadata.keyMaps
                                    .get(bucketName)
                                    .has(objectKey1), true);
                                assert.strictEqual(metadata.keyMaps
                                    .get(bucketName)
                                    .has(objectKey2), true);
                                assert.strictEqual(metadata.keyMaps
                                    .get(bucketName)
                                    .has(objectKey3), true);
                                done();
                            });
                        });
                    });
                });
            });
        });

        it('should successfully get object metadata and then ' +
        'delete metadata and data', done => {
            getObjMetadataAndDelete(authInfo, authInfo.getCanonicalID(),
            request, bucketName, bucket, true, [],
            [{ key: objectKey1 }, { key: objectKey2 }, { key: objectKey3 }],
            log, (err, quietSetting, errorResults, deletedObjStats) => {
                assert.ifError(err);
                assert.strictEqual(quietSetting, true);
                assert.deepStrictEqual(errorResults, []);
                assert.strictEqual(deletedObjStats.requesterIsObjOwner.
                    numOfObjectsRemoved, 1);
                assert.strictEqual(deletedObjStats.requesterNotObjOwner.
                    numOfObjectsRemoved, 2);
                assert.strictEqual(deletedObjStats.requesterIsObjOwner.
                    totalContentLengthDeleted, postBody.length);
                assert.strictEqual(deletedObjStats.requesterNotObjOwner.
                    totalContentLengthDeleted, contentLength);
                assert.strictEqual(metadata.keyMaps.get(bucketName)
                    .has(objectKey1), false);
                assert.strictEqual(metadata.keyMaps.get(bucketName)
                    .has(objectKey2), false);
                assert.strictEqual(metadata.keyMaps.get(bucketName)
                    .has(objectKey3), false);
                // call to delete data is async so wait 20 ms to check
                // that data deleted
                setTimeout(() => {
                    // eslint-disable-next-line
                    assert.deepStrictEqual(ds, [ , , , , ]);
                    done();
                }, 20);
            });
        });
    });
});
