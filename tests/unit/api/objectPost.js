const assert = require('assert');
const async = require('async');
const moment = require('moment');
const { errors } = require('arsenal');
const sinon = require('sinon');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutObjectLock = require('../../../lib/api/bucketPutObjectLock');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils }
    = require('../helpers');
const { ds } = require('arsenal').storage.data.inMemory.datastore;
const metadata = require('../metadataswitch');
const objectPost = require('../../../lib/api/objectPost');
const { objectLockTestUtils } = require('../helpers');
const DummyRequest = require('../DummyRequest');
const mpuUtils = require('../utils/mpuUtils');
const any = sinon.match.any;

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const bucketName = 'bucketname123';
const postBody = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const mockDate = new Date(2050, 10, 12);
const testPutBucketRequest = new DummyRequest({
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
});
const testPutBucketRequestLock = new DummyRequest({
    bucketName,
    headers: {
        'host': `${bucketName}.s3.amazonaws.com`,
        'x-amz-bucket-object-lock-enabled': 'true',
    },
    url: '/',
});

const originalputObjectMD = metadata.putObjectMD;
const objectName = 'objectName';

let testPostObjectRequest;
const enableVersioningRequest =
    versioningTestUtils.createBucketPutVersioningReq(bucketName, 'Enabled');
const suspendVersioningRequest =
    versioningTestUtils.createBucketPutVersioningReq(bucketName, 'Suspended');

describe('objectPost API', () => {
    beforeEach(() => {
        cleanup();
        sinon.spy(metadata, 'putObjectMD');
        testPostObjectRequest = new DummyRequest({
            bucketName,
            formData: {
                bucket: bucketName,
                key: objectName,
            },
            fileEventData: {},
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            url: '/',
        }, postBody);
    });

    afterEach(() => {
        sinon.restore();
        metadata.putObjectMD = originalputObjectMD;
    });

    it('should return an error if the bucket does not exist', done => {
        objectPost(authInfo, testPostObjectRequest, undefined, log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    it('should successfully post an object', done => {
        const testPostObjectRequest = new DummyRequest({
            bucketName,
            formData: {
                bucket: bucketName,
                key: objectName,
            },
            fileEventData: {},
            headers: {},
            url: '/',
            calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
        }, postBody);

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPost(authInfo, testPostObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    metadata.getObjectMD(bucketName, objectName,
                        {}, log, (err, md) => {
                            assert(md);
                            assert
                                .strictEqual(md['content-md5'], correctMD5);
                            done();
                        });
                });
        });
    });

    const mockModes = ['GOVERNANCE', 'COMPLIANCE'];
    mockModes.forEach(mockMode => {
        it(`should post an object with valid date & ${mockMode} mode`, done => {
            const testPostObjectRequest = new DummyRequest({
                bucketName,
                formData: {
                    bucket: bucketName,
                    key: objectName,
                },
                fileEventData: {},
                headers: {
                    'x-amz-object-lock-retain-until-date': mockDate,
                    'x-amz-object-lock-mode': mockMode,
                },
                url: '/',
                calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
            }, postBody);
            bucketPut(authInfo, testPutBucketRequestLock, log, () => {
                objectPost(authInfo, testPostObjectRequest, undefined, log,
                    (err, headers) => {
                        assert.ifError(err);
                        assert.strictEqual(headers.ETag, `"${correctMD5}"`);
                        metadata.getObjectMD(bucketName, objectName, {}, log,
                            (err, md) => {
                                const mode = md.retentionMode;
                                const retainUntilDate = md.retentionDate;
                                assert.ifError(err);
                                assert(md);
                                assert.strictEqual(mode, mockMode);
                                assert.strictEqual(retainUntilDate, mockDate);
                                done();
                            });
                    });
            });
        });
    });

    const formatTime = time => time.slice(0, 20);

    const testObjectLockConfigs = [
        {
            testMode: 'COMPLIANCE',
            val: 30,
            type: 'Days',
        },
        {
            testMode: 'GOVERNANCE',
            val: 5,
            type: 'Years',
        },
    ];
    testObjectLockConfigs.forEach(config => {
        const { testMode, type, val } = config;
        it('should put an object with default retention if object does not ' +
            'have retention configuration but bucket has', done => {
            const testPostObjectRequest = new DummyRequest({
                bucketName,
                formData: {
                    bucket: bucketName,
                    key: objectName,
                },
                fileEventData: {},
                headers: {},
                url: '/',
                calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
            }, postBody);

            const testObjLockRequest = {
                bucketName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                post: objectLockTestUtils.generateXml(testMode, val, type),
            };

            bucketPut(authInfo, testPutBucketRequestLock, log, () => {
                bucketPutObjectLock(authInfo, testObjLockRequest, log, () => {
                    objectPost(authInfo, testPostObjectRequest, undefined, log,
                            (err, headers) => {
                                assert.ifError(err);
                                assert.strictEqual(headers.ETag, `"${correctMD5}"`);
                                metadata.getObjectMD(bucketName, objectName, {},
                                    log, (err, md) => {
                                        const mode = md.retentionMode;
                                        const retainDate = md.retentionDate;
                                        const date = moment();
                                        const days
                                            = type === 'Days' ? val : val * 365;
                                        const expectedDate
                                            = date.add(days, 'days');
                                        assert.ifError(err);
                                        assert.strictEqual(mode, testMode);
                                        assert.strictEqual(formatTime(retainDate),
                                            formatTime(expectedDate.toISOString()));
                                        done();
                                    });
                            });
                });
            });
        });
    });


    it('should successfully put an object with legal hold ON', done => {
        const request = new DummyRequest({
            bucketName,
            formData: {
                bucket: bucketName,
                key: objectName,
            },
            fileEventData: {},
            headers: {
                'x-amz-object-lock-legal-hold': 'ON',
            },
            url: '/',
            calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
        }, postBody);

        bucketPut(authInfo, testPutBucketRequestLock, log, () => {
            objectPost(authInfo, request, undefined, log, (err, headers) => {
                assert.ifError(err);
                assert.strictEqual(headers.ETag, `"${correctMD5}"`);
                metadata.getObjectMD(bucketName, objectName, {}, log,
                    (err, md) => {
                        assert.ifError(err);
                        assert.strictEqual(md.legalHold, true);
                        done();
                    });
            });
        });
    });

    it('should successfully put an object with legal hold OFF', done => {
        const request = new DummyRequest({
            bucketName,
            formData: {
                bucket: bucketName,
                key: objectName,
            },
            fileEventData: {},
            headers: {
                'x-amz-object-lock-legal-hold': 'OFF',
            },
            url: '/',
            calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
        }, postBody);

        bucketPut(authInfo, testPutBucketRequestLock, log, () => {
            objectPost(authInfo, request, undefined, log, (err, headers) => {
                assert.ifError(err);
                assert.strictEqual(headers.ETag, `"${correctMD5}"`);
                metadata.getObjectMD(bucketName, objectName, {}, log,
                    (err, md) => {
                        assert.ifError(err);
                        assert(md);
                        assert.strictEqual(md.legalHold, false);
                        done();
                    });
            });
        });
    });

    it('should not leave orphans in data when overwriting an object', done => {
        const testPostObjectRequest2 = new DummyRequest({
            bucketName,
            formData: {
                bucket: bucketName,
                key: objectName,
            },
            fileEventData: {},
            headers: {},
            url: '/',
        }, Buffer.from('I am another body', 'utf8'));

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPost(authInfo, testPostObjectRequest,
                undefined, log, () => {
                    objectPost(authInfo, testPostObjectRequest2, undefined,
                        log,
                        () => {
                            // orphan objects don't get deleted
                            // until the next tick
                            // in memory
                            setImmediate(() => {
                                // Data store starts at index 1
                                assert.strictEqual(ds[0], undefined);
                                assert.strictEqual(ds[1], undefined);
                                assert.deepStrictEqual(ds[2].value,
                                    Buffer.from('I am another body', 'utf8'));
                                done();
                            });
                        });
                });
        });
    });

    it('should not leave orphans in data when overwriting an multipart upload object', done => {
        bucketPut(authInfo, testPutBucketRequest, log, () => {
            mpuUtils.createMPU('default', bucketName, objectName, log,
                (err, testUploadId) => {
                    objectPost(authInfo, testPostObjectRequest, undefined, log, err => {
                        assert.ifError(err);
                        sinon.assert.calledWith(metadata.putObjectMD,
                            any, any, any, sinon.match({ oldReplayId: testUploadId }), any, any);
                        done();
                    });
                });
        });
    });

    describe('objectPost API with versioning', () => {
        beforeEach(() => {
            cleanup();
        });

        const objData = ['foo0', 'foo1', 'foo2'].map(str =>
            Buffer.from(str, 'utf8'));
        const testPostObjectRequests = objData.map(data => versioningTestUtils
            .createPostObjectRequest(bucketName, objectName, data));

        it('should delete latest version when creating new null version ' +
            'if latest version is null version', done => {
            async.series([
                callback => bucketPut(authInfo, testPutBucketRequest, log,
                        callback),
                    // putting null version by putting obj before versioning configured
                callback => objectPost(authInfo, testPostObjectRequests[0], undefined,
                        log, err => {
                            versioningTestUtils.assertDataStoreValues(ds, [objData[0]]);
                            callback(err);
                        }),
                callback => bucketPutVersioning(authInfo, suspendVersioningRequest,
                        log, callback),
                    // creating new null version by putting obj after ver suspended
                callback => objectPost(authInfo, testPostObjectRequests[1],
                        undefined, log, err => {
                            // wait until next tick since mem backend executes
                            // deletes in the next tick
                            setImmediate(() => {
                                // old null version should be deleted
                                versioningTestUtils.assertDataStoreValues(ds,
                                    [undefined, objData[1]]);
                                callback(err);
                            });
                        }),
                    // create another null version
                callback => objectPost(authInfo, testPostObjectRequests[2],
                        undefined, log, err => {
                            setImmediate(() => {
                                // old null version should be deleted
                                versioningTestUtils.assertDataStoreValues(ds,
                                    [undefined, undefined, objData[2]]);
                                callback(err);
                            });
                        }),
            ], done);
        });

        describe('when null version is not the latest version', () => {
            const objData = ['foo0', 'foo1', 'foo2'].map(str =>
                Buffer.from(str, 'utf8'));
            const testPostObjectRequests = objData.map(data => versioningTestUtils
                .createPostObjectRequest(bucketName, objectName, data));
            beforeEach(done => {
                async.series([
                    callback => bucketPut(authInfo, testPutBucketRequest, log,
                        callback),
                    // putting null version: put obj before versioning configured
                    callback => objectPost(authInfo, testPostObjectRequests[0],
                        undefined, log, callback),
                    callback => bucketPutVersioning(authInfo,
                        enableVersioningRequest, log, callback),
                    // put another version:
                    callback => objectPost(authInfo, testPostObjectRequests[1],
                        undefined, log, callback),
                    callback => bucketPutVersioning(authInfo,
                        suspendVersioningRequest, log, callback),
                ], err => {
                    if (err) {
                        return done(err);
                    }
                    versioningTestUtils.assertDataStoreValues(ds,
                        objData.slice(0, 2));
                    return done();
                });
            });

            it('should still delete null version when creating new null version',
                done => {
                    objectPost(authInfo, testPostObjectRequests[2], undefined,
                        log, err => {
                            assert.ifError(err, `Unexpected err: ${err}`);
                            setImmediate(() => {
                                // old null version should be deleted after putting
                                // new null version
                                versioningTestUtils.assertDataStoreValues(ds,
                                    [undefined, objData[1], objData[2]]);
                                done(err);
                            });
                        });
                });
        });

        it('should return BadDigest error and not leave orphans in data when ' +
            'contentMD5 and completedHash do not match', done => {
            const testPostObjectRequests = new DummyRequest({
                bucketName,
                formData: {
                    bucket: bucketName,
                    key: objectName,
                },
                fileEventData: {},
                headers: {},
                url: '/',
                contentMD5: 'vnR+tLdVF79rPPfF+7YvOg==',
            }, Buffer.from('I am another body', 'utf8'));

            bucketPut(authInfo, testPutBucketRequest, log, () => {
                objectPost(authInfo, testPostObjectRequests, undefined, log,
                        err => {
                            assert.deepStrictEqual(err, errors.BadDigest);
                            // orphan objects don't get deleted
                            // until the next tick
                            // in memory
                            setImmediate(() => {
                                // Data store starts at index 1
                                assert.strictEqual(ds[0], undefined);
                                assert.strictEqual(ds[1], undefined);
                                done();
                            });
                        });
            });
        });
    });
});

