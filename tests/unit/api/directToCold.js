const assert = require('assert');
const async = require('async');

const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');
const objectCopy = require('../../../lib/api/objectCopy');
const objectGet = require('../../../lib/api/objectGet');
const objectHead = require('../../../lib/api/objectHead');
const objectRestore = require('../../../lib/api/objectRestore');
const initiateMultipartUpload = require('../../../lib/api/initiateMultipartUpload');
const DummyRequest = require('../DummyRequest');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const metadata = require('../metadataswitch');
const mpuUtils = require('../utils/mpuUtils');
const { config } = require('../../../lib/Config');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const namespace = 'default';
const bucketName = 'bucketname';
const objectKey = 'objectName';
const postBody = Buffer.from('I am a body', 'utf8');
const coldLocation = 'location-dmf-v1';
const hotLocation = 'scality-internal-mem';
// marks a request as a restore, writing back an object already stored in a cold location
const putVersionHeader = { 'x-scal-s3-version-id': '' };

const putBucketRequest = new DummyRequest({
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    post:
        '<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        `<LocationConstraint>${hotLocation}</LocationConstraint>` +
        '</CreateBucketConfiguration>',
});

function putObjectRequest(headers = {}) {
    return new DummyRequest(
        {
            bucketName,
            namespace,
            objectKey,
            headers: { host: `${bucketName}.s3.amazonaws.com`, ...headers },
            url: `/${bucketName}/${objectKey}`,
        },
        postBody,
    );
}

function copyObjectRequest(headers = {}) {
    return new DummyRequest({
        bucketName,
        namespace,
        objectKey: 'copiedObject',
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
            'x-amz-copy-source': `/${bucketName}/${objectKey}`,
            ...headers,
        },
        url: `/${bucketName}/copiedObject`,
        socket: {},
    });
}

function getObjectMD(key, cb) {
    return metadata.getObjectMD(bucketName, key, {}, log, cb);
}

function getObjectRequest() {
    return {
        bucketName,
        namespace,
        objectKey,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: `/${bucketName}/${objectKey}`,
        actionImplicitDenies: false,
    };
}

function restoreObjectRequest(days) {
    return {
        ...getObjectRequest(),
        post:
            '<RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
            `<Days>${days}</Days>` +
            '<Tier>Standard</Tier>' +
            '</RestoreRequest>',
    };
}

function putDirectToColdObject(cb) {
    return objectPut(authInfo, putObjectRequest({ 'x-amz-storage-class': coldLocation }), undefined, log, err =>
        cb(err),
    );
}

function assertDirectToCold(md) {
    assert.strictEqual(md['x-amz-storage-class'], coldLocation);
    // the data itself stays in the hot location
    assert.strictEqual(md.dataStoreName, hotLocation);
    // the transition has not happened yet, so there is no archive info
    assert.strictEqual(md.archive, undefined);
    assert.strictEqual(md['x-amz-scal-transition-in-progress'], true);
    assert.match(md['x-amz-scal-transition-time'], /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
}

function assertNotTransitioned(md) {
    assert.strictEqual(md.dataStoreName, hotLocation);
    assert.strictEqual(md.archive, undefined);
    assert.strictEqual(md['x-amz-scal-transition-in-progress'], undefined);
}

describe('direct to cold', () => {
    let originalEnableDirectToCold;

    beforeEach(done => {
        originalEnableDirectToCold = config.enableDirectToCold;
        cleanup();
        bucketPut(authInfo, putBucketRequest, log, done);
    });

    afterEach(() => {
        config.enableDirectToCold = originalEnableDirectToCold;
        cleanup();
    });

    describe('when disabled', () => {
        beforeEach(() => {
            config.enableDirectToCold = false;
        });

        it('should reject a PUT naming a cold location', done => {
            objectPut(authInfo, putObjectRequest({ 'x-amz-storage-class': coldLocation }), undefined, log, err => {
                assert.strictEqual(err.message, 'InvalidStorageClass');
                done();
            });
        });

        it('should reject a CreateMultipartUpload naming a cold location', done => {
            const request = mpuUtils.createinitiateMPURequest(namespace, bucketName, objectKey, {
                'x-amz-storage-class': coldLocation,
            });
            initiateMultipartUpload(authInfo, request, log, err => {
                assert.strictEqual(err.message, 'InvalidStorageClass');
                done();
            });
        });

        it('should reject a CopyObject naming a cold location', done => {
            async.series(
                [
                    next => objectPut(authInfo, putObjectRequest(), undefined, log, next),
                    next =>
                        objectCopy(
                            authInfo,
                            copyObjectRequest({ 'x-amz-storage-class': coldLocation }),
                            bucketName,
                            objectKey,
                            undefined,
                            log,
                            next,
                        ),
                ],
                err => {
                    assert.strictEqual(err.message, 'InvalidStorageClass');
                    done();
                },
            );
        });
    });

    describe('when enabled', () => {
        beforeEach(() => {
            config.enableDirectToCold = true;
        });

        it('should flag an object PUT with a cold storage class for transition', done => {
            async.waterfall(
                [
                    next =>
                        objectPut(
                            authInfo,
                            putObjectRequest({ 'x-amz-storage-class': coldLocation }),
                            undefined,
                            log,
                            err => next(err),
                        ),
                    next => getObjectMD(objectKey, next),
                ],
                (err, md) => {
                    assert.ifError(err);
                    assertDirectToCold(md);
                    assert.strictEqual(md.originOp, 's3:ObjectCreated:Put');
                    done();
                },
            );
        });

        it('should not flag an object PUT without a storage class', done => {
            async.waterfall(
                [
                    next => objectPut(authInfo, putObjectRequest(), undefined, log, err => next(err)),
                    next => getObjectMD(objectKey, next),
                ],
                (err, md) => {
                    assert.ifError(err);
                    assertNotTransitioned(md);
                    assert.strictEqual(md['x-amz-storage-class'], 'STANDARD');
                    done();
                },
            );
        });

        it('should flag a completed multipart upload with a cold storage class for transition', done => {
            async.waterfall(
                [
                    next =>
                        mpuUtils
                            .initiateMpuP(bucketName, namespace, objectKey, log, {
                                'x-amz-storage-class': coldLocation,
                            })
                            .then(uploadId => next(null, uploadId), next),
                    (uploadId, next) =>
                        mpuUtils
                            .uploadPartP(bucketName, namespace, objectKey, uploadId, log)
                            .then(() => next(null, uploadId), next),
                    (uploadId, next) =>
                        mpuUtils.completeMpuP(bucketName, namespace, objectKey, uploadId, log).then(() => next(), next),
                    next => getObjectMD(objectKey, next),
                ],
                (err, md) => {
                    assert.ifError(err);
                    assertDirectToCold(md);
                    assert.strictEqual(md.originOp, 's3:ObjectCreated:CompleteMultipartUpload');
                    done();
                },
            );
        });

        it('should flag a copied object with a cold storage class for transition', done => {
            async.waterfall(
                [
                    next => objectPut(authInfo, putObjectRequest(), undefined, log, err => next(err)),
                    next =>
                        objectCopy(
                            authInfo,
                            copyObjectRequest({ 'x-amz-storage-class': coldLocation }),
                            bucketName,
                            objectKey,
                            undefined,
                            log,
                            err => next(err),
                        ),
                    next => getObjectMD('copiedObject', next),
                ],
                (err, md) => {
                    assert.ifError(err);
                    assertDirectToCold(md);
                    assert.strictEqual(md.originOp, 's3:ObjectCreated:Copy');
                    done();
                },
            );
        });

        it('should flag a self-copy changing only the storage class, and keep the data in place', done => {
            const selfCopyRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: {
                    host: `${bucketName}.s3.amazonaws.com`,
                    'x-amz-copy-source': `/${bucketName}/${objectKey}`,
                    'x-amz-storage-class': coldLocation,
                },
                url: `/${bucketName}/${objectKey}`,
                socket: {},
            });
            async.waterfall(
                [
                    next => objectPut(authInfo, putObjectRequest(), undefined, log, err => next(err)),
                    next => getObjectMD(objectKey, next),
                    (sourceMD, next) =>
                        objectCopy(authInfo, selfCopyRequest, bucketName, objectKey, undefined, log, err =>
                            next(err, sourceMD),
                        ),
                    (sourceMD, next) => getObjectMD(objectKey, (err, md) => next(err, sourceMD, md)),
                ],
                (err, sourceMD, md) => {
                    assert.ifError(err);
                    assertDirectToCold(md);
                    // the bytes are not rewritten: the existing data locations are reused
                    assert.deepStrictEqual(
                        md.location.map(l => l.key),
                        sourceMD.location.map(l => l.key),
                    );
                    done();
                },
            );
        });

        it('should reject a restore naming a cold location', done => {
            const request = putObjectRequest({
                'x-amz-storage-class': coldLocation,
                ...putVersionHeader,
            });
            objectPut(authInfo, request, undefined, log, err => {
                assert.strictEqual(err.message, 'InvalidStorageClass');
                done();
            });
        });

        it('should reject a restore initiating a multipart upload naming a cold location', done => {
            const request = mpuUtils.createinitiateMPURequest(namespace, bucketName, objectKey, {
                'x-amz-storage-class': coldLocation,
                ...putVersionHeader,
            });
            initiateMultipartUpload(authInfo, request, log, err => {
                assert.strictEqual(err.message, 'InvalidStorageClass');
                done();
            });
        });

        it('should not flag a restore completing a multipart upload', done => {
            async.waterfall(
                [
                    next => objectPut(authInfo, putObjectRequest(), undefined, log, err => next(err)),
                    next => getObjectMD(objectKey, next),
                    // simulate an object whose restore from the cold location is in progress
                    (md, next) => {
                        /* eslint-disable no-param-reassign */
                        md['x-amz-storage-class'] = coldLocation;
                        md.dataStoreName = coldLocation;
                        md.archive = {
                            archiveInfo: { archiveId: 'archive-id' },
                            restoreRequestedAt: new Date().toString(),
                            restoreRequestedDays: 5,
                        };
                        /* eslint-enable no-param-reassign */
                        metadata.putObjectMD(bucketName, objectKey, md, {}, log, err => next(err));
                    },
                    next =>
                        mpuUtils
                            .initiateMpuP(bucketName, namespace, objectKey, log, {
                                'x-amz-storage-class': coldLocation,
                            })
                            .then(uploadId => next(null, uploadId), next),
                    (uploadId, next) =>
                        mpuUtils
                            .uploadPartP(bucketName, namespace, objectKey, uploadId, log)
                            .then(() => next(null, uploadId), next),
                    (uploadId, next) =>
                        mpuUtils
                            .completeMpuP(bucketName, namespace, objectKey, uploadId, log, {
                                extraHeaders: putVersionHeader,
                            })
                            .then(() => next(), next),
                    next => getObjectMD(objectKey, next),
                ],
                (err, md) => {
                    assert.ifError(err);
                    // the object is being restored, it must not be transitioned back to cold
                    assert.strictEqual(md['x-amz-scal-transition-in-progress'], undefined);
                    done();
                },
            );
        });

        it('should not flag the source object of a copy', done => {
            async.waterfall(
                [
                    next => objectPut(authInfo, putObjectRequest(), undefined, log, err => next(err)),
                    next =>
                        objectCopy(
                            authInfo,
                            copyObjectRequest({ 'x-amz-storage-class': coldLocation }),
                            bucketName,
                            objectKey,
                            undefined,
                            log,
                            err => next(err),
                        ),
                    next => getObjectMD(objectKey, next),
                ],
                (err, md) => {
                    assert.ifError(err);
                    assertNotTransitioned(md);
                    done();
                },
            );
        });
    });

    describe('restore during the archive window', () => {
        beforeEach(done => {
            config.enableDirectToCold = true;
            putDirectToColdObject(done);
        });

        it('should accept a restore and record it in the object metadata', done => {
            const testStartTime = new Date();
            async.waterfall(
                [
                    next =>
                        objectRestore(authInfo, restoreObjectRequest(5), log, (err, statusCode) => {
                            assert.ifError(err);
                            assert.strictEqual(statusCode, 202);
                            next();
                        }),
                    next => getObjectMD(objectKey, next),
                ],
                (err, md) => {
                    assert.ifError(err);
                    // the object has not been archived, so the request is only recorded
                    assert.strictEqual(md.archive.archiveInfo, undefined);
                    assert.strictEqual(md.archive.restoreRequestedDays, 5);
                    assert.ok(new Date(md.archive.restoreRequestedAt) >= testStartTime);
                    assert.strictEqual(md.archive.restoreCompletedAt, undefined);
                    // the object is still declared cold, its data still hot, and it still needs
                    // to be transitioned
                    assert.strictEqual(md['x-amz-storage-class'], coldLocation);
                    assert.strictEqual(md.dataStoreName, hotLocation);
                    assert.strictEqual(md['x-amz-scal-transition-in-progress'], true);
                    assert.strictEqual(md.originOp, 's3:ObjectRestore:Post');
                    done();
                },
            );
        });

        it('should update the pending request on a repeated restore', done => {
            async.waterfall(
                [
                    next => objectRestore(authInfo, restoreObjectRequest(5), log, err => next(err)),
                    next => getObjectMD(objectKey, next),
                    (md, next) =>
                        objectRestore(authInfo, restoreObjectRequest(9), log, (err, statusCode) => {
                            assert.ifError(err);
                            assert.strictEqual(statusCode, 202);
                            next(null, md);
                        }),
                    (md, next) => getObjectMD(objectKey, (err, updatedMd) => next(err, md, updatedMd)),
                ],
                (err, md, updatedMd) => {
                    assert.ifError(err);
                    assert.strictEqual(updatedMd.archive.restoreRequestedDays, 9);
                    assert.ok(new Date(updatedMd.archive.restoreRequestedAt)
                        >= new Date(md.archive.restoreRequestedAt));
                    assert.strictEqual(updatedMd.archive.archiveInfo, undefined);
                    done();
                },
            );
        });

        it('should report an ongoing restore on HEAD', done => {
            async.waterfall(
                [
                    next =>
                        objectHead(authInfo, getObjectRequest(), log, (err, headers) => {
                            assert.ifError(err);
                            // before the restore request, the object simply appears cold
                            assert.strictEqual(headers['x-amz-storage-class'], coldLocation);
                            assert.strictEqual(headers['x-amz-meta-scal-s3-transition-in-progress'], true);
                            assert.strictEqual(headers['x-amz-restore'], undefined);
                            next();
                        }),
                    next => objectRestore(authInfo, restoreObjectRequest(5), log, err => next(err)),
                    next => objectHead(authInfo, getObjectRequest(), log, next),
                ],
                (err, headers) => {
                    assert.ifError(err);
                    assert.strictEqual(headers['x-amz-restore'], 'ongoing-request="true"');
                    assert.strictEqual(headers['x-amz-storage-class'], coldLocation);
                    assert.strictEqual(headers['x-amz-meta-scal-s3-transition-in-progress'], true);
                    done();
                },
            );
        });

        it('should still allow the object to be read', done => {
            async.waterfall(
                [
                    next => objectRestore(authInfo, restoreObjectRequest(5), log, err => next(err)),
                    next => objectGet(authInfo, getObjectRequest(), false, log,
                        (err, _, headers) => next(err, headers)),
                ],
                (err, headers) => {
                    // the data is still in the hot location, so it stays readable
                    assert.ifError(err);
                    assert.strictEqual(headers['x-amz-restore'], 'ongoing-request="true"');
                    done();
                },
            );
        });

        it('should accept a restore once direct-to-cold is disabled', done => {
            // the object was created while the feature was enabled: turning it off must not make
            // it unrestorable
            config.enableDirectToCold = false;
            objectRestore(authInfo, restoreObjectRequest(5), log, (err, statusCode) => {
                assert.ifError(err);
                assert.strictEqual(statusCode, 202);
                done();
            });
        });
    });
});
