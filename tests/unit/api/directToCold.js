const assert = require('assert');
const async = require('async');

const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');
const objectCopy = require('../../../lib/api/objectCopy');
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
});
