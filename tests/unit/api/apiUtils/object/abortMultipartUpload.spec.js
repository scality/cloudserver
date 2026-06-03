const assert = require('assert');
const sinon = require('sinon');
const { parseString } = require('xml2js');
const { errors } = require('arsenal');
const async = require('async');
const crypto = require('crypto');

const abortMultipartUpload = require('../../../../../lib/api/apiUtils/object/abortMultipartUpload');
const { bucketPut } = require('../../../../../lib/api/bucketPut');
const initiateMultipartUpload = require('../../../../../lib/api/initiateMultipartUpload');
const bucketPutVersioning = require('../../../../../lib/api/bucketPutVersioning');
const objectPutPart = require('../../../../../lib/api/objectPutPart');
const { data } = require('../../../../../lib/data/wrapper');
const quotaUtils = require('../../../../../lib/api/apiUtils/quotas/quotaUtils');
const services = require('../../../../../lib/services');
const metadata = require('../../../../../lib/metadata/wrapper');
const metadataUtils = require('../../../../../lib/metadata/metadataUtils');
const { DummyRequestLogger, makeAuthInfo, cleanup, versioningTestUtils } = require('../../../helpers');
const DummyRequest = require('../../../DummyRequest');

describe('abortMultipartUpload', () => {
    const log = new DummyRequestLogger();
    const authInfo = makeAuthInfo('testCanonicalId');
    const bucketName = 'test-bucket';
    const objectKey = 'test-object';
    const postBody = Buffer.from('I am a part', 'utf8');

    const bucketRequest = new DummyRequest({
        bucketName,
        namespace: 'default',
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
    });

    const initiateRequest = new DummyRequest({
        bucketName,
        namespace: 'default',
        objectKey,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: `/${objectKey}?uploads`,
        actionImplicitDenies: false,
    });

    const abortRequest = new DummyRequest({
        bucketName,
        namespace: 'default',
        objectKey,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: `/${objectKey}?uploadId=test-upload-id`,
        query: { uploadId: 'test-upload-id' },
        apiMethods: 'multipartDelete',
        actionImplicitDenies: false,
        accountQuotas: {},
    });

    const enableVersioningRequest = versioningTestUtils.createBucketPutVersioningReq(bucketName, 'Enabled');

    beforeEach(() => {
        cleanup();
        sinon
            .stub(data, 'abortMPU')
            .callsFake(
                (
                    objectKey,
                    uploadId,
                    location,
                    bucketName,
                    request,
                    destBucket,
                    locationConstraintCheckFn,
                    log,
                    callback,
                ) => callback(null, false),
            );
        sinon.stub(data, 'delete').yields(null);
        sinon.stub(quotaUtils, 'validateQuotas').yields(null);

        sinon.stub(services, 'metadataValidateMultipart').yields(
            null,
            {
                getName: () => 'mpu-shadow-bucket',
                getMdBucketModelVersion: () => 2,
                isVersioningEnabled: () => true,
            },
            { controllingLocationConstraint: 'us-east-1' },
        );

        sinon.stub(services, 'getMPUparts').yields(null, { Contents: [] });
        sinon.stub(services, 'batchDeleteObjectMetadata').yields(null);
    });

    afterEach(() => {
        sinon.restore();
    });

    function createBucketAndMPU(versioned, callback) {
        async.waterfall(
            [
                next => bucketPut(authInfo, bucketRequest, log, err => next(err)),
                next => {
                    if (versioned) {
                        return bucketPutVersioning(authInfo, enableVersioningRequest, log, err => next(err));
                    }
                    return next();
                },
                next => initiateMultipartUpload(authInfo, initiateRequest, log, (err, result) => next(err, result)),
                (result, next) =>
                    parseString(result, (err, json) => next(err, json.InitiateMultipartUploadResult.UploadId[0])),
            ],
            callback,
        );
    }

    describe('basic functionality', () => {
        it('should successfully abort multipart upload', done => {
            createBucketAndMPU(false, (err, uploadId) => {
                assert.ifError(err);
                abortMultipartUpload(
                    authInfo,
                    bucketName,
                    objectKey,
                    uploadId,
                    log,
                    err => {
                        assert.strictEqual(err, null);
                        done();
                    },
                    { ...abortRequest, query: { uploadId } },
                );
            });
        });

        it('should return error for non-existent bucket', done => {
            abortMultipartUpload(
                authInfo,
                'non-existent-bucket',
                objectKey,
                'fake-upload-id',
                log,
                err => {
                    assert(err);
                    assert.strictEqual(err.is.NoSuchBucket, true);
                    done();
                },
                abortRequest,
            );
        });

        it('should return error for non-existent upload', done => {
            services.metadataValidateMultipart.yields(errors.NoSuchUpload);
            bucketPut(authInfo, bucketRequest, log, err => {
                assert.ifError(err);
                abortMultipartUpload(
                    authInfo,
                    bucketName,
                    objectKey,
                    'fake-upload-id',
                    log,
                    err => {
                        assert(err);
                        assert.strictEqual(err.is.NoSuchUpload, true);
                        done();
                    },
                    abortRequest,
                );
            });
        });

        it('should return error if data backend fails to abort', done => {
            const testError = new Error('Data backend abort failed');
            // This stub is now more explicit to avoid side-effects.
            data.abortMPU.callsFake((objKey, upId, loc, bucket, req, destB, locCheckFn, log, cb) => {
                cb(testError);
            });
            createBucketAndMPU(false, (err, uploadId) => {
                assert.ifError(err);
                abortMultipartUpload(
                    authInfo,
                    bucketName,
                    objectKey,
                    uploadId,
                    log,
                    err => {
                        assert.deepStrictEqual(err, testError);
                        done();
                    },
                    { ...abortRequest, query: { uploadId } },
                );
            });
        });
    });

    describe('with multipart upload parts', () => {
        it('should delete part data when aborting', done => {
            createBucketAndMPU(false, (err, uploadId) => {
                assert.ifError(err);
                const partRequest = new DummyRequest(
                    {
                        bucketName,
                        objectKey,
                        namespace: 'default',
                        url: `/${objectKey}?partNumber=1&uploadId=${uploadId}`,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        query: { partNumber: '1', uploadId },
                        calculatedHash: crypto.createHash('md5').update(postBody).digest('hex'),
                    },
                    postBody,
                );

                objectPutPart(authInfo, partRequest, undefined, log, err => {
                    assert.ifError(err);
                    services.getMPUparts.yields(null, {
                        Contents: [
                            {
                                key: `1${uploadId}`,
                                value: {
                                    Size: 11,
                                    partLocations: [{ key: 'a-key' }],
                                },
                            },
                        ],
                    });

                    abortMultipartUpload(
                        authInfo,
                        bucketName,
                        objectKey,
                        uploadId,
                        log,
                        err => {
                            assert.strictEqual(err, null);
                            sinon.assert.called(data.delete);
                            done();
                        },
                        { ...abortRequest, query: { uploadId } },
                    );
                });
            });
        });
    });

    describe('orphaned version cleanup', () => {
        let findObjectVersionStub;
        let metadataGetObjectStub;

        beforeEach(done => {
            findObjectVersionStub = sinon.stub(services, 'findObjectVersionByUploadId');
            metadataGetObjectStub = sinon.stub(metadataUtils, 'metadataGetObject');
            bucketPut(authInfo, bucketRequest, log, err => {
                assert.ifError(err);
                done();
            });
        });

        it('should NOT search for orphans if master object does not exist', done => {
            bucketPutVersioning(authInfo, enableVersioningRequest, log, err => {
                assert.ifError(err);
                abortMultipartUpload(
                    authInfo,
                    bucketName,
                    objectKey,
                    'any-id',
                    log,
                    err => {
                        assert.ifError(err);
                        sinon.assert.notCalled(findObjectVersionStub);
                        done();
                    },
                    abortRequest,
                );
            });
        });

        it('should NOT search for orphans on non-versioned bucket with mismatched master uploadId', done => {
            const standardMetadataValidateStub = sinon.stub(metadataUtils, 'standardMetadataValidateBucketAndObj');
            const mockBucket = {
                isVersioningEnabled: () => false,
                getOwner: () => 'testCanonicalId',
                getName: () => bucketName,
            };
            const mockMasterMD = { uploadId: 'master-id' };
            standardMetadataValidateStub.yields(null, mockBucket, mockMasterMD);

            abortMultipartUpload(
                authInfo,
                bucketName,
                objectKey,
                'abort-id',
                log,
                err => {
                    assert.ifError(err);
                    sinon.assert.notCalled(findObjectVersionStub);
                    done();
                },
                { ...abortRequest, query: { uploadId: 'abort-id' } },
            );
        });

        it('should proceed without cleanup if finding object version fails', done => {
            const testError = new Error('Find version failed');
            findObjectVersionStub.yields(testError);
            const deleteObjectMDStub = sinon.stub(metadata, 'deleteObjectMD').yields(null);

            const standardMetadataValidateStub = sinon.stub(metadataUtils, 'standardMetadataValidateBucketAndObj');
            const mockBucket = {
                isVersioningEnabled: () => true,
                getOwner: () => 'testCanonicalId',
                getName: () => bucketName,
            };
            const mockMasterMD = { uploadId: 'master-id' };
            standardMetadataValidateStub.yields(null, mockBucket, mockMasterMD);

            abortMultipartUpload(
                authInfo,
                bucketName,
                objectKey,
                'abort-id',
                log,
                err => {
                    assert.ifError(err);
                    sinon.assert.calledOnce(findObjectVersionStub);
                    sinon.assert.notCalled(metadataGetObjectStub);
                    sinon.assert.notCalled(deleteObjectMDStub);
                    done();
                },
                { ...abortRequest, query: { uploadId: 'abort-id' } },
            );
        });

        it('should proceed without cleanup if finding object version returns null', done => {
            findObjectVersionStub.yields(null, null);
            const deleteObjectMDStub = sinon.stub(metadata, 'deleteObjectMD').yields(null);

            const standardMetadataValidateStub = sinon.stub(metadataUtils, 'standardMetadataValidateBucketAndObj');
            const mockBucket = {
                isVersioningEnabled: () => true,
                getOwner: () => 'testCanonicalId',
                getName: () => bucketName,
            };
            const mockMasterMD = { uploadId: 'master-id' };
            standardMetadataValidateStub.yields(null, mockBucket, mockMasterMD);

            abortMultipartUpload(
                authInfo,
                bucketName,
                objectKey,
                'abort-id',
                log,
                err => {
                    assert.ifError(err);
                    sinon.assert.calledOnce(findObjectVersionStub);
                    sinon.assert.notCalled(metadataGetObjectStub);
                    sinon.assert.notCalled(deleteObjectMDStub);
                    done();
                },
                { ...abortRequest, query: { uploadId: 'abort-id' } },
            );
        });

        it('should proceed without cleanup if found version getObject fails', done => {
            const testError = new Error('Find version failed');
            findObjectVersionStub.yields(null, { uploadId: 'abort-id', VersionId: 'orphan-vid' });
            metadataGetObjectStub.yields(testError);
            const deleteObjectMDStub = sinon.stub(metadata, 'deleteObjectMD').yields(null);

            const standardMetadataValidateStub = sinon.stub(metadataUtils, 'standardMetadataValidateBucketAndObj');
            const mockBucket = {
                isVersioningEnabled: () => true,
                getOwner: () => 'testCanonicalId',
                getName: () => bucketName,
            };
            const mockMasterMD = { uploadId: 'master-id' };
            standardMetadataValidateStub.yields(null, mockBucket, mockMasterMD);

            abortMultipartUpload(
                authInfo,
                bucketName,
                objectKey,
                'abort-id',
                log,
                err => {
                    assert.ifError(err);
                    sinon.assert.calledOnce(findObjectVersionStub);
                    sinon.assert.calledOnce(metadataGetObjectStub);
                    sinon.assert.calledWith(metadataGetObjectStub, bucketName, objectKey, 'orphan-vid', null, log);
                    sinon.assert.notCalled(deleteObjectMDStub);
                    done();
                },
                { ...abortRequest, query: { uploadId: 'abort-id' } },
            );
        });

        it('should delete the correct orphaned object version', done => {
            const deleteObjectMDStub = sinon.stub(metadata, 'deleteObjectMD').yields(null);
            findObjectVersionStub.yields(null, { uploadId: 'abort-id', VersionId: 'orphan-vid' });
            metadataGetObjectStub.yields(null, { uploadId: 'abort-id', versionId: 'orphan-vid' });

            const standardMetadataValidateStub = sinon.stub(metadataUtils, 'standardMetadataValidateBucketAndObj');
            const mockBucket = {
                isVersioningEnabled: () => true,
                getOwner: () => 'testCanonicalId',
                getName: () => bucketName,
                getVersioningConfiguration: () => ({ Status: 'Enabled' }),
            };
            const mockMasterMD = { uploadId: 'master-id' };
            standardMetadataValidateStub.yields(null, mockBucket, mockMasterMD);

            abortMultipartUpload(
                authInfo,
                bucketName,
                objectKey,
                'abort-id',
                log,
                err => {
                    assert.ifError(err);
                    sinon.assert.calledOnce(deleteObjectMDStub);
                    assert.strictEqual(deleteObjectMDStub.getCall(0).args[2].versionId, 'orphan-vid');
                    done();
                },
                { ...abortRequest, query: { uploadId: 'abort-id' } },
            );
        });

        it('should proceed if orphaned object version is already deleted (NoSuchKey)', done => {
            const deleteObjectMDStub = sinon.stub(metadata, 'deleteObjectMD').yields(errors.NoSuchKey);
            findObjectVersionStub.yields(null, { uploadId: 'abort-id', VersionId: 'orphan-vid' });
            metadataGetObjectStub.yields(null, { uploadId: 'abort-id', versionId: 'orphan-vid' });

            const standardMetadataValidateStub = sinon.stub(metadataUtils, 'standardMetadataValidateBucketAndObj');
            const mockBucket = {
                isVersioningEnabled: () => true,
                getOwner: () => 'testCanonicalId',
                getName: () => bucketName,
                getVersioningConfiguration: () => ({ Status: 'Enabled' }),
            };
            const mockMasterMD = { uploadId: 'master-id' };
            standardMetadataValidateStub.yields(null, mockBucket, mockMasterMD);

            abortMultipartUpload(
                authInfo,
                bucketName,
                objectKey,
                'abort-id',
                log,
                err => {
                    assert.ifError(err);
                    sinon.assert.calledOnce(deleteObjectMDStub);
                    done();
                },
                { ...abortRequest, query: { uploadId: 'abort-id' } },
            );
        });
    });
});
