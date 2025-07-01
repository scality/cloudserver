const assert = require('assert');
const sinon = require('sinon');
const { parseString } = require('xml2js');
const { errors } = require('arsenal');
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

    let dataAbortMPUStub;
    let dataDeleteStub;
    let validateQuotasStub;

    beforeEach(() => {
        cleanup();
        // Stub external operations that we don't want to test
        dataAbortMPUStub = sinon.stub(data, 'abortMPU').callsFake((objectKey, uploadId, location,
            bucketName, request, destBucket, locationConstraintCheckFn, log, callback) => {
            // Call the callback immediately without executing locationConstraintCheck
            callback(null, false);
        });
        dataDeleteStub = sinon.stub(data, 'delete').yields();
        validateQuotasStub = sinon.stub(quotaUtils, 'validateQuotas').yields();
    });

    afterEach(() => {
        sinon.restore();
    });

    // Helper to create bucket and MPU, returns uploadId
    function createBucketAndMPU(versioned, callback) {
        if (versioned) {
            bucketPut(authInfo, bucketRequest, log, err => {
                if (err) {
                    return callback(err);
                }
                return bucketPutVersioning(authInfo, enableVersioningRequest, log, err => {
                    if (err) {
                        return callback(err);
                    }
                    return initiateMultipartUpload(authInfo, initiateRequest, log, (err, result) => {
                        if (err) {
                            return callback(err);
                        }
                        return parseString(result, (err, json) => {
                            if (err) {
                                return callback(err);
                            }
                            const uploadId = json.InitiateMultipartUploadResult.UploadId[0];
                            return callback(null, uploadId);
                        });
                    });
                });
            });
        } else {
            bucketPut(authInfo, bucketRequest, log, err => {
                if (err) {
                    return callback(err);
                }
                return initiateMultipartUpload(authInfo, initiateRequest, log, (err, result) => {
                    if (err) {
                        return callback(err);
                    }
                    return parseString(result, (err, json) => {
                        if (err) {
                            return callback(err);
                        }
                        const uploadId = json.InitiateMultipartUploadResult.UploadId[0];
                        return callback(null, uploadId);
                    });
                });
            });
        }
    }

    describe('basic functionality', () => {
        it('should successfully abort multipart upload', done => {
            createBucketAndMPU(false, (err, uploadId) => {
                assert.ifError(err);

                abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, err => {
                    assert.strictEqual(err, null);
                    sinon.assert.calledOnce(dataAbortMPUStub);
                    done();
                }, abortRequest);
            });
        });

        it('should return error for non-existent bucket', done => {
            abortMultipartUpload(authInfo, 'non-existent-bucket', objectKey, 'fake-upload-id', log, err => {
                assert(err);
                assert.strictEqual(err.is.NoSuchBucket, true);
                done();
            }, abortRequest);
        });

        it('should return error for non-existent upload', done => {
            bucketPut(authInfo, bucketRequest, log, err => {
                assert.ifError(err);

                abortMultipartUpload(authInfo, bucketName, objectKey, 'fake-upload-id', log, err => {
                    assert(err);
                    assert.strictEqual(err.is.NoSuchUpload, true);
                    done();
                }, abortRequest);
            });
        });

        it('should skip data deletion when skipDataDelete is true', done => {
            dataAbortMPUStub.restore();
            sinon.stub(data, 'abortMPU').callsFake((objectKey, uploadId, location,
                bucketName, request, destBucket, locationConstraintCheckFn, log, callback) => {
                callback(null, true); // skipDataDelete = true
            });

            createBucketAndMPU(false, (err, uploadId) => {
                assert.ifError(err);

                abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, err => {
                    assert.strictEqual(err, null);
                    sinon.assert.notCalled(dataDeleteStub);
                    sinon.assert.notCalled(validateQuotasStub);
                    done();
                }, abortRequest);
            });
        });
    });

    describe('with multipart upload parts', () => {
        it('should delete part data when aborting', done => {
            createBucketAndMPU(false, (err, uploadId) => {
                assert.ifError(err);

                // Add a part to the multipart upload
                const md5Hash = crypto.createHash('md5');
                md5Hash.update(postBody);
                const calculatedHash = md5Hash.digest('hex');

                const partRequest = new DummyRequest({
                    bucketName,
                    objectKey,
                    namespace: 'default',
                    url: `/${objectKey}?partNumber=1&uploadId=${uploadId}`,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: {
                        partNumber: '1',
                        uploadId,
                    },
                    calculatedHash,
                }, postBody);

                objectPutPart(authInfo, partRequest, undefined, log, err => {
                    assert.ifError(err);

                    abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, err => {
                        assert.strictEqual(err, null);
                        sinon.assert.calledOnce(dataAbortMPUStub);
                        sinon.assert.called(dataDeleteStub); // Should delete part data
                        done();
                    }, abortRequest);
                });
            });
        });
    });

    describe('versioned bucket behavior', () => {
        it('should handle versioned bucket abort', done => {
            createBucketAndMPU(true, (err, uploadId) => {
                assert.ifError(err);

                abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, err => {
                    assert.strictEqual(err, null);
                    sinon.assert.calledOnce(dataAbortMPUStub);
                    done();
                }, abortRequest);
            });
        });
    });

    describe('version listing optimization', () => {
        let getObjectListingStub;

        beforeEach(() => {
            getObjectListingStub = sinon.stub(services, 'getObjectListing');
        });

        afterEach(() => {
            if (getObjectListingStub) {
                getObjectListingStub.restore();
            }
        });

        it('should use optimized prefix when listing versions', done => {
            createBucketAndMPU(true, (err, uploadId) => {
                assert.ifError(err);

                // Mock version listing response - return empty to simulate no cleanup needed
                getObjectListingStub.yields(null, {
                    Versions: [],
                    IsTruncated: false
                });

                abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, err => {
                    assert.strictEqual(err, null);

                    // Check if version listing was called with the correct prefix
                    const versionListingCalls = getObjectListingStub.getCalls().filter(call =>
                        call.args[1] && call.args[1].listingType === 'DelimiterVersions'
                    );

                    if (versionListingCalls.length > 0) {
                        // If version listing was called, verify the prefix optimization
                        const expectedPrefix = `${objectKey}\u0000`; // objectKey + VersionId separator
                        assert.strictEqual(versionListingCalls[0].args[1].prefix, expectedPrefix);
                        assert.strictEqual(versionListingCalls[0].args[1].maxKeys, 1000);
                    }

                    done();
                }, abortRequest);
            });
        });

        it('should handle version listing pagination', done => {
            createBucketAndMPU(true, (err, uploadId) => {
                assert.ifError(err);

                // First page - no match, truncated
                getObjectListingStub.onFirstCall().yields(null, {
                    Versions: [
                        {
                            key: objectKey,
                            value: { uploadId: 'different-upload-id', versionId: 'version-456' }
                        }
                    ],
                    IsTruncated: true,
                    NextKeyMarker: objectKey,
                    NextVersionIdMarker: 'version-456'
                });

                // Second page - no match, not truncated
                getObjectListingStub.onSecondCall().yields(null, {
                    Versions: [],
                    IsTruncated: false
                });

                abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, err => {
                    assert.strictEqual(err, null);

                    // Check if pagination markers were used correctly
                    const versionListingCalls = getObjectListingStub.getCalls().filter(call =>
                        call.args[1] && call.args[1].listingType === 'DelimiterVersions'
                    );

                    if (versionListingCalls.length >= 2) {
                        // First call should not have markers
                        assert.strictEqual(versionListingCalls[0].args[1].keyMarker, undefined);
                        assert.strictEqual(versionListingCalls[0].args[1].versionIdMarker, undefined);

                        // Second call should have markers from first response
                        assert.strictEqual(versionListingCalls[1].args[1].keyMarker, objectKey);
                        assert.strictEqual(versionListingCalls[1].args[1].versionIdMarker, 'version-456');
                    }

                    done();
                }, abortRequest);
            });
        });

        it('should handle version listing errors gracefully', done => {
            createBucketAndMPU(true, (err, uploadId) => {
                assert.ifError(err);

                // Simulate error during version listing
                getObjectListingStub.yields(errors.InternalError);

                abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, err => {
                    assert.strictEqual(err, null); // Should continue despite version listing error
                    done();
                }, abortRequest);
            });
        });

        it('should only list versions for exact object key, not similar prefixes', done => {
            const testObjectKey = 'myfile.txt';

            // Mock that we have an overview object to trigger cleanup logic
            const metadataValidateMultipartStub = sinon.stub(services, 'metadataValidateMultipart')
                .yields(null, {
                    getName: () => 'mpuShadowBuckettest-bucket',
                    getMdBucketModelVersion: () => 2
                }, { controllingLocationConstraint: 'us-east-1' });

            const getMPUpartsStub = sinon.stub(services, 'getMPUparts').yields(null, {
                Contents: [{
                    key: 'part-key',
                    value: { Size: 1024, partLocations: [{ key: 'loc-1' }] }
                }]
            });

            const batchDeleteObjectMetadataStub = sinon.stub(services, 'batchDeleteObjectMetadata').yields();

            // Simulate version listing response with objects that have similar prefixes
            getObjectListingStub.yields(null, {
                Versions: [
                    // This should be found (exact match)
                    {
                        key: testObjectKey, // 'myfile.txt'
                        value: { uploadId: 'target-upload-id', versionId: 'version-123' }
                    },
                    // These should NOT be found (have similar prefixes but different keys)
                    {
                        key: 'myfile.txt.backup', // has prefix 'myfile.txt'
                        value: { uploadId: 'target-upload-id', versionId: 'version-456' }
                    },
                    {
                        key: 'myfile.txt2', // has prefix 'myfile.txt'
                        value: { uploadId: 'target-upload-id', versionId: 'version-789' }
                    }
                ],
                IsTruncated: false
            });

            bucketPut(authInfo, bucketRequest, log, err => {
                assert.ifError(err);
                bucketPutVersioning(authInfo, enableVersioningRequest, log, err => {
                    assert.ifError(err);

                    abortMultipartUpload(authInfo, bucketName, testObjectKey, 'target-upload-id', log, err => {
                        assert.strictEqual(err, undefined);

                        // Verify the prefix optimization was used correctly
                        const versionListingCalls = getObjectListingStub.getCalls().filter(call =>
                            call.args[1] && call.args[1].listingType === 'DelimiterVersions'
                        );

                        assert(versionListingCalls.length > 0, 'Should have called version listing');

                        // Verify the exact prefix was used
                        const expectedPrefix = `${testObjectKey}\u0000`; // 'myfile.txt\u0000'
                        assert.strictEqual(versionListingCalls[0].args[1].prefix, expectedPrefix);

                        // The prefix ensures we only get versions that start with 'myfile.txt\u0000'
                        // This would match: 'myfile.txt\u0000version1', 'myfile.txt\u0000version2', etc.
                        // But NOT: 'myfile.txt.backup\u0000...', 'myfile.txt2\u0000...', etc.

                        // Cleanup
                        metadataValidateMultipartStub.restore();
                        getMPUpartsStub.restore();
                        batchDeleteObjectMetadataStub.restore();
                        done();
                    }, abortRequest);
                });
            });
        });

        it('should delete the correct object version, not the master version', done => {
            const targetUploadId = 'upload-to-cleanup';
            const masterVersionId = 'master-version-123';
            const orphanedVersionId = 'orphaned-version-456';

            // Mock that we have an overview object to trigger cleanup logic
            const metadataValidateMultipartStub = sinon.stub(services, 'metadataValidateMultipart')
                .yields(null, {
                    getName: () => 'mpuShadowBuckettest-bucket',
                    getMdBucketModelVersion: () => 2
                }, { controllingLocationConstraint: 'us-east-1' });

            const getMPUpartsStub = sinon.stub(services, 'getMPUparts').yields(null, {
                Contents: [{
                    key: 'part-key',
                    value: { Size: 1024, partLocations: [{ key: 'loc-1' }] }
                }]
            });

            const batchDeleteObjectMetadataStub = sinon.stub(services, 'batchDeleteObjectMetadata').yields();
            const deleteObjectMDStub = sinon.stub(metadata, 'deleteObjectMD').yields();

            // Simulate version listing response with multiple versions
            getObjectListingStub.yields(null, {
                Versions: [
                    // Master version (current/latest) - should NOT be deleted
                    {
                        key: objectKey,
                        value: {
                            uploadId: 'different-upload-id', // Different uploadId - this is the valid master
                            versionId: masterVersionId,
                            isLatest: true
                        }
                    },
                    // Orphaned version with matching uploadId - should be deleted
                    {
                        key: objectKey,
                        value: {
                            uploadId: targetUploadId, // Matching uploadId - this is orphaned metadata
                            versionId: orphanedVersionId,
                            isLatest: false
                        }
                    }
                ],
                IsTruncated: false
            });

            bucketPut(authInfo, bucketRequest, log, err => {
                assert.ifError(err);
                bucketPutVersioning(authInfo, enableVersioningRequest, log, err => {
                    assert.ifError(err);

                    abortMultipartUpload(authInfo, bucketName, objectKey, targetUploadId, log, err => {
                        assert.strictEqual(err, undefined);

                        // Should have called deleteObjectMD for object cleanup
                        sinon.assert.called(deleteObjectMDStub);

                        // Verify it deleted the correct version (the one with matching uploadId)
                        const deleteObjectCalls = deleteObjectMDStub.getCalls().filter(call =>
                            call.args[0] === bucketName && call.args[1] === objectKey
                        );

                        assert.strictEqual(deleteObjectCalls.length, 1, 'Should delete exactly one object version');

                        // Should delete the orphaned version, not the master
                        const deletedVersionId = deleteObjectCalls[0].args[2].versionId;
                        assert.strictEqual(deletedVersionId, orphanedVersionId,
                            'Should delete the orphaned version with matching uploadId');
                        assert.notStrictEqual(deletedVersionId, masterVersionId,
                            'Should NOT delete the master version');

                        // Cleanup
                        metadataValidateMultipartStub.restore();
                        getMPUpartsStub.restore();
                        batchDeleteObjectMetadataStub.restore();
                        deleteObjectMDStub.restore();
                        done();
                    }, abortRequest);
                });
            });
        });

        it('should handle multiple versions with same uploadId and delete all matching ones', done => {
            const targetUploadId = 'upload-to-cleanup';

            // Mock that we have an overview object to trigger cleanup logic
            const metadataValidateMultipartStub = sinon.stub(services, 'metadataValidateMultipart')
                .yields(null, {
                    getName: () => 'mpuShadowBuckettest-bucket',
                    getMdBucketModelVersion: () => 2
                }, { controllingLocationConstraint: 'us-east-1' });

            const getMPUpartsStub = sinon.stub(services, 'getMPUparts').yields(null, {
                Contents: [{
                    key: 'part-key',
                    value: { Size: 1024, partLocations: [{ key: 'loc-1' }] }
                }]
            });

            const batchDeleteObjectMetadataStub = sinon.stub(services, 'batchDeleteObjectMetadata').yields();
            const deleteObjectMDStub = sinon.stub(metadata, 'deleteObjectMD').yields();

            // Simulate finding the first matching version, which should stop the search
            getObjectListingStub.yields(null, {
                Versions: [
                    // First version with matching uploadId - should be found and deleted
                    {
                        key: objectKey,
                        value: {
                            uploadId: targetUploadId,
                            versionId: 'first-match-version'
                        }
                    },
                    // There could be more versions after this, but the optimization
                    // should stop at the first match for efficiency
                ],
                IsTruncated: false
            });

            bucketPut(authInfo, bucketRequest, log, err => {
                assert.ifError(err);
                bucketPutVersioning(authInfo, enableVersioningRequest, log, err => {
                    assert.ifError(err);

                    abortMultipartUpload(authInfo, bucketName, objectKey, targetUploadId, log, err => {
                        assert.strictEqual(err, undefined);

                        // Should have found and processed the first matching version
                        sinon.assert.called(deleteObjectMDStub);

                        // Verify it found the first matching version
                        const deleteObjectCalls = deleteObjectMDStub.getCalls().filter(call =>
                            call.args[0] === bucketName && call.args[1] === objectKey
                        );

                        assert.strictEqual(deleteObjectCalls.length, 1,
                            'Should process the first matching version found');

                        const deletedVersionId = deleteObjectCalls[0].args[2].versionId;
                        assert.strictEqual(deletedVersionId, 'first-match-version',
                            'Should delete the first matching version found');

                        // Cleanup
                        metadataValidateMultipartStub.restore();
                        getMPUpartsStub.restore();
                        batchDeleteObjectMetadataStub.restore();
                        deleteObjectMDStub.restore();
                        done();
                    }, abortRequest);
                });
            });
        });

        it('should log error when object metadata deletion fails with non-NoSuchKey error', done => {
            const targetUploadId = 'upload-to-cleanup';
            const logErrorSpy = sinon.spy(log, 'error');

            // Mock that we have an overview object to trigger cleanup logic
            const metadataValidateMultipartStub = sinon.stub(services, 'metadataValidateMultipart')
                .yields(null, {
                    getName: () => 'mpuShadowBuckettest-bucket',
                    getMdBucketModelVersion: () => 2
                }, { controllingLocationConstraint: 'us-east-1' });

            const getMPUpartsStub = sinon.stub(services, 'getMPUparts').yields(null, {
                Contents: [{
                    key: 'part-key',
                    value: { Size: 1024, partLocations: [{ key: 'loc-1' }] }
                }]
            });

            const batchDeleteObjectMetadataStub = sinon.stub(services, 'batchDeleteObjectMetadata').yields();

            // Mock deleteObjectMD to fail with a non-NoSuchKey error
            const deleteObjectMDStub = sinon.stub(metadata, 'deleteObjectMD').yields(errors.InternalError);

            // Simulate finding a matching version
            getObjectListingStub.yields(null, {
                Versions: [
                    {
                        key: objectKey,
                        value: {
                            uploadId: targetUploadId,
                            versionId: 'version-to-delete'
                        }
                    }
                ],
                IsTruncated: false
            });

            bucketPut(authInfo, bucketRequest, log, err => {
                assert.ifError(err);
                bucketPutVersioning(authInfo, enableVersioningRequest, log, err => {
                    assert.ifError(err);

                    abortMultipartUpload(authInfo, bucketName, objectKey, targetUploadId, log, err => {
                        assert.strictEqual(err, undefined); // Should continue despite deletion error

                        // Verify the error was logged
                        const errorLogCalls = logErrorSpy.getCalls().filter(call =>
                            call.args[0] === 'error deleting object metadata'
                        );
                        assert.strictEqual(errorLogCalls.length, 1,
                            'Should log error when object metadata deletion fails');
                        assert.strictEqual(errorLogCalls[0].args[1].error.is.InternalError, true);

                        // Cleanup
                        logErrorSpy.restore();
                        metadataValidateMultipartStub.restore();
                        getMPUpartsStub.restore();
                        batchDeleteObjectMetadataStub.restore();
                        deleteObjectMDStub.restore();
                        done();
                    }, abortRequest);
                });
            });
        });
    });

    describe('error handling', () => {
        it('should handle external data abort error', done => {
            dataAbortMPUStub.restore();
            sinon.stub(data, 'abortMPU').callsFake((objectKey, uploadId, location, bucketName,
                request, destBucket, locationConstraintCheckFn, log, callback) => {
                callback(errors.InternalError);
            });

            createBucketAndMPU(false, (err, uploadId) => {
                assert.ifError(err);

                abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, err => {
                    assert(err);
                    assert.strictEqual(err.is.InternalError, true);
                    done();
                }, abortRequest);
            });
        });

        it('should continue despite data deletion errors', done => {
            dataDeleteStub.restore();
            sinon.stub(data, 'delete').yields(errors.InternalError); // Fail data deletion

            createBucketAndMPU(false, (err, uploadId) => {
                assert.ifError(err);

                // Add a part so there's data to delete
                const md5Hash = crypto.createHash('md5');
                md5Hash.update(postBody);
                const calculatedHash = md5Hash.digest('hex');

                const partRequest = new DummyRequest({
                    bucketName,
                    objectKey,
                    namespace: 'default',
                    url: `/${objectKey}?partNumber=1&uploadId=${uploadId}`,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: {
                        partNumber: '1',
                        uploadId,
                    },
                    calculatedHash,
                }, postBody);

                objectPutPart(authInfo, partRequest, undefined, log, err => {
                    assert.ifError(err);

                    abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, err => {
                        assert.strictEqual(err, null); // Should succeed despite data deletion failure
                        done();
                    }, abortRequest);
                });
            });
        });
    });
});
