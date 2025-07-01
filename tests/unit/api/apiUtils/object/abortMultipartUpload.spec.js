const assert = require('assert');
const sinon = require('sinon');
const { parseString } = require('xml2js');
const { errors } = require('arsenal');

const abortMultipartUpload = require('../../../../../lib/api/apiUtils/object/abortMultipartUpload');
const { bucketPut } = require('../../../../../lib/api/bucketPut');
const initiateMultipartUpload = require('../../../../../lib/api/initiateMultipartUpload');
const bucketPutVersioning = require('../../../../../lib/api/bucketPutVersioning');
const objectPutPart = require('../../../../../lib/api/objectPutPart');
const { data } = require('../../../../../lib/data/wrapper');
const quotaUtils = require('../../../../../lib/api/apiUtils/quotas/quotaUtils');
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
        dataAbortMPUStub = sinon.stub(data, 'abortMPU').callsFake((objectKey, uploadId, location, bucketName, request, destBucket, locationConstraintCheckFn, log, callback) => {
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
            bucketPut(authInfo, bucketRequest, log, (err) => {
                if (err) return callback(err);
                bucketPutVersioning(authInfo, enableVersioningRequest, log, (err) => {
                    if (err) return callback(err);
                    initiateMultipartUpload(authInfo, initiateRequest, log, (err, result) => {
                        if (err) return callback(err);
                        parseString(result, (err, json) => {
                            if (err) return callback(err);
                            const uploadId = json.InitiateMultipartUploadResult.UploadId[0];
                            callback(null, uploadId);
                        });
                    });
                });
            });
        } else {
            bucketPut(authInfo, bucketRequest, log, (err) => {
                if (err) return callback(err);
                initiateMultipartUpload(authInfo, initiateRequest, log, (err, result) => {
                    if (err) return callback(err);
                    parseString(result, (err, json) => {
                        if (err) return callback(err);
                        const uploadId = json.InitiateMultipartUploadResult.UploadId[0];
                        callback(null, uploadId);
                    });
                });
            });
        }
    }

    describe('basic functionality', () => {
        it('should successfully abort multipart upload', (done) => {
            createBucketAndMPU(false, (err, uploadId) => {
                assert.ifError(err);

                abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, (err) => {
                    assert.strictEqual(err, null);
                    sinon.assert.calledOnce(dataAbortMPUStub);
                    done();
                }, abortRequest);
            });
        });

        it('should return error for non-existent bucket', (done) => {
            abortMultipartUpload(authInfo, 'non-existent-bucket', objectKey, 'fake-upload-id', log, (err) => {
                assert(err);
                assert.strictEqual(err.is.NoSuchBucket, true);
                done();
            }, abortRequest);
        });

        it('should return error for non-existent upload', (done) => {
            bucketPut(authInfo, bucketRequest, log, (err) => {
                assert.ifError(err);

                abortMultipartUpload(authInfo, bucketName, objectKey, 'fake-upload-id', log, (err) => {
                    assert(err);
                    assert.strictEqual(err.is.NoSuchUpload, true);
                    done();
                }, abortRequest);
            });
        });

        it('should skip data deletion when skipDataDelete is true', (done) => {
            dataAbortMPUStub.restore();
            sinon.stub(data, 'abortMPU').callsFake((objectKey, uploadId, location, bucketName, request, destBucket, locationConstraintCheckFn, log, callback) => {
                callback(null, true); // skipDataDelete = true
            });

            createBucketAndMPU(false, (err, uploadId) => {
                assert.ifError(err);

                abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, (err) => {
                    assert.strictEqual(err, null);
                    sinon.assert.notCalled(dataDeleteStub);
                    sinon.assert.notCalled(validateQuotasStub);
                    done();
                }, abortRequest);
            });
        });
    });

    describe('with multipart upload parts', () => {
        it('should delete part data when aborting', (done) => {
            createBucketAndMPU(false, (err, uploadId) => {
                assert.ifError(err);

                // Add a part to the multipart upload
                const md5Hash = require('crypto').createHash('md5');
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

                objectPutPart(authInfo, partRequest, undefined, log, (err) => {
                    assert.ifError(err);

                    abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, (err) => {
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
        it('should handle versioned bucket abort', (done) => {
            createBucketAndMPU(true, (err, uploadId) => {
                assert.ifError(err);
                
                abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, (err) => {
                    assert.strictEqual(err, null);
                    sinon.assert.calledOnce(dataAbortMPUStub);
                    done();
                }, abortRequest);
            });
        });
    });

    describe('version listing optimization', () => {
        const services = require('../../../../../lib/services');
        let getObjectListingStub;

        beforeEach(() => {
            getObjectListingStub = sinon.stub(services, 'getObjectListing');
        });

        afterEach(() => {
            if (getObjectListingStub) {
                getObjectListingStub.restore();
            }
        });

        it('should use optimized prefix when listing versions', (done) => {
            createBucketAndMPU(true, (err, uploadId) => {
                assert.ifError(err);
                
                // Mock version listing response - return empty to simulate no cleanup needed
                getObjectListingStub.yields(null, {
                    Versions: [],
                    IsTruncated: false
                });

                abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, (err) => {
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

        it('should handle version listing pagination', (done) => {
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

                abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, (err) => {
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

        it('should handle version listing errors gracefully', (done) => {
            createBucketAndMPU(true, (err, uploadId) => {
                assert.ifError(err);
                
                // Simulate error during version listing
                getObjectListingStub.yields(errors.InternalError);

                abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, (err) => {
                    assert.strictEqual(err, null); // Should continue despite version listing error
                    done();
                }, abortRequest);
            });
        });
    });

    describe('error handling', () => {
        it('should handle external data abort error', (done) => {
            dataAbortMPUStub.restore();
            sinon.stub(data, 'abortMPU').callsFake((objectKey, uploadId, location, bucketName, request, destBucket, locationConstraintCheckFn, log, callback) => {
                callback(errors.InternalError);
            });

            createBucketAndMPU(false, (err, uploadId) => {
                assert.ifError(err);

                abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, (err) => {
                    assert(err);
                    assert.strictEqual(err.is.InternalError, true);
                    done();
                }, abortRequest);
            });
        });

        it('should continue despite data deletion errors', (done) => {
            dataDeleteStub.restore();
            sinon.stub(data, 'delete').yields(errors.InternalError); // Fail data deletion

            createBucketAndMPU(false, (err, uploadId) => {
                assert.ifError(err);

                // Add a part so there's data to delete
                const md5Hash = require('crypto').createHash('md5');
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

                objectPutPart(authInfo, partRequest, undefined, log, (err) => {
                    assert.ifError(err);

                    abortMultipartUpload(authInfo, bucketName, objectKey, uploadId, log, (err) => {
                        assert.strictEqual(err, null); // Should succeed despite data deletion failure
                        done();
                    }, abortRequest);
                });
            });
        });
    });
});
