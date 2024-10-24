const crypto = require('crypto');
const assert = require('assert');
const async = require('async');
const { parseString } = require('xml2js');
const { errors } = require('arsenal');
const sinon = require('sinon');

const inMemory = require('../../../lib/kms/in_memory/backend').backend;
const bucketDelete = require('../../../lib/api/bucketDelete');
const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutEncryption = require('../../../lib/api/bucketPutEncryption');
const { templateSSEConfig, templateRequest } = require('../utils/bucketEncryption');
const constants = require('../../../constants');
const initiateMultipartUpload
    = require('../../../lib/api/initiateMultipartUpload');
const metadata = require('../metadataswitch');
const metadataMem = require('arsenal').storage.metadata.inMemory.metadata;
const objectPut = require('../../../lib/api/objectPut');
const objectPutPart = require('../../../lib/api/objectPutPart');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const DummyRequest = require('../DummyRequest');


const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = Buffer.from('I am a body', 'utf8');
const usersBucket = constants.usersBucket;
const objectName = 'objectName';
const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;

function createMPU(testRequest, initiateRequest, deleteOverviewMPUObj, cb) {
    async.waterfall([
        next => bucketPut(authInfo, testRequest, log, next),
        (corsHeaders, next) => initiateMultipartUpload(authInfo,
            initiateRequest, log, next),
        (result, corsHeaders, next) => {
            parseString(result, next);
        },
        (json, next) => {
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                objectKey: objectName,
                namespace,
                url: `/${objectName}?partNumber=1&uploadId=${testUploadId}`,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, err => {
                if (err) {
                    return next(err);
                }
                return next(null, testUploadId);
            });
        },
    ], (err, testUploadId) => {
        assert.strictEqual(err, null);
        const mpuBucketKeyMap =
            metadataMem.metadata.keyMaps.get(mpuBucket);
        assert.strictEqual(mpuBucketKeyMap.size, 2);
        if (deleteOverviewMPUObj) {
            const overviewKey = `overview${constants.splitter}` +
            `${objectName}${constants.splitter}${testUploadId}`;
            // remove overview key from in mem mpu bucket
            mpuBucketKeyMap.delete(overviewKey);
            assert.strictEqual(mpuBucketKeyMap.size, 1);
        }
        bucketDelete(authInfo, testRequest, log, err => {
            assert.strictEqual(err, null);
            cb();
        });
    });
}

describe('bucketDelete API', () => {
    beforeEach(() => {
        cleanup();
    });

    const testRequest = {
        bucketName,
        namespace,
        headers: {},
        url: `/${bucketName}`,
        actionImplicitDenies: false,
    };

    const initiateRequest = {
        bucketName,
        namespace,
        objectKey: objectName,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: `/${objectName}?uploads`,
        actionImplicitDenies: false,
    };

    it('should return an error if the bucket is not empty', done => {
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            objectKey: objectName,
        }, postBody);

        bucketPut(authInfo, testRequest, log, err => {
            assert.strictEqual(err, null);
            objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
                assert.strictEqual(err, null);
                bucketDelete(authInfo, testRequest, log, err => {
                    assert.strictEqual(err.is.BucketNotEmpty, true);
                    metadata.getBucket(bucketName, log, (err, md) => {
                        assert.strictEqual(md.getName(), bucketName);
                        metadata.listObject(usersBucket,
                            { prefix: authInfo.getCanonicalID() },
                            log, (err, listResponse) => {
                                assert.strictEqual(listResponse.Contents.length,
                                                   1);
                                done();
                            });
                    });
                });
            });
        });
    });

    it('should not return an error if the bucket has an initiated mpu',
    done => {
        bucketPut(authInfo, testRequest, log, err => {
            assert.strictEqual(err, null);
            initiateMultipartUpload(authInfo, initiateRequest, log, err => {
                assert.strictEqual(err, null);
                bucketDelete(authInfo, testRequest, log, err => {
                    assert.strictEqual(err, null);
                    done();
                });
            });
        });
    });

    it('should delete a bucket', done => {
        bucketPut(authInfo, testRequest, log, () => {
            bucketDelete(authInfo, testRequest, log, () => {
                metadata.getBucket(bucketName, log, (err, md) => {
                    assert.strictEqual(err.is.NoSuchBucket, true);
                    assert.strictEqual(md, undefined);
                    metadata.listObject(usersBucket, { prefix: canonicalID },
                        log, (err, listResponse) => {
                            assert.strictEqual(listResponse.Contents.length, 0);
                            done();
                        });
                });
            });
        });
    });

    it('should delete a bucket even if the bucket has ongoing mpu',
        done => createMPU(testRequest, initiateRequest, false, done));

    // if only part object (and no overview objects) is in mpu shadow bucket
    it('should delete a bucket even if the bucket has an orphan part',
        done => createMPU(testRequest, initiateRequest, true, done));


    it('should prevent anonymous user delete bucket API access', done => {
        const publicAuthInfo = makeAuthInfo(constants.publicId);
        bucketDelete(publicAuthInfo, testRequest, log, err => {
            assert.strictEqual(err.is.AccessDenied, true);
            done();
        });
    });

    describe('with encryption', () => {
        let destroyBucketKeySpy;

        beforeEach(() => {
            destroyBucketKeySpy = sinon.spy(inMemory, 'destroyBucketKey');
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should delete the bucket-level encryption key if AES256 algorithm', done => {
            bucketPut(authInfo, testRequest, log, () => {
                const post = templateSSEConfig({ algorithm: 'AES256' });
                bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                    assert.ifError(err);
                    bucketDelete(authInfo, testRequest, log, () => {
                        metadata.getBucket(bucketName, log, (err, md) => {
                            assert.strictEqual(err.is.NoSuchBucket, true);
                            assert.strictEqual(md, undefined);
                            // delete the default bucket-level master encryption key
                            sinon.assert.calledOnce(destroyBucketKeySpy);
                            done();
                        });
                    });
                });
            });
        });

        it('should not delete the bucket-level encryption key if aws:kms algorithm', done => {
            bucketPut(authInfo, testRequest, log, () => {
                const post = templateSSEConfig({ algorithm: 'aws:kms' });
                bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                    assert.ifError(err);
                    bucketDelete(authInfo, testRequest, log, () => {
                        metadata.getBucket(bucketName, log, (err, md) => {
                            assert.strictEqual(err.is.NoSuchBucket, true);
                            assert.strictEqual(md, undefined);
                            // do not delete the default bucket-level master encryption key
                            sinon.assert.notCalled(destroyBucketKeySpy);
                            done();
                        });
                    });
                });
            });
        });

        it('should not delete the account-level encryption key', done => {
            sinon.stub(inMemory, 'supportsDefaultKeyPerAccount').value(true);
            bucketPut(authInfo, testRequest, log, () => {
                const post = templateSSEConfig({ algorithm: 'AES256' });
                bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                    assert.ifError(err);
                    bucketDelete(authInfo, testRequest, log, () => {
                        metadata.getBucket(bucketName, log, (err, md) => {
                            assert.strictEqual(err.is.NoSuchBucket, true);
                            assert.strictEqual(md, undefined);
                            // do not delete the default bucket-level master encryption key
                            sinon.assert.notCalled(destroyBucketKeySpy);
                            done();
                        });
                    });
                });
            });
        });
    });

    describe('with failed encryption', () => {
        beforeEach(() => {
            sinon.stub(inMemory, 'destroyBucketKey').callsFake((bucketKeyId, log, cb) => cb(errors.InternalError));
        });

        afterEach(() => {
            sinon.restore();
            cleanup();
        });

        it('should fail deleting the bucket-level encryption key', done => {
            bucketPut(authInfo, testRequest, log, () => {
                const post = templateSSEConfig({ algorithm: 'AES256' });
                bucketPutEncryption(authInfo, templateRequest(bucketName, { post }), log, err => {
                    assert.ifError(err);
                    bucketDelete(authInfo, testRequest, log, err => {
                        assert(err && err.InternalError);
                        done();
                    });
                });
            });
        });
    });
});
