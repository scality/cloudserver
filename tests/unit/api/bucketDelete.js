const crypto = require('crypto');
const assert = require('assert');
const async = require('async');
const { parseString } = require('xml2js');
const { errors } = require('arsenal');

const bucketDelete = require('../../../lib/api/bucketDelete');
const { bucketPut } = require('../../../lib/api/bucketPut');
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

describe('bucketDelete API', () => {
    beforeEach(() => {
        cleanup();
    });

    const testRequest = {
        bucketName,
        namespace,
        headers: {},
        url: `/${bucketName}`,
    };

    const objectName = 'objectName';
    const initiateRequest = {
        bucketName,
        namespace,
        objectKey: objectName,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: `/${objectName}?uploads`,
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
                    assert.deepStrictEqual(err, errors.BucketNotEmpty);
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

    it('should return an error if the bucket has an initiated mpu', done => {
        bucketPut(authInfo, testRequest, log, err => {
            assert.strictEqual(err, null);
            initiateMultipartUpload(authInfo, initiateRequest, log, err => {
                assert.strictEqual(err, null);
                bucketDelete(authInfo, testRequest, log, err => {
                    assert.deepStrictEqual(err, errors.MPUinProgress);
                    done();
                });
            });
        });
    });

    it('should delete a bucket if only part object (and no overview ' +
        'objects) is in mpu shadow bucket', done => {
        const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;
        const postBody = Buffer.from('I am a body', 'utf8');
        async.waterfall([
            next => bucketPut(authInfo, testRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => {
                parseString(result, next);
            },
        ],
        (err, json) => {
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
                assert.strictEqual(err, null);
                const mpuBucketKeyMap =
                    metadataMem.metadata.keyMaps.get(mpuBucket);
                assert.strictEqual(mpuBucketKeyMap.size, 2);
                const overviewKey = `overview${constants.splitter}` +
                    `${objectName}${constants.splitter}${testUploadId}`;
                // remove overview key from in mem mpu bucket
                mpuBucketKeyMap.delete(overviewKey);
                assert.strictEqual(mpuBucketKeyMap.size, 1);
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
                    assert.deepStrictEqual(err, errors.NoSuchBucket);
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

    it('should prevent anonymous user delete bucket API access', done => {
        const publicAuthInfo = makeAuthInfo(constants.publicId);
        bucketDelete(publicAuthInfo, testRequest, log, err => {
            assert.deepStrictEqual(err, errors.AccessDenied);
            done();
        });
    });
});
