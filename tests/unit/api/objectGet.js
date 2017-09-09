const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const { parseString } = require('xml2js');

const { bucketPut } = require('../../../lib/api/bucketPut');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const completeMultipartUpload
    = require('../../../lib/api/completeMultipartUpload');
const DummyRequest = require('../DummyRequest');
const initiateMultipartUpload
    = require('../../../lib/api/initiateMultipartUpload');
const objectPut = require('../../../lib/api/objectPut');
const objectGet = require('../../../lib/api/objectGet');
const objectPutPart = require('../../../lib/api/objectPutPart');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const objectName = 'objectName';
const postBody = Buffer.from('I am a body', 'utf8');

describe('objectGet API', () => {
    let testPutObjectRequest;

    beforeEach(() => {
        cleanup();
        testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'x-amz-meta-test': 'some metadata',
                'content-length': '12',
            },
            parsedContentLength: 12,
            url: `/${bucketName}/${objectName}`,
        }, postBody);
    });

    const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
    const testPutBucketRequest = {
        bucketName,
        namespace,
        headers: {},
        url: `/${bucketName}`,
    };
    const userMetadataKey = 'x-amz-meta-test';
    const userMetadataValue = 'some metadata';
    const testGetRequest = {
        bucketName,
        namespace,
        objectKey: objectName,
        headers: {},
        url: `/${bucketName}/${objectName}`,
    };

    it('should get the object metadata', done => {
        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined,
                log, (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectGet(authInfo, testGetRequest, false,
                        log, (err, result, responseMetaHeaders) => {
                            assert.strictEqual(
                                responseMetaHeaders[userMetadataKey],
                                userMetadataValue);
                            assert.strictEqual(responseMetaHeaders.ETag,
                                `"${correctMD5}"`);
                            done();
                        });
                });
        });
    });

    it('should get the object data retrieval info', done => {
        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectGet(authInfo, testGetRequest, false, log,
                        (err, dataGetInfo) => {
                            assert.deepStrictEqual(dataGetInfo,
                                [{
                                    key: 1,
                                    start: 0,
                                    size: 12,
                                    dataStoreName: 'mem',
                                    dataStoreETag: `1:${correctMD5}`,
                                }]);
                            done();
                        });
                });
        });
    });

    it('should get the object data retrieval info for an object put by MPU',
        done => {
            const partBody = Buffer.from('I am a part\n', 'utf8');
            const initiateRequest = {
                bucketName,
                namespace,
                objectKey: objectName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectName}?uploads`,
            };
            async.waterfall([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                (corsHeaders, next) => initiateMultipartUpload(authInfo,
                    initiateRequest, log, next),
                (result, corsHeaders, next) => parseString(result, next),
                (json, next) => {
                    const testUploadId =
                    json.InitiateMultipartUploadResult.UploadId[0];
                    const md5Hash = crypto.createHash('md5').update(partBody);
                    const calculatedHash = md5Hash.digest('hex');
                    const partRequest = new DummyRequest({
                        bucketName,
                        namespace,
                        objectKey: objectName,
                        headers: {
                            'host': `${bucketName}.s3.amazonaws.com`,
                            // Part (other than last part) must be at least 5MB
                            'content-length': '5242880',
                        },
                        parsedContentLength: 5242880,
                        url: `/${objectName}?partNumber=1&uploadId` +
                            `=${testUploadId}`,
                        query: {
                            partNumber: '1',
                            uploadId: testUploadId,
                        },
                        calculatedHash,
                    }, partBody);
                    objectPutPart(authInfo, partRequest, undefined, log, () => {
                        next(null, testUploadId, calculatedHash);
                    });
                },
                (testUploadId, calculatedHash, next) => {
                    const part2Request = new DummyRequest({
                        bucketName,
                        namespace,
                        objectKey: objectName,
                        headers: {
                            'host': `${bucketName}.s3.amazonaws.com`,
                            'content-length': '12',
                        },
                        parsedContentLength: 12,
                        url: `/${objectName}?partNumber=2&uploadId=` +
                            `${testUploadId}`,
                        query: {
                            partNumber: '2',
                            uploadId: testUploadId,
                        },
                        calculatedHash,
                    }, partBody);
                    objectPutPart(authInfo, part2Request, undefined,
                        log, () => {
                            next(null, testUploadId, calculatedHash);
                        });
                },
                (testUploadId, calculatedHash, next) => {
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${calculatedHash}"</ETag>` +
                        '</Part>' +
                        '<Part>' +
                        '<PartNumber>2</PartNumber>' +
                        `<ETag>"${calculatedHash}"</ETag>` +
                        '</Part>' +
                        '</CompleteMultipartUpload>';
                    const completeRequest = {
                        bucketName,
                        namespace,
                        objectKey: objectName,
                        parsedHost: 's3.amazonaws.com',
                        url: `/${objectName}?uploadId=${testUploadId}`,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        query: { uploadId: testUploadId },
                        post: completeBody,
                    };
                    completeMultipartUpload(authInfo, completeRequest,
                                            log, err => {
                                                next(err, calculatedHash);
                                            });
                },
            ],
            (err, calculatedHash) => {
                assert.strictEqual(err, null);
                objectGet(authInfo, testGetRequest, false, log,
                (err, dataGetInfo) => {
                    assert.strictEqual(err, null);
                    assert.deepStrictEqual(dataGetInfo,
                        [{
                            key: 1,
                            dataStoreName: 'mem',
                            dataStoreETag: `1:${calculatedHash}`,
                            size: 5242880,
                            start: 0,
                        },
                        {
                            key: 2,
                            dataStoreName: 'mem',
                            dataStoreETag: `2:${calculatedHash}`,
                            size: 12,
                            start: 5242880,
                        }]);
                    done();
                });
            });
        });

    it('should get a 0 bytes object', done => {
        const postBody = '';
        const correctMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'content-length': '0',
                'x-amz-meta-test': 'some metadata',
            },
            parsedContentLength: 0,
            url: `/${bucketName}/${objectName}`,
            calculatedHash: 'd41d8cd98f00b204e9800998ecf8427e',
        }, postBody);
        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectGet(authInfo, testGetRequest, false,
                    log, (err, result, responseMetaHeaders) => {
                        assert.strictEqual(result, null);
                        assert.strictEqual(
                            responseMetaHeaders[userMetadataKey],
                            userMetadataValue);
                        assert.strictEqual(responseMetaHeaders.ETag,
                            `"${correctMD5}"`);
                        done();
                    });
                });
        });
    });
});
