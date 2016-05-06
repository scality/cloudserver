import assert from 'assert';
import async from 'async';
import crypto from 'crypto';
import { parseString } from 'xml2js';

import bucketPut from '../../../lib/api/bucketPut';
import { cleanup, DummyRequestLogger, makeAuthInfo } from '../helpers';
import completeMultipartUpload from '../../../lib/api/completeMultipartUpload';
import DummyRequest from '../DummyRequest';
import initiateMultipartUpload from '../../../lib/api/initiateMultipartUpload';
import objectPut from '../../../lib/api/objectPut';
import objectGet from '../../../lib/api/objectGet';
import objectPutPart from '../../../lib/api/objectPutPart';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const objectName = 'objectName';
const postBody = new Buffer('I am a body');

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
                'content-length': 12,
            },
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
            objectPut(authInfo, testPutObjectRequest, log, (err, result) => {
                assert.strictEqual(result, correctMD5);
                objectGet(authInfo, testGetRequest,
                    log, (err, result, responseMetaHeaders) => {
                        assert.strictEqual(responseMetaHeaders[userMetadataKey],
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
            objectPut(authInfo, testPutObjectRequest, log, (err, result) => {
                assert.strictEqual(result, correctMD5);
                objectGet(authInfo, testGetRequest, log, (err, dataGetInfo) => {
                    assert.deepStrictEqual(dataGetInfo,
                        [{
                            key: 1,
                            start: 0,
                            size: 12,
                            dataStoreName: 'mem',
                        }]);
                    done();
                });
            });
        });
    });

    it('should get the object data retrieval info for an object put by MPU',
        done => {
            const partBody = new Buffer('I am a part\n');
            const initiateRequest = {
                bucketName,
                namespace,
                objectKey: objectName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectName}?uploads`,
            };
            async.waterfall([
                function waterfall1(next) {
                    bucketPut(authInfo, testPutBucketRequest, log, next);
                },
                function waterfall2(next) {
                    initiateMultipartUpload(
                        authInfo, initiateRequest, log, next);
                },
                function waterfall3(result, next) {
                    parseString(result, next);
                },
                function waterfall4(json, next) {
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
                        url: `/${objectName}?partNumber=1&uploadId` +
                            `=${testUploadId}`,
                        query: {
                            partNumber: '1',
                            uploadId: testUploadId,
                        },
                        calculatedHash,
                    }, partBody);
                    objectPutPart(authInfo, partRequest, log, () => {
                        next(null, testUploadId, calculatedHash);
                    });
                },
                function waterfall5(testUploadId, calculatedHash, next) {
                    const part2Request = new DummyRequest({
                        bucketName,
                        namespace,
                        objectKey: objectName,
                        headers: {
                            'host': `${bucketName}.s3.amazonaws.com`,
                            'content-length': '12',
                        },
                        url: `/${objectName}?partNumber=2&uploadId=` +
                            `${testUploadId}`,
                        query: {
                            partNumber: '2',
                            uploadId: testUploadId,
                        },
                        calculatedHash,
                    }, partBody);
                    objectPutPart(authInfo, part2Request, log, () => {
                        next(null, testUploadId, calculatedHash);
                    });
                },
                function waterfall6(testUploadId, calculatedHash, next) {
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
                        log, next);
                },
            ],
    function waterfallFinal() {
        objectGet(authInfo, testGetRequest, log, (err, dataGetInfo) => {
            assert.deepStrictEqual(dataGetInfo,
                [{
                    key: 1,
                    dataStoreName: 'mem',
                    size: 5242880,
                    start: 0,
                },
                {
                    key: 2,
                    dataStoreName: 'mem',
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
            url: `/${bucketName}/${objectName}`,
            calculatedHash: 'd41d8cd98f00b204e9800998ecf8427e',
        }, postBody);
        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, log, (err, result) => {
                assert.strictEqual(result, correctMD5);
                objectGet(authInfo, testGetRequest,
                    log, (err, result, responseMetaHeaders) => {
                        assert.strictEqual(result, null);
                        assert.strictEqual(responseMetaHeaders
                            [userMetadataKey], userMetadataValue);
                        assert.strictEqual(responseMetaHeaders.ETag,
                            `"${correctMD5}"`);
                        done();
                    });
            });
        });
    });
});
