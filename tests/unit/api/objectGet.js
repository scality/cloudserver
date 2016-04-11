import assert from 'assert';
import crypto from 'crypto';

import bucketPut from '../../../lib/api/bucketPut';
import { cleanup, DummyRequestLogger, makeAuthInfo } from '../helpers';
import objectPut from '../../../lib/api/objectPut';
import objectGet from '../../../lib/api/objectGet';
import DummyRequest from '../DummyRequest';

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
            headers: { 'x-amz-meta-test': 'some metadata' },
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
        bucketPut(authInfo, testPutBucketRequest, log, (err, res) => {
            assert.strictEqual(res, 'Bucket created');
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

    it('should get the object data', done => {
        bucketPut(authInfo, testPutBucketRequest, log, (err, res) => {
            assert.strictEqual(res, 'Bucket created');
            objectPut(authInfo, testPutObjectRequest, log, (err, result) => {
                assert.strictEqual(result, correctMD5);
                objectGet(authInfo, testGetRequest, log, (err, readable) => {
                    const chunks = [];
                    readable.on('data', chunk => {
                        chunks.push(chunk);
                    });
                    readable.on('end', () => {
                        assert.deepStrictEqual(chunks, [postBody]);
                        done();
                    });
                });
            });
        });
    });

    it('should get the object data for large objects', done => {
        const testBigData = crypto.randomBytes(1000000);
        const correctBigMD5 =
            crypto.createHash('md5').update(testBigData).digest('hex');

        const testPutBigObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-meta-test': 'some metadata' },
            url: `/${bucketName}/${objectName}`,
        }, testBigData);
        bucketPut(authInfo, testPutBucketRequest, log, (err, success) => {
            assert.strictEqual(success, 'Bucket created');
            objectPut(authInfo, testPutBigObjectRequest, log, (err, result) => {
                assert.strictEqual(result, correctBigMD5);
                objectGet(authInfo, testGetRequest, log, (err, readable) => {
                    const md5Hash = crypto.createHash('md5');
                    const chunks = [];
                    readable.on('data', chunk => {
                        const cBuffer = new Buffer(chunk, 'binary');
                        chunks.push(cBuffer);
                        md5Hash.update(cBuffer);
                    });
                    readable.on('end', () => {
                        const resultmd5Hash = md5Hash.digest('hex');
                        assert.strictEqual(resultmd5Hash, correctBigMD5);
                        done();
                    });
                });
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
        bucketPut(authInfo, testPutBucketRequest, log, (err, res) => {
            assert.strictEqual(res, 'Bucket created');
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
