import assert from 'assert';
import crypto from 'crypto';

import bucketPut from '../../../lib/api/bucketPut';
import metadata from '../metadataswitch';
import objectPut from '../../../lib/api/objectPut';
import objectGet from '../../../lib/api/objectGet';
import DummyRequestLogger from '../helpers';

const log = new DummyRequestLogger();
const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = [ new Buffer('I am a body'), ];

describe('objectGet API', () => {
    let metastore;

    beforeEach((done) => {
        metastore = {
            "users": {
                "accessKey1": {
                    "buckets": []
                },
                "accessKey2": {
                    "buckets": []
                }
            },
        };
        metadata.deleteBucket(bucketName, log, () => done());
    });

    after((done) => {
        metadata.deleteBucket(bucketName, log, () => done());
    });

    const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
    const objectName = 'objectName';
    const testPutBucketRequest = {
        bucketName,
        namespace,
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
    };
    const userMetadataKey = 'x-amz-meta-test';
    const userMetadataValue = 'some metadata';
    const testPutObjectRequest = {
        bucketName,
        namespace,
        objectKey: objectName,
        lowerCaseHeaders: {
            'x-amz-meta-test': 'some metadata'
        },
        url: `/${bucketName}/${objectName}`,
        post: postBody,
        calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
    };
    const testGetRequest = {
        bucketName,
        namespace,
        objectKey: objectName,
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName}`,
    };

    it("should get the object metadata", (done) => {
        bucketPut(accessKey, metastore, testPutBucketRequest, log,
            (err, res) => {
                assert.strictEqual(res, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, log, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectGet(accessKey, metastore, testGetRequest,
                            log, (err, result, responseMetaHeaders) => {
                                assert.strictEqual(responseMetaHeaders
                                    [userMetadataKey], userMetadataValue);
                                assert.strictEqual(responseMetaHeaders.ETag,
                                    `"${correctMD5}"`);
                                done();
                            });
                    });
            });
    });

    it('should get the object data', (done) => {
        bucketPut(accessKey, metastore, testPutBucketRequest, log,
            (err, res) => {
                assert.strictEqual(res, 'Bucket created');
                objectPut(accessKey, metastore, testPutObjectRequest, log,
                    (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectGet(accessKey, metastore, testGetRequest, log,
                            (err, readable) => {
                                const chunks = [];
                                readable.on('data', function chunkRcvd(chunk) {
                                    chunks.push(chunk);
                                });
                                readable.on('end', function combineChunks() {
                                    assert.deepStrictEqual(chunks, postBody);
                                    done();
                                });
                            });
                    });
            });
    });

    it('should get the object data for large objects', (done) => {
        const testBigData = crypto.randomBytes(1000000);
        const correctBigMD5 =
            crypto.createHash('md5').update(testBigData).digest('base64');

        const testPutBigObjectRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            lowerCaseHeaders: {
                'x-amz-meta-test': 'some metadata'
            },
            url: `/${bucketName}/${objectName}`,
            post: [ testBigData ],
            calculatedMD5: correctBigMD5
        };
        bucketPut(accessKey, metastore, testPutBucketRequest, log,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore, testPutBigObjectRequest, log,
                    (err, result) => {
                        assert.strictEqual(result, correctBigMD5);
                        objectGet(accessKey, metastore, testGetRequest, log,
                            (err, readable) => {
                                const md5Hash = crypto.createHash('md5');
                                const chunks = [];
                                readable.on('data', function chunkRcvd(chunk) {
                                    const cBuffer = new Buffer(chunk, "binary");
                                    chunks.push(cBuffer);
                                    md5Hash.update(cBuffer);
                                });
                                readable.on('end', function combineChunks() {
                                    const resultmd5Hash =
                                        md5Hash.digest('base64');
                                    assert.strictEqual(resultmd5Hash,
                                        correctBigMD5);
                                    done();
                                });
                            });
                    });
            });
    });
});
