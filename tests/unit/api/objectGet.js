import assert from 'assert';
import crypto from 'crypto';

import bucketPut from '../../../lib/api/bucketPut';
import metadata from '../../../lib/metadata/wrapper';
import objectPut from '../../../lib/api/objectPut';
import objectGet from '../../../lib/api/objectGet';
import utils from '../../../lib/utils';

const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
const testBucketUID = utils.getResourceUID(namespace, bucketName);
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
            "buckets": {}
        };
        metadata.deleteBucket(testBucketUID, ()=> {
            done();
        });
    });

    after((done) => {
        metadata.deleteBucket(testBucketUID, ()=> {
            done();
        });
    });

    const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
    const objectName = 'objectName';
    const testPutBucketRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace: namespace,
    };
    const userMetadataKey = 'x-amz-meta-test';
    const userMetadataValue = 'some metadata';
    const testPutObjectRequest = {
        lowerCaseHeaders: {
            'x-amz-meta-test': 'some metadata'
        },
        url: `/${bucketName}/${objectName}`,
        namespace: namespace,
        post: postBody,
        calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
    };

    it("should get the object metadata", (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest, (err, res) => {
            assert.strictEqual(res, 'Bucket created');
            objectPut(accessKey, metastore,
                testPutObjectRequest, (err, result) => {
                    assert.strictEqual(result, correctMD5);
                    objectGet(accessKey, metastore, testGetRequest,
                        (err, result, responseMetaHeaders) => {
                            assert.strictEqual(responseMetaHeaders
                                [userMetadataKey], userMetadataValue);
                            assert.strictEqual(responseMetaHeaders.Etag,
                                correctMD5);
                            done();
                        });
                });
        });
    });

    it.skip('should get the object data', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest, (err, res) => {
            assert.strictEqual(res, 'Bucket created');
            objectPut(accessKey, metastore,
                testPutObjectRequest, (err, result) => {
                    assert.strictEqual(result, correctMD5);
                    objectGet(accessKey, metastore,
                        testGetRequest, (err, readable) => {
                            const chunks = [];
                            readable.on('data', function chunkRcvd(chunk) {
                                chunks.push(chunk);
                            });
                            readable.on('end', function combineChunks() {
                                const final = [ chunks ];
                                assert.strictEqual(final,
                                    postBody);
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
            lowerCaseHeaders: {
                'x-amz-meta-test': 'some metadata'
            },
            url: `/${bucketName}/${objectName}`,
            namespace: namespace,
            post: [ testBigData ],
            calculatedMD5: correctBigMD5
        };

        const testGetRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore, testPutBigObjectRequest,
                    (err, result) => {
                        assert.strictEqual(result, correctBigMD5);
                        objectGet(accessKey,
                            metastore, testGetRequest, (err, readable) => {
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
