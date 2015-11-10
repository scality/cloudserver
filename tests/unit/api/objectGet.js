import { expect } from 'chai';
import crypto from 'crypto';
import bucketPut from '../../../lib/api/bucketPut';
import objectPut from '../../../lib/api/objectPut';
import objectGet from '../../../lib/api/objectGet';

const accessKey = 'accessKey1';
const namespace = 'default';

describe('objectGet API', () => {
    let metastore;
    let datastore;

    beforeEach(() => {
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
        datastore = {};
    });

    const bucketName = 'bucketname';
    const postBody = 'I am a body';
    const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
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
        calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
    };

    it("should get the object metadata", (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectGet(accessKey, datastore,
                            metastore, testGetRequest,
                            (err, result, responseMetaHeaders) => {
                                expect(responseMetaHeaders[userMetadataKey])
                                    .to.equal(userMetadataValue);
                                expect(responseMetaHeaders.Etag)
                                    .to.equal(correctMD5);
                                done();
                            });
                    });
            });
    });

    it('should get the object data', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectGet(accessKey, datastore, metastore,
                            testGetRequest, (err, result) => {
                                expect(result).to.equal(postBody);
                                done();
                            });
                    });
            });
    });

    it('should get the object data for large objects', (done) => {
        const testBigData = crypto.randomBytes(1000000);
        const correctBigMD5 =
            crypto.createHash('md5').update(testBigData).digest('hex');

        const testPutBigObjectRequest = {
            lowerCaseHeaders: {
                'x-amz-meta-test': 'some metadata'
            },
            url: `/${bucketName}/${objectName}`,
            namespace: namespace,
            post: testBigData,
            calculatedMD5: correctBigMD5
        };

        const testGetRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutBigObjectRequest, (err, result) => {
                        expect(result).to.equal(correctBigMD5);
                        objectGet(accessKey, datastore,
                            metastore, testGetRequest, (err, result) => {
                                const resultmd5Hash =
                                    crypto.createHash('md5')
                                        .update(result).digest('hex');
                                expect(resultmd5Hash).to.equal(correctBigMD5);
                                done();
                            });
                    });
            });
    });
});
