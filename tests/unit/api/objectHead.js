import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import metadata from '../metadataswitch';
import objectPut from '../../../lib/api/objectPut';
import objectHead from '../../../lib/api/objectHead';

const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = [ new Buffer('I am a body'), ];

describe('objectHead API', () => {
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
        metadata.deleteBucket(bucketName, ()=> {
            done();
        });
    });

    after((done) => {
        metadata.deleteBucket(bucketName, ()=> {
            done();
        });
    });

    const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
    const incorrectMD5 = 'fkjwelfjlslfksdfsdfsdfsdfsdfsdj';
    const objectName = 'objectName';
    const date = new Date();
    const laterDate = date.setMinutes(date.getMinutes() + 30);
    const earlierDate = date.setMinutes(date.getMinutes() - 30);
    const testPutBucketRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace: namespace,
    };
    const userMetadataKey = 'x-amz-meta-test';
    const userMetadataValue = 'some metadata';
    const testPutObjectRequest = {
        lowerCaseHeaders: {
            'x-amz-meta-test': userMetadataValue
        },
        url: `/${bucketName}/${objectName}`,
        namespace: namespace,
        post: postBody,
        calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
    };


    it('should return NotModified if request header ' +
       'includes "if-modified-since" and object ' +
       'not modified since specified time', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {
                'if-modified-since': laterDate
            },
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectHead(accessKey, metastore, testGetRequest,
                            (err) => {
                                assert.strictEqual(err, 'NotModified');
                                done();
                            });
                    });
            });
    });

    it('should return PreconditionFailed if request header ' +
       'includes "if-unmodified-since" and object has ' +
       'been modified since specified time', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {
                'if-unmodified-since': earlierDate
            },
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };
        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectHead(accessKey, metastore,
                            testGetRequest, (err) => {
                                assert.strictEqual(err, 'PreconditionFailed');
                                done();
                            });
                    });
            });
    });

    it('should return PreconditionFailed if request header ' +
       'includes "if-match" and ETag of object ' +
       'does not match specified ETag', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {
                'if-match': incorrectMD5
            },
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectHead(accessKey, metastore,
                            testGetRequest, (err) => {
                                assert.strictEqual(err, 'PreconditionFailed');
                                done();
                            });
                    });
            });
    });

    it('should return NotModified if request header ' +
       'includes "if-none-match" and ETag of object does ' +
       'match specified ETag', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {
                'if-none-match': correctMD5
            },
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectHead(accessKey, metastore,
                            testGetRequest, (err) => {
                                assert.strictEqual(err, 'NotModified');
                                done();
                            });
                    });
            });
    });

    it('should get the object metadata', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectHead(accessKey, metastore,
                            testGetRequest, (err, success) => {
                                assert.strictEqual(success[userMetadataKey],
                                    userMetadataValue);
                                assert.strictEqual(success.ETag,
                                    `"${correctMD5}"`);
                                done();
                            });
                    });
            });
    });
});
