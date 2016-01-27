import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import metadata from '../metadataswitch';
import objectPut from '../../../lib/api/objectPut';
import objectHead from '../../../lib/api/objectHead';
import DummyRequestLogger from '../helpers';

const log = new DummyRequestLogger();
const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = [ new Buffer('I am a body'), ];

describe('objectHead API', () => {
    beforeEach((done) => {
        metadata.deleteBucket(bucketName, log, () => done());
    });

    after((done) => {
        metadata.deleteBucket(bucketName, log, () => done());
    });

    const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
    const incorrectMD5 = 'fkjwelfjlslfksdfsdfsdfsdfsdfsdj';
    const objectName = 'objectName';
    const date = new Date();
    const laterDate = date.setMinutes(date.getMinutes() + 30);
    const earlierDate = date.setMinutes(date.getMinutes() - 30);
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
            'x-amz-meta-test': userMetadataValue
        },
        url: `/${bucketName}/${objectName}`,
        post: postBody,
        calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
    };


    it('should return NotModified if request header ' +
       'includes "if-modified-since" and object ' +
       'not modified since specified time', (done) => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            lowerCaseHeaders: {
                'if-modified-since': laterDate
            },
            url: `/${bucketName}/${objectName}`,
        };

        bucketPut(accessKey,  testPutBucketRequest, log,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey,
                    testPutObjectRequest, log, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectHead(accessKey,  testGetRequest, log,
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
            bucketName,
            namespace,
            objectKey: objectName,
            lowerCaseHeaders: {
                'if-unmodified-since': earlierDate
            },
            url: `/${bucketName}/${objectName}`,
        };
        bucketPut(accessKey, testPutBucketRequest, log,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey,
                    testPutObjectRequest, log, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectHead(accessKey,
                            testGetRequest, log, (err) => {
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
            bucketName,
            namespace,
            objectKey: objectName,
            lowerCaseHeaders: {
                'if-match': incorrectMD5
            },
            url: `/${bucketName}/${objectName}`,
        };

        bucketPut(accessKey,  testPutBucketRequest, log,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey,
                    testPutObjectRequest, log, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectHead(accessKey,
                            testGetRequest, log, (err) => {
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
            bucketName,
            namespace,
            objectKey: objectName,
            lowerCaseHeaders: {
                'if-none-match': correctMD5
            },
            url: `/${bucketName}/${objectName}`,
        };

        bucketPut(accessKey,  testPutBucketRequest, log,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey,
                    testPutObjectRequest, log, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectHead(accessKey,
                            testGetRequest, log, (err) => {
                                assert.strictEqual(err, 'NotModified');
                                done();
                            });
                    });
            });
    });

    it('should get the object metadata', (done) => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
        };

        bucketPut(accessKey,  testPutBucketRequest, log,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey,
                    testPutObjectRequest, log, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectHead(accessKey,
                            testGetRequest, log, (err, success) => {
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
