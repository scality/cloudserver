import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import metadata from '../metadataswitch';
import objectPut from '../../../lib/api/objectPut';
import DummyRequestLogger from '../helpers';

const log = new DummyRequestLogger();
const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = [ new Buffer('I am a body'), ];
const testPutBucketRequest = {
    bucketName,
    namespace,
    lowerCaseHeaders: {},
    headers: {host: `${bucketName}.s3.amazonaws.com`},
    url: '/',
};
const objectName = 'objectName';
const testPutObjectRequest = {
    bucketName,
    namespace,
    objectKey: objectName,
    lowerCaseHeaders: {},
    headers: {host: `${bucketName}.s3.amazonaws.com`},
    url: '/',
    post: postBody
};

describe('objectPut API', () => {
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
        metadata.deleteBucket(bucketName, log, () => done());
    });

    after((done) => {
        metadata.deleteBucket(bucketName, log, () => done());
    });


    it('should return an error if the bucket does not exist', (done) => {
        objectPut(accessKey, metastore, testPutObjectRequest, log, (err) => {
            assert.strictEqual(err, 'NoSuchBucket');
            done();
        });
    });

    it('should return an error if user is not authorized', (done) => {
        const putAccessKey = 'accessKey2';
        bucketPut(putAccessKey, metastore, testPutBucketRequest, log,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore, testPutObjectRequest, log,
                    (err) => {
                        assert.strictEqual(err, 'AccessDenied');
                        done();
                    });
            });
    });

    it('should successfully put an object', (done) => {
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const testPutObjectRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        bucketPut(accessKey, metastore, testPutBucketRequest, log,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, log, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        metadata.getBucket(bucketName, log, (err, md) => {
                            const MD = md.keyMap[objectName];
                            assert(MD);
                            assert.strictEqual(MD['content-md5'], correctMD5);
                            done();
                        });
                    });
            });
    });

    it('should successfully put an object with user metadata', (done) => {
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const testPutObjectRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            lowerCaseHeaders: {
                // Note that Node will collapse common headers into one
                // (e.g. "x-amz-meta-test: hi" and "x-amz-meta-test:
                // there" becomes "x-amz-meta-test: hi, there")
                // Here we are not going through an actual http
                // request so will not collapse properly.
                'x-amz-meta-test': 'some metadata',
                'x-amz-meta-test2': 'some more metadata',
                'x-amz-meta-test3': 'even more metadata',
            },
            url: `/${bucketName}/${objectName}`,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        bucketPut(accessKey, metastore, testPutBucketRequest, log,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, log, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        metadata.getBucket(bucketName, log, (err, md) => {
                            const MD = md.keyMap[objectName];
                            assert(MD);
                            assert.strictEqual(MD['x-amz-meta-test'],
                                               'some metadata');
                            assert.strictEqual(MD['x-amz-meta-test2'],
                                               'some more metadata');
                            assert.strictEqual(MD['x-amz-meta-test3'],
                                               'even more metadata');
                            done();
                        });
                    });
            });
    });
});
