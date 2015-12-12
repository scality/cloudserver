import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import metadata from '../../../lib/metadata/wrapper';
import objectPut from '../../../lib/api/objectPut';
import utils from '../../../lib/utils';

const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
const testBucketUID = utils.getResourceUID(namespace, bucketName);

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
        metadata.deleteBucket(testBucketUID, ()=> {
            done();
        });
    });

    after((done) => {
        metadata.deleteBucket(testBucketUID, ()=> {
            done();
        });
    });


    it('should return an error if the bucket does not exist', (done) => {
        const postBody = 'I am a body';
        const testRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace,
            post: postBody
        };

        objectPut(accessKey, metastore, testRequest, (err) => {
            assert.strictEqual(err, 'NoSuchBucket');
            done();
        });
    });

    it('should return an error if user is not authorized', (done) => {
        const postBody = 'I am a body';
        const putAccessKey = 'accessKey2';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace,
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace,
            post: postBody
        };

        bucketPut(putAccessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore, testPutObjectRequest,
                    (err) => {
                        assert.strictEqual(err, 'AccessDenied');
                        done();
                    });
            });
    });

    it.skip('should return an error if datastore ' +
            'reports an error back', () => {
        // TODO: Test to be written once services.putDataStore
        // includes an actual call to
        // datastore rather than just the in
        // memory adding of a key/value pair to the datastore
        // object
    });

    it.skip('should return an error if metastore ' +
            'reports an error back', () => {
        // TODO: Test to be written once
        // services.metadataStoreObject includes an actual call to
        // datastore rather than just the in
        // memory adding of a key/value pair to the datastore
        // object
    });

    it('should successfully put an object with bucket' +
    ' and object in pathname', (done) => {
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        metadata.getBucket(testBucketUID, (err, md) => {
                            assert(md.keyMap[objectName]);
                            assert.strictEqual(md.keyMap[objectName]
                                ['content-md5'], correctMD5);
                            done();
                        });
                    });
            });
    });

    it('should successfully put an object with object ' +
    'in pathname and bucket in hostname', (done) => {
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: `/${objectName}`,
            namespace: namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        metadata.getBucket(testBucketUID, (err, md) => {
                            assert(md.keyMap[objectName]);
                            assert.strictEqual(md.keyMap[objectName]
                                ['content-md5'], correctMD5);
                            done();
                        });
                    });
            });
    });

    it('should successfully put an object with user metadata', (done) => {
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace,
        };
        const testPutObjectRequest = {
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
            namespace: namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        metadata.getBucket(testBucketUID, (err, md) => {
                            assert(md.keyMap[objectName]);
                            assert.strictEqual(md.keyMap[objectName]
                                ['x-amz-meta-test'], 'some metadata');
                            assert.strictEqual(md.keyMap[objectName]
                                ['x-amz-meta-test2'], 'some more metadata');
                            assert.strictEqual(md.keyMap[objectName]
                                ['x-amz-meta-test3'], 'even more metadata');
                            done();
                        });
                    });
            });
    });
});
