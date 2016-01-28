import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import { DummyRequestLogger, makeAuthInfo } from '../helpers';
import metadata from '../metadataswitch';
import objectPut from '../../../lib/api/objectPut';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
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
    beforeEach((done) => {
        metadata.deleteBucket(bucketName, log, () => done());
    });

    after((done) => {
        metadata.deleteBucket(bucketName, log, () => done());
    });


    it('should return an error if the bucket does not exist', (done) => {
        objectPut(authInfo,  testPutObjectRequest, log, (err) => {
            assert.strictEqual(err, 'NoSuchBucket');
            done();
        });
    });

    it('should return an error if user is not authorized', (done) => {
        const putAuthInfo = makeAuthInfo('accessKey2');
        bucketPut(putAuthInfo,  testPutBucketRequest, log,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(authInfo,  testPutObjectRequest, log,
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

        bucketPut(authInfo,  testPutBucketRequest, log,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(authInfo,
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

        bucketPut(authInfo,  testPutBucketRequest, log,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(authInfo,
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
