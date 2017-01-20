import { errors } from 'arsenal';
import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import bucketPutACL from '../../../lib/api/bucketPutACL';
import { cleanup, DummyRequestLogger, makeAuthInfo } from '../helpers';
import { ds } from '../../../lib/data/in_memory/backend';
import metadata from '../metadataswitch';
import objectPut from '../../../lib/api/objectPut';
import DummyRequest from '../DummyRequest';
import config from '../../../lib/Config';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const testPutBucketRequest = new DummyRequest({
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
});
const locationConstraint = config.locationConstraints ? 'aws-us-east-1' :
'us-east-1';

const objectName = 'objectName';

let testPutObjectRequest;

function testAuth(bucketOwner, authUser, bucketPutReq, log, cb) {
    bucketPut(bucketOwner, bucketPutReq, locationConstraint, log, () => {
        bucketPutACL(bucketOwner, testPutBucketRequest, log, err => {
            assert.strictEqual(err, undefined);
            objectPut(authUser, testPutObjectRequest, undefined,
                log, (err, res) => {
                    assert.strictEqual(err, null);
                    assert.strictEqual(res, correctMD5);
                    cb();
                });
        });
    });
}
describe('objectPut API', () => {
    beforeEach(() => {
        cleanup();
        testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            url: '/',
        }, postBody);
    });


    it('should return an error if the bucket does not exist', done => {
        objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    it('should return an error if user is not authorized', done => {
        const putAuthInfo = makeAuthInfo('accessKey2');
        bucketPut(putAuthInfo, testPutBucketRequest, locationConstraint,
            log, () => {
                objectPut(authInfo, testPutObjectRequest,
                    undefined, log, err => {
                        assert.deepStrictEqual(err, errors.AccessDenied);
                        done();
                    });
            });
    });

    it('should put object if user has FULL_CONTROL grant on bucket', done => {
        const bucketOwner = makeAuthInfo('accessKey2');
        const authUser = makeAuthInfo('accessKey3');
        testPutBucketRequest.headers['x-amz-grant-full-control'] =
            `id=${authUser.getCanonicalID()}`;
        testAuth(bucketOwner, authUser, testPutBucketRequest, log, done);
    });

    it('should put object if user has WRITE grant on bucket', done => {
        const bucketOwner = makeAuthInfo('accessKey2');
        const authUser = makeAuthInfo('accessKey3');
        testPutBucketRequest.headers['x-amz-grant-write'] =
            `id=${authUser.getCanonicalID()}`;

        testAuth(bucketOwner, authUser, testPutBucketRequest, log, done);
    });

    it('should put object in bucket with public-read-write acl', done => {
        const bucketOwner = makeAuthInfo('accessKey2');
        const authUser = makeAuthInfo('accessKey3');
        testPutBucketRequest.headers['x-amz-acl'] = 'public-read-write';

        testAuth(bucketOwner, authUser, testPutBucketRequest, log, done);
    });

    it('should successfully put an object', done => {
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
            calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
        }, postBody);

        bucketPut(authInfo, testPutBucketRequest, locationConstraint,
            log, () => {
                objectPut(authInfo, testPutObjectRequest, undefined, log,
                    (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        metadata.getObjectMD(bucketName, objectName,
                            log, (err, md) => {
                                assert(md);
                                assert
                                .strictEqual(md['content-md5'], correctMD5);
                                done();
                            });
                    });
            });
    });

    it('should successfully put an object with user metadata', done => {
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
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
            calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
        }, postBody);

        bucketPut(authInfo, testPutBucketRequest, locationConstraint,
            log, () => {
                objectPut(authInfo, testPutObjectRequest, undefined, log,
                    (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        metadata.getObjectMD(bucketName, objectName, log,
                            (err, md) => {
                                assert(md);
                                assert.strictEqual(md['x-amz-meta-test'],
                                'some metadata');
                                assert.strictEqual(md['x-amz-meta-test2'],
                                           'some more metadata');
                                assert.strictEqual(md['x-amz-meta-test3'],
                                           'even more metadata');
                                done();
                            });
                    });
            });
    });

    it('should put an object with user metadata but no data', done => {
        const postBody = '';
        const correctMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'content-length': '0',
                'x-amz-meta-test': 'some metadata',
                'x-amz-meta-test2': 'some more metadata',
                'x-amz-meta-test3': 'even more metadata',
            },
            parsedContentLength: 0,
            url: `/${bucketName}/${objectName}`,
            calculatedHash: 'd41d8cd98f00b204e9800998ecf8427e',
        }, postBody);

        bucketPut(authInfo, testPutBucketRequest, locationConstraint,
            log, () => {
                objectPut(authInfo, testPutObjectRequest, undefined, log,
                    (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        assert.deepStrictEqual(ds, []);
                        metadata.getObjectMD(bucketName, objectName, log,
                            (err, md) => {
                                assert(md);
                                assert.strictEqual(md.location, null);
                                assert.strictEqual(md['x-amz-meta-test'],
                                'some metadata');
                                assert.strictEqual(md['x-amz-meta-test2'],
                                           'some more metadata');
                                assert.strictEqual(md['x-amz-meta-test3'],
                                           'even more metadata');
                                done();
                            });
                    });
            });
    });

    it('should not leave orphans in data when overwriting an object', done => {
        const testPutObjectRequest2 = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        }, Buffer.from('I am another body', 'utf8'));

        bucketPut(authInfo, testPutBucketRequest, locationConstraint,
            log, () => {
                objectPut(authInfo, testPutObjectRequest,
                    undefined, log, () => {
                        objectPut(authInfo, testPutObjectRequest2, undefined,
                            log,
                        () => {
                            // orphan objects don't get deleted
                            // until the next tick
                            // in memory
                            process.nextTick(() => {
                                // Data store starts at index 1
                                assert.strictEqual(ds[0], undefined);
                                assert.strictEqual(ds[1], undefined);
                                assert.deepStrictEqual(ds[2].value,
                                    Buffer.from('I am another body', 'utf8'));
                                done();
                            });
                        });
                    });
            });
    });
});
