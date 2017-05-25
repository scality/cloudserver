const assert = require('assert');
const { errors } = require('arsenal');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutACL = require('../../../lib/api/bucketPutACL');
const constants = require('../../../constants');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const objectPut = require('../../../lib/api/objectPut');
const objectDelete = require('../../../lib/api/objectDelete');
const objectGet = require('../../../lib/api/objectGet');
const DummyRequest = require('../DummyRequest');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = Buffer.from('I am a body', 'utf8');
const objectKey = 'objectName';

function testAuth(bucketOwner, authUser, bucketPutReq, objPutReq, objDelReq,
    log, cb) {
    bucketPut(bucketOwner, bucketPutReq, log, () => {
        bucketPutACL(bucketOwner, bucketPutReq, log, err => {
            assert.strictEqual(err, undefined);
            objectPut(bucketOwner, objPutReq, undefined, log, err => {
                assert.strictEqual(err, null);
                objectDelete(authUser, objDelReq, log, err => {
                    assert.strictEqual(err, null);
                    cb();
                });
            });
        });
    });
}

describe('objectDelete API', () => {
    let testPutObjectRequest;

    beforeEach(() => {
        cleanup();
        testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey,
            headers: {},
            url: `/${bucketName}/${objectKey}`,
        }, postBody);
    });

    const testBucketPutRequest = new DummyRequest({
        bucketName,
        namespace,
        headers: {},
        url: `/${bucketName}`,
    });
    const testGetObjectRequest = new DummyRequest({
        bucketName,
        namespace,
        objectKey,
        headers: {},
        url: `/${bucketName}/${objectKey}`,
    });
    const testDeleteRequest = new DummyRequest({
        bucketName,
        namespace,
        objectKey,
        headers: {},
        url: `/${bucketName}/${objectKey}`,
    });

    it('should delete an object', done => {
        bucketPut(authInfo, testBucketPutRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest,
                undefined, log, () => {
                    objectDelete(authInfo, testDeleteRequest, log, err => {
                        assert.strictEqual(err, null);
                        objectGet(authInfo, testGetObjectRequest, false,
                            log, err => {
                                assert.deepStrictEqual(err,
                                    errors.NoSuchKey);
                                done();
                            });
                    });
                });
        });
    });

    it('should delete a 0 bytes object', done => {
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey,
            headers: {},
            url: `/${bucketName}/${objectKey}`,
        }, '');
        bucketPut(authInfo, testBucketPutRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest,
                undefined, log, () => {
                    objectDelete(authInfo, testDeleteRequest, log, err => {
                        assert.strictEqual(err, null);
                        objectGet(authInfo, testGetObjectRequest, false,
                            log, err => {
                                const expected =
                                Object.assign({}, errors.NoSuchKey);
                                const received = Object.assign({}, err);
                                assert.deepStrictEqual(received, expected);
                                done();
                            });
                    });
                });
        });
    });

    it('should prevent anonymous user deleteObject API access', done => {
        const publicAuthInfo = makeAuthInfo(constants.publicId);
        objectDelete(publicAuthInfo, testDeleteRequest, log, err => {
            assert.deepStrictEqual(err, errors.AccessDenied);
            done();
        });
    });

    it('should del object if user has FULL_CONTROL grant on bucket', done => {
        const bucketOwner = makeAuthInfo('accessKey2');
        const authUser = makeAuthInfo('accessKey3');
        testBucketPutRequest.headers['x-amz-grant-full-control'] =
            `id=${authUser.getCanonicalID()}`;
        testAuth(bucketOwner, authUser, testBucketPutRequest,
            testPutObjectRequest, testDeleteRequest, log, done);
    });

    it('should del object if user has WRITE grant on bucket', done => {
        const bucketOwner = makeAuthInfo('accessKey2');
        const authUser = makeAuthInfo('accessKey3');
        testBucketPutRequest.headers['x-amz-grant-write'] =
            `id=${authUser.getCanonicalID()}`;
        testAuth(bucketOwner, authUser, testBucketPutRequest,
            testPutObjectRequest, testDeleteRequest, log, done);
    });

    it('should del object in bucket with public-read-write acl', done => {
        const bucketOwner = makeAuthInfo('accessKey2');
        const authUser = makeAuthInfo('accessKey3');
        testBucketPutRequest.headers['x-amz-acl'] = 'public-read-write';
        testAuth(bucketOwner, authUser, testBucketPutRequest,
            testPutObjectRequest, testDeleteRequest, log, done);
    });
});
