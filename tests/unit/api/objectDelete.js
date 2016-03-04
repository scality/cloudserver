import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import bucketPutACL from '../../../lib/api/bucketPutACL';
import constants from '../../../constants';
import { DummyRequestLogger, makeAuthInfo } from '../helpers';
import metadata from '../metadataswitch';
import objectPut from '../../../lib/api/objectPut';
import objectDelete from '../../../lib/api/objectDelete';
import objectGet from '../../../lib/api/objectGet';
import DummyRequest from '../DummyRequest';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = new Buffer('I am a body');
const objectKey = 'objectName';

function testAuth(bucketOwner, authUser, bucketPutReq, objPutReq, objDelReq,
    log, cb) {
    bucketPut(bucketOwner, bucketPutReq, log, (err, success) => {
        assert.strictEqual(success, 'Bucket created');
        bucketPutACL(bucketOwner, bucketPutReq, log, err => {
            assert.strictEqual(err, undefined);
            objectPut(bucketOwner, objPutReq, log, err => {
                assert.strictEqual(err, undefined);
                objectDelete(authUser, objDelReq, log, err => {
                    assert.strictEqual(err, undefined);
                    cb();
                });
            });
        });
    });
}

describe('objectDelete API', () => {
    let testPutObjectRequest;

    beforeEach(done => {
        testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey,
            headers: {},
            url: `/${bucketName}/${objectKey}`,
        }, postBody);
        metadata.deleteBucket(bucketName, log, () => done());
    });

    after(done => {
        metadata.deleteBucket(bucketName, log, () => done());
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

    it.skip('should set delete markers when versioning enabled', () => {
        // TODO
    });

    it('should delete an object', done => {
        bucketPut(authInfo, testBucketPutRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, log, () => {
                objectDelete(authInfo, testDeleteRequest, log, err => {
                    assert.strictEqual(err, undefined);
                    objectGet(authInfo, testGetObjectRequest, log, err => {
                        assert.strictEqual(err, 'NoSuchKey');
                        done();
                    });
                });
            });
        });
    });

    it('should prevent anonymous user deleteObject API access', done => {
        const publicAuthInfo = makeAuthInfo(constants.publicId);
        objectDelete(publicAuthInfo, testDeleteRequest, log, err => {
            assert.strictEqual(err, 'AccessDenied');
            done();
        });
    });

    it('should del object if user has FULL_CONTROL grant on bucket', done => {
        const bucketOwner = makeAuthInfo('accessKey2');
        const authUser = makeAuthInfo('accessKey3');
        testBucketPutRequest.headers['x-amz-grant-full-control'] =
            `id="${authUser.getCanonicalID()}"`;
        testAuth(bucketOwner, authUser, testBucketPutRequest,
            testPutObjectRequest, testDeleteRequest, log, done);
    });

    it('should del object if user has WRITE grant on bucket', done => {
        const bucketOwner = makeAuthInfo('accessKey2');
        const authUser = makeAuthInfo('accessKey3');
        testBucketPutRequest.headers['x-amz-grant-write'] =
            `id="${authUser.getCanonicalID()}"`;
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
