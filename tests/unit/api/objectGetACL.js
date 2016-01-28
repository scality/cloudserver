import assert from 'assert';
import async from 'async';
import { parseString } from 'xml2js';

import bucketPut from '../../../lib/api/bucketPut';
import constants from '../../../constants';
import { DummyRequestLogger, makeAuthInfo } from '../helpers';
import metadata from '../metadataswitch';
import objectPut from '../../../lib/api/objectPut';
import objectGetACL from '../../../lib/api/objectGetACL';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = [ new Buffer('I am a body'), ];

describe('objectGetACL API', () => {
    beforeEach((done) => {
        metadata.deleteBucket(bucketName, log, () => done());
    });

    after((done) => {
        metadata.deleteBucket(bucketName, log, () => done());
    });

    const objectName = 'objectName';
    const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
    const testBucketPutRequest = {
        bucketName,
        namespace,
        lowerCaseHeaders: {},
        headers: {host: `${bucketName}.s3.amazonaws.com`},
        url: '/',
    };
    const testGetACLRequest = {
        bucketName,
        namespace,
        objectKey: objectName,
        url: `/${bucketName}/${objectName}?acl`,
        query: {
            acl: ''
        }
    };

    it('should get a canned private ACL', (done) => {
        const testPutObjectRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            lowerCaseHeaders: {
                'x-amz-acl': 'private'
            },
            headers: {
                'x-amz-acl': 'private'
            },
            url: `/${bucketName}/${objectName}`,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(authInfo,  testBucketPutRequest, log,
                    next);
            },
            function waterfall2(result, next) {
                assert.strictEqual(result, 'Bucket created');
                objectPut(authInfo,  testPutObjectRequest, log,
                    next);
            },
            function waterfall3(result, next) {
                assert.strictEqual(result, correctMD5);
                objectGetACL(authInfo,  testGetACLRequest, log,
                    next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], canonicalID);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            done();
        });
    });

    it('should return an error if try to get an ACL ' +
        'for a nonexistent object', (done) => {
        bucketPut(authInfo,  testBucketPutRequest, log,
            (err, result) => {
                assert.strictEqual(result, 'Bucket created');
                objectGetACL(authInfo,  testGetACLRequest, log,
                    (err) => {
                        assert.strictEqual(err, 'NoSuchKey');
                        done();
                    });
            });
    });

    it('should get a canned public-read ACL', (done) => {
        const testPutObjectRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            lowerCaseHeaders: {
                'x-amz-acl': 'public-read'
            },
            headers: {
                'x-amz-acl': 'public-read'
            },
            url: `/${bucketName}/${objectName}`,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(authInfo,  testBucketPutRequest, log,
                    next);
            },
            function waterfall2(result, next) {
                assert.strictEqual(result, 'Bucket created');
                objectPut(authInfo,  testPutObjectRequest, log,
                    next);
            },
            function waterfall3(result, next) {
                assert.strictEqual(result, correctMD5);
                objectGetACL(authInfo,  testGetACLRequest, log,
                    next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], canonicalID);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .URI[0], constants.publicId);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0], 'READ');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2], undefined);
            done();
        });
    });

    it('should get a canned public-read-write ACL', (done) => {
        const testPutObjectRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            lowerCaseHeaders: {
                'x-amz-acl': 'public-read-write'
            },
            headers: {
                'x-amz-acl': 'public-read-write'
            },
            url: `/${bucketName}/${objectName}`,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(authInfo,  testBucketPutRequest, log,
                    next);
            },
            function waterfall2(result, next) {
                assert.strictEqual(result, 'Bucket created');
                objectPut(authInfo,  testPutObjectRequest, log,
                    next);
            },
            function waterfall3(result, next) {
                assert.strictEqual(result, correctMD5);
                objectGetACL(authInfo,  testGetACLRequest, log,
                    next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], canonicalID);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .URI[0], constants.publicId);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0],
                'READ');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .URI[0], constants.publicId);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Permission[0],
                'WRITE');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[3], undefined);
            done();
        });
    });

    it('should get a canned authenticated-read ACL', (done) => {
        const testPutObjectRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            lowerCaseHeaders: {
                'x-amz-acl': 'authenticated-read'
            },
            headers: {
                'x-amz-acl': 'authenticated-read'
            },
            url: `/${bucketName}/${objectName}`,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(authInfo,  testBucketPutRequest, log,
                    next);
            },
            function waterfall2(result, next) {
                assert.strictEqual(result, 'Bucket created');
                objectPut(authInfo,  testPutObjectRequest, log,
                    next);
            },
            function waterfall3(result, next) {
                assert.strictEqual(result, correctMD5);
                objectGetACL(authInfo,  testGetACLRequest, log,
                    next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], canonicalID);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .URI[0], constants.allAuthedUsersId);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0],
                'READ');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2],
                undefined);
            done();
        });
    });

    it('should get a canned bucket-owner-read ACL', (done) => {
        const testPutObjectRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            lowerCaseHeaders: {
                'x-amz-acl': 'bucket-owner-read'
            },
            headers: {
                'x-amz-acl': 'bucket-owner-read'
            },
            url: `/${bucketName}/${objectName}`,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(authInfo,  testBucketPutRequest, log,
                    next);
            },
            function waterfall2(result, next) {
                assert.strictEqual(result, 'Bucket created');
                objectPut(authInfo,  testPutObjectRequest, log,
                    next);
            },
            function waterfall3(result, next) {
                assert.strictEqual(result, correctMD5);
                objectGetACL(authInfo,  testGetACLRequest, log,
                    next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], canonicalID);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .ID[0], canonicalID);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0],
                'READ');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2], undefined);
            done();
        });
    });

    it('should get a canned bucket-owner-full-control ACL', (done) => {
        const testPutObjectRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            lowerCaseHeaders: {
                'x-amz-acl': 'bucket-owner-full-control'
            },
            headers: {
                'x-amz-acl': 'bucket-owner-full-control'
            },
            url: `/${bucketName}/${objectName}`,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(authInfo,  testBucketPutRequest, log,
                    next);
            },
            function waterfall2(result, next) {
                assert.strictEqual(result, 'Bucket created');
                objectPut(authInfo,  testPutObjectRequest, log,
                    next);
            },
            function waterfall3(result, next) {
                assert.strictEqual(result, correctMD5);
                objectGetACL(authInfo,  testGetACLRequest, log,
                    next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], canonicalID);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .ID[0], canonicalID);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2], undefined);
            done();
        });
    });

    it('should get specifically set ACLs', (done) => {
        const testPutObjectRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            lowerCaseHeaders: {
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read': `uri=${constants.allAuthedUsersId}`,
                'x-amz-grant-write': `uri=${constants.publicId}`,
                'x-amz-grant-read-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be"',
                'x-amz-grant-write-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf"',            },
            headers: {
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read': `uri=${constants.allAuthedUsersId}`,
                'x-amz-grant-write': `uri=${constants.publicId}`,
                'x-amz-grant-read-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be"',
                'x-amz-grant-write-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf"',            },
            url: `/${bucketName}/${objectName}`,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(authInfo,  testBucketPutRequest, log,
                    next);
            },
            function waterfall2(result, next) {
                assert.strictEqual(result, 'Bucket created');
                objectPut(authInfo,  testPutObjectRequest, log,
                    next);
            },
            function waterfall3(result, next) {
                assert.strictEqual(result, correctMD5);
                objectGetACL(authInfo,  testGetACLRequest, log,
                    next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], '79a59df900b949e55d96a1e698fbacedfd6e09d98' +
                'eacf8f8d5218e7cd47ef2be');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .DisplayName[0], 'sampleAccount1@sampling.com');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .ID[0], '79a59df900b949e55d96a1e698fbacedfd6e09d98' +
                'eacf8f8d5218e7cd47ef2bf');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .DisplayName[0], 'sampleAccount2@sampling.com');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .ID[0], '79a59df900b949e55d96a1e698fbacedfd6e09d98' +
                'eacf8f8d5218e7cd47ef2bf');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .DisplayName[0], 'sampleAccount2@sampling.com');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Permission[0],
                'WRITE_ACP');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Grantee[0]
                .ID[0], '79a59df900b949e55d96a1e698fbacedfd6e09d98' +
                'eacf8f8d5218e7cd47ef2be');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Grantee[0]
                .DisplayName[0], 'sampleAccount1@sampling.com');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Permission[0],
                'READ_ACP');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[4].Grantee[0]
                .URI[0], constants.allAuthedUsersId);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[4].Permission[0],
                'READ');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[5],
                undefined);
            done();
        });
    });
});
