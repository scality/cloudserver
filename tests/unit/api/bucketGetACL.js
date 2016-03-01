import assert from 'assert';
import async from 'async';
import { parseString } from 'xml2js';

import bucketPut from '../../../lib/api/bucketPut';
import bucketGetACL from '../../../lib/api/bucketGetACL';
import bucketPutACL from '../../../lib/api/bucketPutACL';
import constants from '../../../constants';
import metadata from '../metadataswitch';
import { DummyRequestLogger, makeAuthInfo } from '../helpers';

const log = new DummyRequestLogger();
const accessKey = 'accessKey1';
const authInfo = makeAuthInfo(accessKey);
const canonicalID = authInfo.getCanonicalID();
const namespace = 'default';
const bucketName = 'bucketname';

describe('bucketGetACL API', () => {
    beforeEach(done => {
        metadata.deleteBucket(bucketName, log, () => done());
    });

    after(done => {
        metadata.deleteBucket(bucketName, log, () => done());
    });

    const testBucketPutRequest = {
        bucketName,
        namespace,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
    };
    const testGetACLRequest = {
        bucketName,
        namespace,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`
        },
        url: '/?acl',
        query: {
            acl: ''
        }
    };

    it('should get a canned private ACL', done => {
        const testPutACLRequest = {
            bucketName,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'private'
            },
            url: '/?acl',
            query: {
                acl: ''
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(authInfo, testBucketPutRequest, log, next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                bucketPutACL(authInfo, testPutACLRequest, log, next);
            },
            function waterfall3(result, next) {
                bucketGetACL(authInfo, testGetACLRequest, log, next);
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
                AccessControlList[0].Grant[1], undefined);
            done();
        });
    });

    it('should get a canned public-read-write ACL', done => {
        const testPutACLRequest = {
            bucketName,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'public-read-write',
            },
            url: '/?acl',
            query: {
                acl: ''
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(authInfo, testBucketPutRequest, log, next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                bucketPutACL(authInfo, testPutACLRequest, log, next);
            },
            function waterfall3(result, next) {
                bucketGetACL(authInfo, testGetACLRequest, log, next);
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
                AccessControlList[0].Grant[0].Permission[0], 'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0].URI[0],
                constants.publicId);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1]
                .Permission[0], 'READ');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0].URI[0],
                constants.publicId);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2]
                .Permission[0], 'WRITE');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[3], undefined);
            done();
        });
    });

    it('should get a canned public-read ACL', done => {
        const testPutACLRequest = {
            bucketName,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'public-read'
            },
            url: '/?acl',
            query: {
                acl: ''
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(authInfo, testBucketPutRequest, log,
                    next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                bucketPutACL(authInfo, testPutACLRequest, log,
                    next);
            },
            function waterfall3(result, next) {
                bucketGetACL(authInfo, testGetACLRequest, log,
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
                AccessControlList[0].Grant[0].Permission[0], 'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0].URI[0],
                constants.publicId);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1]
                .Permission[0], 'READ');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2], undefined);
            done();
        });
    });

    it('should get a canned authenticated-read ACL', done => {
        const testPutACLRequest = {
            bucketName,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'authenticated-read'
            },
            url: '/?acl',
            query: {
                acl: ''
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(authInfo, testBucketPutRequest, log,
                    next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                bucketPutACL(authInfo, testPutACLRequest, log,
                    next);
            },
            function waterfall3(result, next) {
                bucketGetACL(authInfo, testGetACLRequest, log,
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
                AccessControlList[0].Grant[1]
                .Permission[0], 'READ');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2], undefined);
            done();
        });
    });

    it('should get a canned log-delivery-write ACL', done => {
        const testPutACLRequest = {
            bucketName,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'log-delivery-write',
            },
            url: '/?acl',
            query: {
                acl: ''
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(authInfo, testBucketPutRequest, log,
                    next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                bucketPutACL(authInfo, testPutACLRequest, log,
                    next);
            },
            function waterfall3(result, next) {
                bucketGetACL(authInfo, testGetACLRequest, log,
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
                .URI[0], constants.logId);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1]
                .Permission[0], 'WRITE');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .URI[0], constants.logId);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2]
                .Permission[0], 'READ_ACP');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[3], undefined);
            done();
        });
    });

    it('should get specifically set ACLs', done => {
        const testPutACLRequest = {
            bucketName,
            namespace,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read': `uri=${constants.logId}`,
                'x-amz-grant-write': `uri=${constants.publicId}`,
                'x-amz-grant-read-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be"',
                'x-amz-grant-write-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf"',
            },
            url: '/?acl',
            query: {
                acl: ''
            }
        };
        const canonicalIDforSample1 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
        const canonicalIDforSample2 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2bf';

        async.waterfall([
            function waterfall1(next) {
                bucketPut(authInfo, testBucketPutRequest, log, next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                bucketPutACL(authInfo, testPutACLRequest, log, next);
            },
            function waterfall3(result, next) {
                bucketGetACL(authInfo, testGetACLRequest, log, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], canonicalIDforSample1);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .DisplayName[0], 'sampleAccount1@sampling.com');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .ID[0], canonicalIDforSample2);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .DisplayName[0], 'sampleAccount2@sampling.com');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .ID[0], canonicalIDforSample2);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .DisplayName[0], 'sampleAccount2@sampling.com');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Permission[0],
                'WRITE_ACP');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Grantee[0]
                .ID[0], canonicalIDforSample1);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Grantee[0]
                .DisplayName[0], 'sampleAccount1@sampling.com');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Permission[0],
                'READ_ACP');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[4].Grantee[0]
                .URI[0], constants.publicId);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[4]
                .Permission[0], 'WRITE');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[5].Grantee[0]
                .URI[0], constants.logId);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[5]
                .Permission[0], 'READ');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[6], undefined);
            done();
        });
    });
});
