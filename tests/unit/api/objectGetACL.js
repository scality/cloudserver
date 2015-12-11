import assert from 'assert';
import async from 'async';
import { parseString } from 'xml2js';

import bucketPut from '../../../lib/api/bucketPut';
import metadata from '../../../lib/metadata/wrapper';
import objectPut from '../../../lib/api/objectPut';
import objectGetACL from '../../../lib/api/objectGetACL';
import utils from '../../../lib/utils';

const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
const testBucketUID = utils.getResourceUID(namespace, bucketName);

describe('objectGetACL API', () => {
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

    const objectName = 'objectName';
    const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
    const testBucketPutRequest = {
        lowerCaseHeaders: {},
        headers: {host: `${bucketName}.s3.amazonaws.com`},
        url: '/',
        namespace: namespace
    };
    const testGetACLRequest = {
        url: `/${bucketName}/${objectName}?acl`,
        namespace: namespace,
        query: {
            acl: ''
        }
    };

    it('should get a canned private ACL', (done) => {
        const testPutObjectRequest = {
            lowerCaseHeaders: {
                'x-amz-acl': 'private'
            },
            headers: {
                'x-amz-acl': 'private'
            },
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: 'I am a post body',
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(result, next) {
                assert.strictEqual(result, 'Bucket created');
                objectPut(accessKey, metastore, testPutObjectRequest, next);
            },
            function waterfall3(result, next) {
                assert.strictEqual(result, correctMD5);
                objectGetACL(accessKey, metastore, testGetACLRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], 'accessKey1');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            done();
        });
    });

    it('should get a canned public-read ACL', (done) => {
        const testPutObjectRequest = {
            lowerCaseHeaders: {
                'x-amz-acl': 'public-read'
            },
            headers: {
                'x-amz-acl': 'public-read'
            },
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: 'I am a post body',
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(result, next) {
                assert.strictEqual(result, 'Bucket created');
                objectPut(accessKey, metastore, testPutObjectRequest, next);
            },
            function waterfall3(result, next) {
                assert.strictEqual(result, correctMD5);
                objectGetACL(accessKey, metastore, testGetACLRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], 'accessKey1');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .URI[0],
                'http://acs.amazonaws.com/groups/global/AllUsers');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0], 'READ');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2], undefined);
            done();
        });
    });

    it('should get a canned public-read-write ACL', (done) => {
        const testPutObjectRequest = {
            lowerCaseHeaders: {
                'x-amz-acl': 'public-read-write'
            },
            headers: {
                'x-amz-acl': 'public-read-write'
            },
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: 'I am a post body',
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(result, next) {
                assert.strictEqual(result, 'Bucket created');
                objectPut(accessKey, metastore, testPutObjectRequest, next);
            },
            function waterfall3(result, next) {
                assert.strictEqual(result, correctMD5);
                objectGetACL(accessKey, metastore, testGetACLRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], 'accessKey1');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .URI[0],
                'http://acs.amazonaws.com/groups/global/AllUsers');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0],
                'READ');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .URI[0],
                'http://acs.amazonaws.com/groups/global/AllUsers');
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
            lowerCaseHeaders: {
                'x-amz-acl': 'authenticated-read'
            },
            headers: {
                'x-amz-acl': 'authenticated-read'
            },
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: 'I am a post body',
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(result, next) {
                assert.strictEqual(result, 'Bucket created');
                objectPut(accessKey, metastore, testPutObjectRequest, next);
            },
            function waterfall3(result, next) {
                assert.strictEqual(result, correctMD5);
                objectGetACL(accessKey, metastore, testGetACLRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], 'accessKey1');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .URI[0],
                'http://acs.amazonaws.com/' +
                'groups/global/AuthenticatedUsers');
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
            lowerCaseHeaders: {
                'x-amz-acl': 'bucket-owner-read'
            },
            headers: {
                'x-amz-acl': 'bucket-owner-read'
            },
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: 'I am a post body',
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(result, next) {
                assert.strictEqual(result, 'Bucket created');
                objectPut(accessKey, metastore, testPutObjectRequest, next);
            },
            function waterfall3(result, next) {
                assert.strictEqual(result, correctMD5);
                objectGetACL(accessKey, metastore, testGetACLRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], 'accessKey1');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .ID[0], 'accessKey1');
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
            lowerCaseHeaders: {
                'x-amz-acl': 'bucket-owner-full-control'
            },
            headers: {
                'x-amz-acl': 'bucket-owner-full-control'
            },
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: 'I am a post body',
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(result, next) {
                assert.strictEqual(result, 'Bucket created');
                objectPut(accessKey, metastore, testPutObjectRequest, next);
            },
            function waterfall3(result, next) {
                assert.strictEqual(result, correctMD5);
                objectGetACL(accessKey, metastore, testGetACLRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], 'accessKey1');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .ID[0], 'accessKey1');
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
            lowerCaseHeaders: {
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read':
                    'uri="http://acs.amazonaws.com/groups/global/' +
                    'AuthenticatedUsers"',
                'x-amz-grant-write':
                    'uri="http://acs.amazonaws.com/groups/global/AllUsers"',
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
                'x-amz-grant-read':
                    'uri="http://acs.amazonaws.com/groups/global/' +
                    'AuthenticatedUsers"',
                'x-amz-grant-write':
                    'uri="http://acs.amazonaws.com/groups/global/AllUsers"',
                'x-amz-grant-read-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be"',
                'x-amz-grant-write-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf"',            },
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: 'I am a post body',
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(result, next) {
                assert.strictEqual(result, 'Bucket created');
                objectPut(accessKey, metastore, testPutObjectRequest, next);
            },
            function waterfall3(result, next) {
                assert.strictEqual(result, correctMD5);
                objectGetACL(accessKey, metastore, testGetACLRequest, next);
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
                .URI[0], 'http://acs.amazonaws.com/groups/global/' +
                'AuthenticatedUsers');
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
