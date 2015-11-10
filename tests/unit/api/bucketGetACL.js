import { expect } from 'chai';
import async from 'async';
import { parseString } from 'xml2js';
import bucketPut from '../../../lib/api/bucketPut';
import bucketGetACL from '../../../lib/api/bucketGetACL';
import bucketPutACL from '../../../lib/api/bucketPutACL';

const accessKey = 'accessKey1';
const namespace = 'default';

describe('bucketGetACL API', () => {
    let metastore;

    beforeEach(() => {
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
    });
    const bucketName = 'bucketname';
    const testBucketPutRequest = {
        lowerCaseHeaders: {},
        headers: {host: `${bucketName}.s3.amazonaws.com`},
        url: '/',
        namespace: namespace
    };
    const testGetACLRequest = {
        lowerCaseHeaders: {
            host: `${bucketName}.s3.amazonaws.com`
        },
        headers: {
            host: `${bucketName}.s3.amazonaws.com`
        },
        url: '/?acl',
        namespace: namespace,
        query: {
            acl: ''
        }
    };

    it('should get a canned private ACL', (done) => {
        const testPutACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'private'
            },
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'private'
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(success, next) {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testPutACLRequest, next);
            },
            function waterfall3(result, next) {
                bucketGetACL(accessKey, metastore, testGetACLRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0]).to.equal('accessKey1');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0])
                .to.equal('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1])
                .to.be.undefined;
            done();
        });
    });

    it('should get a canned public-read-write ACL', (done) => {
        const testPutACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'public-read-write'
            },
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'public-read-write'
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(success, next) {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testPutACLRequest, next);
            },
            function waterfall3(result, next) {
                bucketGetACL(accessKey, metastore, testGetACLRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0]).to.equal('accessKey1');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0])
                .to.equal('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .URI[0]).to.equal(
                    'http://acs.amazonaws.com/groups/global/AllUsers');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1]
                .Permission[0]).to.equal('READ');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .URI[0]).to.equal(
                    'http://acs.amazonaws.com/groups/global/AllUsers');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2]
                .Permission[0]).to.equal('WRITE');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[3])
                .to.be.undefined;
            done();
        });
    });

    it('should get a canned public-read ACL', (done) => {
        const testPutACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'public-read'
            },
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'public-read'
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(success, next) {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testPutACLRequest, next);
            },
            function waterfall3(result, next) {
                bucketGetACL(accessKey, metastore, testGetACLRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0]).to.equal('accessKey1');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0])
                .to.equal('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .URI[0]).to.equal(
                    'http://acs.amazonaws.com/groups/global/AllUsers');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1]
                .Permission[0]).to.equal('READ');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2])
                .to.be.undefined;
            done();
        });
    });

    it('should get a canned authenticated-read ACL', (done) => {
        const testPutACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'authenticated-read'
            },
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'authenticated-read'
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(success, next) {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testPutACLRequest, next);
            },
            function waterfall3(result, next) {
                bucketGetACL(accessKey, metastore, testGetACLRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0]).to.equal('accessKey1');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0])
                .to.equal('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .URI[0]).to.equal(
                    'http://acs.amazonaws.com/groups/' +
                    'global/AuthenticatedUsers');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1]
                .Permission[0]).to.equal('READ');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2])
                .to.be.undefined;
            done();
        });
    });

    it('should get a canned log-delivery-write ACL', (done) => {
        const testPutACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'log-delivery-write'
            },
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'log-delivery-write'
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(success, next) {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testPutACLRequest, next);
            },
            function waterfall3(result, next) {
                bucketGetACL(accessKey, metastore, testGetACLRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0]).to.equal('accessKey1');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0])
                .to.equal('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .URI[0]).to.equal(
                    'http://acs.amazonaws.com/groups/' +
                    's3/LogDelivery');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1]
                .Permission[0]).to.equal('WRITE');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .URI[0]).to.equal(
                    'http://acs.amazonaws.com/groups/' +
                    's3/LogDelivery');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2]
                .Permission[0]).to.equal('READ_ACP');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[3])
                .to.be.undefined;
            done();
        });
    });

    it('should get specifically set ACLs', (done) => {
        const testPutACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read':
                    'uri="http://acs.amazonaws.com/groups/s3/LogDelivery"',
                'x-amz-grant-write':
                    'uri="http://acs.amazonaws.com/groups/global/AllUsers"',
                'x-amz-grant-read-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be"',
                'x-amz-grant-write-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf"',
            },
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read':
                    'uri="http://acs.amazonaws.com/groups/s3/LogDelivery"',
                'x-amz-grant-write':
                        'uri="http://acs.amazonaws.com/groups/global/AllUsers"',
                'x-amz-grant-read-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be"',
                'x-amz-grant-write-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf"',
            },
            url: '/?acl',
            namespace: namespace,
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
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(success, next) {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testPutACLRequest, next);
            },
            function waterfall3(result, next) {
                bucketGetACL(accessKey, metastore, testGetACLRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0]).to.equal(canonicalIDforSample1);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .DisplayName[0]).to.equal('sampleAccount1@sampling.com');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0])
                .to.equal('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .ID[0]).to.equal(canonicalIDforSample2);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .DisplayName[0]).to.equal('sampleAccount2@sampling.com');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0])
                .to.equal('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .ID[0]).to.equal(canonicalIDforSample2);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .DisplayName[0]).to.equal('sampleAccount2@sampling.com');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Permission[0])
                .to.equal('WRITE_ACP');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Grantee[0]
                .ID[0]).to.equal(canonicalIDforSample1);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Grantee[0]
                .DisplayName[0]).to.equal('sampleAccount1@sampling.com');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Permission[0])
                .to.equal('READ_ACP');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[4].Grantee[0]
                .URI[0]).to.equal(
                    'http://acs.amazonaws.com/groups/' +
                    'global/AllUsers');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[4]
                .Permission[0]).to.equal('WRITE');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[5].Grantee[0]
                .URI[0]).to.equal(
                    'http://acs.amazonaws.com/groups/' +
                    's3/LogDelivery');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[5]
                .Permission[0]).to.equal('READ');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[6])
                .to.be.undefined;
            done();
        });
    });
});
