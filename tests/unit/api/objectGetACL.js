import { expect } from 'chai';
import async from 'async';
import { parseString } from 'xml2js';
import bucketPut from '../../../lib/api/bucketPut';
import objectPut from '../../../lib/api/objectPut';
import objectGetACL from '../../../lib/api/objectGetACL';

const accessKey = 'accessKey1';
const namespace = 'default';

describe('objectGetACL API', () => {
    let metastore;
    let datastore;

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
        datastore = {};
    });
    const bucketName = 'bucketname';
    const objectName = 'objectName';
    const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
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
            calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(result, next) {
                expect(result).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore, testPutObjectRequest,
                        next);
            },
            function waterfall3(result, next) {
                expect(result).to.equal(correctMD5);
                objectGetACL(accessKey, metastore, testGetACLRequest, next);
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
            calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(result, next) {
                expect(result).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore, testPutObjectRequest,
                        next);
            },
            function waterfall3(result, next) {
                expect(result).to.equal(correctMD5);
                objectGetACL(accessKey, metastore, testGetACLRequest, next);
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
                .URI[0])
                .to.equal('http://acs.amazonaws.com/groups/global/AllUsers');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0])
                .to.equal('READ');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2])
                .to.be.undefined;
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
            calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(result, next) {
                expect(result).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore, testPutObjectRequest,
                        next);
            },
            function waterfall3(result, next) {
                expect(result).to.equal(correctMD5);
                objectGetACL(accessKey, metastore, testGetACLRequest, next);
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
                .URI[0])
                .to.equal('http://acs.amazonaws.com/groups/global/AllUsers');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0])
                .to.equal('READ');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .URI[0])
                .to.equal('http://acs.amazonaws.com/groups/global/AllUsers');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Permission[0])
                .to.equal('WRITE');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[3]).to.be.undefined;
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
            calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(result, next) {
                expect(result).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore, testPutObjectRequest,
                        next);
            },
            function waterfall3(result, next) {
                expect(result).to.equal(correctMD5);
                objectGetACL(accessKey, metastore, testGetACLRequest, next);
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
                .URI[0])
                .to.equal('http://acs.amazonaws.com/' +
                    'groups/global/AuthenticatedUsers');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0])
                .to.equal('READ');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2])
                .to.be.undefined;
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
            calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(result, next) {
                expect(result).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore, testPutObjectRequest,
                        next);
            },
            function waterfall3(result, next) {
                expect(result).to.equal(correctMD5);
                objectGetACL(accessKey, metastore, testGetACLRequest, next);
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
                .ID[0]).to.equal('accessKey1');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0])
                .to.equal('READ');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2])
                .to.be.undefined;
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
            calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(result, next) {
                expect(result).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore, testPutObjectRequest,
                        next);
            },
            function waterfall3(result, next) {
                expect(result).to.equal(correctMD5);
                objectGetACL(accessKey, metastore, testGetACLRequest, next);
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
                .ID[0]).to.equal('accessKey1');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0])
                .to.equal('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2])
                .to.be.undefined;
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
            calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testBucketPutRequest, next);
            },
            function waterfall2(result, next) {
                expect(result).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore, testPutObjectRequest,
                        next);
            },
            function waterfall3(result, next) {
                expect(result).to.equal(correctMD5);
                objectGetACL(accessKey, metastore, testGetACLRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0]).to.equal('79a59df900b949e55d96a1e698fbacedfd6e09d98' +
                'eacf8f8d5218e7cd47ef2be');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .DisplayName[0]).to.equal('sampleAccount1@sampling.com');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0])
                .to.equal('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .ID[0]).to.equal('79a59df900b949e55d96a1e698fbacedfd6e09d98' +
                'eacf8f8d5218e7cd47ef2bf');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .DisplayName[0]).to.equal('sampleAccount2@sampling.com');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0])
                .to.equal('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .ID[0]).to.equal('79a59df900b949e55d96a1e698fbacedfd6e09d98' +
                'eacf8f8d5218e7cd47ef2bf');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .DisplayName[0]).to.equal('sampleAccount2@sampling.com');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Permission[0])
                .to.equal('WRITE_ACP');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Grantee[0]
                .ID[0]).to.equal('79a59df900b949e55d96a1e698fbacedfd6e09d98' +
                'eacf8f8d5218e7cd47ef2be');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Grantee[0]
                .DisplayName[0]).to.equal('sampleAccount1@sampling.com');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Permission[0])
                .to.equal('READ_ACP');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[4].Grantee[0]
                .URI[0]).to.equal('http://acs.amazonaws.com/groups/global/' +
                'AuthenticatedUsers');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[4].Permission[0])
                .to.equal('READ');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[5])
                .to.be.undefined;
            done();
        });
    });
});
