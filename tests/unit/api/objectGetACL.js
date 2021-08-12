const assert = require('assert');
const async = require('async');
const { parseString } = require('xml2js');
const { errors } = require('arsenal');

const { bucketPut } = require('../../../lib/api/bucketPut');
const constants = require('../../../constants');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const objectPut = require('../../../lib/api/objectPut');
const objectGetACL = require('../../../lib/api/objectGetACL');
const DummyRequest = require('../DummyRequest');

const log = new DummyRequestLogger();
const accessKey = 'accessKey1';
const authInfo = makeAuthInfo(accessKey);
const canonicalID = authInfo.getCanonicalID();
const otherAccountAccessKey = 'accessKey2';
const otherAccountAuthInfo = makeAuthInfo(otherAccountAccessKey);
const otherAccountCanonicalID = otherAccountAuthInfo.getCanonicalID();
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = Buffer.from('I am a body', 'utf8');

describe('objectGetACL API', () => {
    beforeEach(() => {
        cleanup();
    });

    const objectName = 'objectName';
    const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
    const testBucketPutRequest = {
        bucketName,
        namespace,
        headers: {
            'host': `${bucketName}.s3.amazonaws.com`,
            'x-amz-acl': 'public-read-write',
        },
        url: '/',
    };
    const testGetACLRequest = {
        bucketName,
        namespace,
        headers: {},
        objectKey: objectName,
        url: `/${bucketName}/${objectName}?acl`,
        query: { acl: '' },
    };

    it('should get a canned private ACL', done => {
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-acl': 'private' },
            url: `/${bucketName}/${objectName}`,
            post: postBody,
        }, postBody);
        async.waterfall([
            next => bucketPut(authInfo, testBucketPutRequest, log, next),
            (corsHeaders, next) => objectPut(authInfo, testPutObjectRequest,
                undefined, log, next),
            (resHeaders, next) => {
                assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                objectGetACL(authInfo, testGetACLRequest, log, next);
            },
            (result, corsHeaders, next) => parseString(result, next),
        ], (err, result) => {
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
    'for a nonexistent object', done => {
        bucketPut(authInfo, testBucketPutRequest, log, () => {
            objectGetACL(authInfo, testGetACLRequest, log, err => {
                assert.deepStrictEqual(err, errors.NoSuchKey);
                done();
            });
        });
    });

    it('should get a canned public-read ACL', done => {
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-acl': 'public-read' },
            url: `/${bucketName}/${objectName}`,
        }, postBody);
        async.waterfall([
            next => bucketPut(authInfo, testBucketPutRequest, log, next),
            (corsHeaders, next) => objectPut(authInfo, testPutObjectRequest,
                undefined, log, next),
            (resHeaders, next) => {
                assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                objectGetACL(authInfo, testGetACLRequest, log, next);
            },
            (result, corsHeaders, next) => parseString(result, next),
        ], (err, result) => {
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

    it('should get a canned public-read-write ACL', done => {
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-acl': 'public-read-write' },
            url: `/${bucketName}/${objectName}`,
        }, postBody);
        async.waterfall([
            next => bucketPut(authInfo, testBucketPutRequest, log, next),
            (corsHeaders, next) => objectPut(authInfo, testPutObjectRequest,
                undefined, log, next),
            (resHeaders, next) => {
                assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                objectGetACL(authInfo, testGetACLRequest, log, next);
            },
            (result, corsHeaders, next) => parseString(result, next),
        ], (err, result) => {
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

    it('should get a canned authenticated-read ACL', done => {
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-acl': 'authenticated-read' },
            url: `/${bucketName}/${objectName}`,
        }, postBody);
        async.waterfall([
            next => bucketPut(authInfo, testBucketPutRequest, log, next),
            (corsHeaders, next) => objectPut(authInfo, testPutObjectRequest,
                undefined, log, next),
            (resHeaders, next) => {
                assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                objectGetACL(authInfo, testGetACLRequest, log, next);
            },
            (result, corsHeaders, next) => parseString(result, next),
        ], (err, result) => {
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

    it('should get a canned bucket-owner-read ACL', done => {
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-acl': 'bucket-owner-read' },
            url: `/${bucketName}/${objectName}`,
            post: postBody,
        }, postBody);
        async.waterfall([
            next =>
                bucketPut(otherAccountAuthInfo, testBucketPutRequest,
                    log, next),
            (corsHeaders, next) => objectPut(
                authInfo, testPutObjectRequest, undefined, log, next),
            (resHeaders, next) => {
                assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                objectGetACL(authInfo, testGetACLRequest, log, next);
            },
            (result, corsHeaders, next) => parseString(result, next),
        ], (err, result) => {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], canonicalID);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .ID[0], otherAccountCanonicalID);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0],
                'READ');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2], undefined);
            done();
        });
    });

    it('should get a canned bucket-owner-full-control ACL', done => {
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-acl': 'bucket-owner-full-control' },
            url: `/${bucketName}/${objectName}`,
            calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
        }, postBody);
        async.waterfall([
            next =>
                bucketPut(otherAccountAuthInfo, testBucketPutRequest,
                    log, next),
            (corsHeaders, next) => objectPut(authInfo, testPutObjectRequest,
                undefined, log, next),
            (resHeaders, next) => {
                assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                objectGetACL(authInfo, testGetACLRequest, log, next);
            },
            (result, corsHeaders, next) => parseString(result, next),
        ], (err, result) => {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], canonicalID);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .ID[0], otherAccountCanonicalID);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2], undefined);
            done();
        });
    });

    it('should get specifically set ACLs', done => {
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read': `uri=${constants.allAuthedUsersId}`,
                'x-amz-grant-write': `uri=${constants.publicId}`,
                'x-amz-grant-read-acp':
                    'id=79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be',
                'x-amz-grant-write-acp':
                    'id=79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf',
            },
            url: `/${bucketName}/${objectName}`,
        }, postBody);
        async.waterfall([
            next => bucketPut(authInfo, testBucketPutRequest,
                log, next),
            (corsHeaders, next) => objectPut(authInfo, testPutObjectRequest,
                undefined, log, next),
            (resHeaders, next) => {
                assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                objectGetACL(authInfo, testGetACLRequest, log, next);
            },
            (result, corsHeaders, next) => parseString(result, next),
        ], (err, result) => {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], '79a59df900b949e55d96a1e698fbacedfd6e09d98' +
            'eacf8f8d5218e7cd47ef2be');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .DisplayName[0], 'sampleaccount1@sampling.com');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .ID[0], '79a59df900b949e55d96a1e698fbacedfd6e09d98' +
            'eacf8f8d5218e7cd47ef2bf');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .DisplayName[0], 'sampleaccount2@sampling.com');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .ID[0], '79a59df900b949e55d96a1e698fbacedfd6e09d98' +
            'eacf8f8d5218e7cd47ef2bf');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .DisplayName[0], 'sampleaccount2@sampling.com');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Permission[0],
                'WRITE_ACP');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Grantee[0]
                .ID[0], '79a59df900b949e55d96a1e698fbacedfd6e09d98' +
            'eacf8f8d5218e7cd47ef2be');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Grantee[0]
                .DisplayName[0], 'sampleaccount1@sampling.com');
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

    const grantsByURI = [
        constants.publicId,
        constants.allAuthedUsersId,
    ];

    grantsByURI.forEach(uri => {
        it('should get all ACLs when predefined group - ' +
        `${uri} is used for multiple grants`, done => {
            const testPutObjectRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey: objectName,
                headers: {
                    'x-amz-grant-full-control': `uri=${uri}`,
                    'x-amz-grant-read': `uri=${uri}`,
                    'x-amz-grant-read-acp': `uri=${uri}`,
                    'x-amz-grant-write-acp': `uri=${uri}`,
                },
                url: `/${bucketName}/${objectName}`,
            }, postBody);
            async.waterfall([
                next => bucketPut(authInfo, testBucketPutRequest,
                    log, next),
                (corsHeaders, next) => objectPut(authInfo,
                    testPutObjectRequest, undefined, log, next),
                (resHeaders, next) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectGetACL(authInfo, testGetACLRequest, log, next);
                },
                (result, corsHeaders, next) => parseString(result, next),
            ], (err, result) => {
                assert.ifError(err);
                const grants =
                    result.AccessControlPolicy.AccessControlList[0].Grant;
                grants.forEach(grant => {
                    assert.strictEqual(grant.Permission.length, 1);
                    assert.strictEqual(grant.Grantee.length, 1);
                    assert.strictEqual(grant.Grantee[0].URI.length, 1);
                    assert.strictEqual(grant.Grantee[0].URI[0], `${uri}`);
                });
                done();
            });
        });
    });
});
