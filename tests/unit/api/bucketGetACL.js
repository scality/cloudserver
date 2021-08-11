const assert = require('assert');
const async = require('async');
const { parseString } = require('xml2js');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketGetACL = require('../../../lib/api/bucketGetACL');
const bucketPutACL = require('../../../lib/api/bucketPutACL');
const constants = require('../../../constants');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');

const log = new DummyRequestLogger();
const accessKey = 'accessKey1';
const authInfo = makeAuthInfo(accessKey);
const canonicalID = authInfo.getCanonicalID();
const namespace = 'default';
const bucketName = 'bucketname';

describe('bucketGetACL API', () => {
    beforeEach(() => {
        cleanup();
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
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/?acl',
        query: { acl: '' },
    };

    it('should get a canned private ACL', done => {
        const testPutACLRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'private',
            },
            url: '/?acl',
            query: { acl: '' },
        };

        async.waterfall([
            next => bucketPut(authInfo, testBucketPutRequest, log, next),
            (corsHeaders, next) =>
                bucketPutACL(authInfo, testPutACLRequest, log, next),
            (corsHeaders, next) => bucketGetACL(authInfo,
                testGetACLRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ], (err, result) => {
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
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'public-read-write',
            },
            url: '/?acl',
            query: { acl: '' },
        };

        async.waterfall([
            next => bucketPut(authInfo, testBucketPutRequest, log, next),
            (corsHeaders, next) =>
                bucketPutACL(authInfo, testPutACLRequest, log, next),
            (corsHeaders, next) => bucketGetACL(authInfo, testGetACLRequest,
                log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ], (err, result) => {
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
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'public-read',
            },
            url: '/?acl',
            query: { acl: '' },
        };

        async.waterfall([
            next => bucketPut(authInfo, testBucketPutRequest, log, next),
            (corsHeaders, next) =>
                bucketPutACL(authInfo, testPutACLRequest, log, next),
            (corsHeaders, next) => bucketGetACL(authInfo, testGetACLRequest,
                log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ], (err, result) => {
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
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'authenticated-read',
            },
            url: '/?acl',
            query: { acl: '' },
        };

        async.waterfall([
            next => bucketPut(authInfo, testBucketPutRequest, log, next),
            (corsHeaders, next) =>
                bucketPutACL(authInfo, testPutACLRequest, log, next),
            (corsHeaders, next) => bucketGetACL(authInfo, testGetACLRequest,
                log, next),
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
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'log-delivery-write',
            },
            url: '/?acl',
            query: { acl: '' },
        };

        async.waterfall([
            next => bucketPut(authInfo, testBucketPutRequest, log, next),
            (corsHeaders, next) =>
                bucketPutACL(authInfo, testPutACLRequest, log, next),
            (corsHeaders, next) => bucketGetACL(authInfo, testGetACLRequest,
                log, next),
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
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read': `uri=${constants.logId}`,
                'x-amz-grant-write': `uri=${constants.publicId}`,
                'x-amz-grant-read-acp':
                    'id=79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be',
                'x-amz-grant-write-acp':
                    'id=79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf',
            },
            url: '/?acl',
            query: { acl: '' },
        };
        const canonicalIDforSample1 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
        const canonicalIDforSample2 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2bf';

        async.waterfall([
            next => bucketPut(authInfo, testBucketPutRequest, log, next),
            (corsHeaders, next) =>
                bucketPutACL(authInfo, testPutACLRequest, log, next),
            (corsHeaders, next) => bucketGetACL(authInfo, testGetACLRequest,
                log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ], (err, result) => {
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0], canonicalIDforSample1);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .DisplayName[0], 'sampleaccount1@sampling.com');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .ID[0], canonicalIDforSample2);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .DisplayName[0], 'sampleaccount2@sampling.com');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0],
                'FULL_CONTROL');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .ID[0], canonicalIDforSample2);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .DisplayName[0], 'sampleaccount2@sampling.com');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Permission[0],
                'WRITE_ACP');
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Grantee[0]
                .ID[0], canonicalIDforSample1);
            assert.strictEqual(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Grantee[0]
                .DisplayName[0], 'sampleaccount1@sampling.com');
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

    const grantsByURI = [
        constants.publicId,
        constants.allAuthedUsersId,
        constants.logId,
    ];

    grantsByURI.forEach(uri => {
        it('should get all ACLs when predefined group - ' +
        `${uri} is used for multiple grants`, done => {
            const testPutACLRequest = {
                bucketName,
                namespace,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'x-amz-grant-full-control': `uri = ${uri}`,
                    'x-amz-grant-read': `uri = ${uri}`,
                    'x-amz-grant-write': `uri = ${uri}`,
                    'x-amz-grant-read-acp': `uri = ${uri}`,
                    'x-amz-grant-write-acp': `uri = ${uri}`,
                },
                url: '/?acl',
                query: { acl: '' },
            };

            async.waterfall([
                next => bucketPut(authInfo, testBucketPutRequest,
                    log, next), (corsHeaders, next) =>
                    bucketPutACL(authInfo, testPutACLRequest, log, next),
                (corsHeaders, next) => bucketGetACL(authInfo,
                    testGetACLRequest, log, next),
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

    it('should get all ACLs when predefined groups are used for ' +
    'more than one grant', done => {
        const { allAuthedUsersId, publicId } = constants;
        const testPutACLRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-write': `uri = ${allAuthedUsersId} `,
                'x-amz-grant-write-acp': `uri = ${allAuthedUsersId} `,
                'x-amz-grant-read': `uri = ${publicId} `,
                'x-amz-grant-read-acp': `uri = ${publicId} `,
            },
            url: '/?acl',
            query: { acl: '' },
        };

        async.waterfall([
            next => bucketPut(authInfo, testBucketPutRequest, log, next),
            (corsHeaders, next) =>
                bucketPutACL(authInfo, testPutACLRequest, log, next),
            (corsHeaders, next) => bucketGetACL(authInfo, testGetACLRequest,
                log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ], (err, result) => {
            assert.ifError(err);
            const grants =
                result.AccessControlPolicy.AccessControlList[0].Grant;
            grants.forEach(grant => {
                const permissions = grant.Permission;
                assert.strictEqual(permissions.length, 1);
                const permission = permissions[0];
                assert.strictEqual(grant.Grantee.length, 1);
                const grantees = grant.Grantee[0].URI;
                assert.strictEqual(grantees.length, 1);
                const grantee = grantees[0];
                if (['WRITE', 'WRITE_ACP'].includes(permission)) {
                    assert.strictEqual(grantee, constants.allAuthedUsersId);
                }
                if (['READ', 'READ_ACP'].includes(permission)) {
                    assert.strictEqual(grantee, constants.publicId);
                }
            });
            done();
        });
    });
});
