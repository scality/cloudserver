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

    test('should get a canned private ACL', done => {
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
        ],
        (err, result) => {
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0]).toBe(canonicalID);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0]).toBe('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1]).toBe(undefined);
            done();
        });
    });

    test('should get a canned public-read-write ACL', done => {
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
        ],
        (err, result) => {
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0]).toBe(canonicalID);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0]).toBe('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0].URI[0]).toBe(constants.publicId);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1]
                .Permission[0]).toBe('READ');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0].URI[0]).toBe(constants.publicId);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2]
                .Permission[0]).toBe('WRITE');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[3]).toBe(undefined);
            done();
        });
    });

    test('should get a canned public-read ACL', done => {
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
        ],
        (err, result) => {
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0]).toBe(canonicalID);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0]).toBe('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0].URI[0]).toBe(constants.publicId);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1]
                .Permission[0]).toBe('READ');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2]).toBe(undefined);
            done();
        });
    });

    test('should get a canned authenticated-read ACL', done => {
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
        ],
        (err, result) => {
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0]).toBe(canonicalID);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0]).toBe('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .URI[0]).toBe(constants.allAuthedUsersId);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1]
                .Permission[0]).toBe('READ');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2]).toBe(undefined);
            done();
        });
    });

    test('should get a canned log-delivery-write ACL', done => {
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
        ],
        (err, result) => {
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0]).toBe(canonicalID);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0]).toBe('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .URI[0]).toBe(constants.logId);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1]
                .Permission[0]).toBe('WRITE');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .URI[0]).toBe(constants.logId);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2]
                .Permission[0]).toBe('READ_ACP');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[3]).toBe(undefined);
            done();
        });
    });

    test('should get specifically set ACLs', done => {
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
        ],
        (err, result) => {
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .ID[0]).toBe(canonicalIDforSample1);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Grantee[0]
                .DisplayName[0]).toBe('sampleaccount1@sampling.com');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[0].Permission[0]).toBe('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .ID[0]).toBe(canonicalIDforSample2);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Grantee[0]
                .DisplayName[0]).toBe('sampleaccount2@sampling.com');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[1].Permission[0]).toBe('FULL_CONTROL');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .ID[0]).toBe(canonicalIDforSample2);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Grantee[0]
                .DisplayName[0]).toBe('sampleaccount2@sampling.com');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[2].Permission[0]).toBe('WRITE_ACP');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Grantee[0]
                .ID[0]).toBe(canonicalIDforSample1);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Grantee[0]
                .DisplayName[0]).toBe('sampleaccount1@sampling.com');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[3].Permission[0]).toBe('READ_ACP');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[4].Grantee[0]
                .URI[0]).toBe(constants.publicId);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[4]
                .Permission[0]).toBe('WRITE');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[5].Grantee[0]
                .URI[0]).toBe(constants.logId);
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[5]
                .Permission[0]).toBe('READ');
            expect(result.AccessControlPolicy.
                AccessControlList[0].Grant[6]).toBe(undefined);
            done();
        });
    });
});
