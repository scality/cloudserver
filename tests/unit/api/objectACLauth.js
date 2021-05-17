const assert = require('assert');

const BucketInfo = require('arsenal').models.BucketInfo;
const constants = require('../../../constants');
const { isObjAuthorized }
    = require('../../../lib/api/apiUtils/authorization/permissionChecks');
const { DummyRequestLogger, makeAuthInfo } = require('../helpers');

const accessKey = 'accessKey1';
const altAccessKey = 'accessKey2';
const authInfo = makeAuthInfo(accessKey);
const bucketOwnerCanonicalId = authInfo.getCanonicalID();
const creationDate = new Date().toJSON();
const userAuthInfo = makeAuthInfo(accessKey, 'user');
const altAcctAuthInfo = makeAuthInfo(altAccessKey);
const accountToVet = altAcctAuthInfo.getCanonicalID();

const bucket = new BucketInfo('niftyBucket', bucketOwnerCanonicalId,
    'iAmTheOwnerDisplayName', creationDate);
const objectOwnerCanonicalId = userAuthInfo.getCanonicalID();
const object = {
    'owner-id': objectOwnerCanonicalId,
    'acl': {
        Canned: 'private',
        FULL_CONTROL: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    },
};
const log = new DummyRequestLogger();

describe('object acl authorization for objectGet and objectHead', () => {
    // Reset the object ACLs
    afterEach(() => {
        object.acl = {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        };
    });
    const requestTypes = ['objectGet', 'objectHead'];

    it('should allow access to object owner', () => {
        const results = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, objectOwnerCanonicalId,
                authInfo, log));
        assert.deepStrictEqual(results, [true, true]);
    });

    it('should allow access to user in object owner account', () => {
        const results = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, objectOwnerCanonicalId,
                userAuthInfo, log));
        assert.deepStrictEqual(results, [true, true]);
    });

    it('should allow access to bucket owner if same account as object owner', () => {
        const results = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, bucketOwnerCanonicalId,
                authInfo, log));
        assert.deepStrictEqual(results, [true, true]);
    });

    it('should allow access to anyone if canned public-read ACL', () => {
        object.acl.Canned = 'public-read';
        const results = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, accountToVet, altAcctAuthInfo, log));
        assert.deepStrictEqual(results, [true, true]);
    });

    it('should allow access to anyone if canned public-read-write ACL', () => {
        object.acl.Canned = 'public-read-write';
        const results = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, accountToVet, altAcctAuthInfo, log));
        assert.deepStrictEqual(results, [true, true]);
    });

    it('should not allow access to public user if ' +
        'authenticated-read ACL', () => {
        object.acl.Canned = 'authenticated-read';
        const publicResults = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, constants.publicId, null, log));
        assert.deepStrictEqual(publicResults, [false, false]);
    });

    it('should allow access to any authenticated user if ' +
        'authenticated-read ACL', () => {
        object.acl.Canned = 'authenticated-read';
        const results = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, accountToVet, altAcctAuthInfo, log));
        assert.deepStrictEqual(results, [true, true]);
    });

    it('should allow access to bucker owner when object owner is alt account if ' +
        'bucket-owner-read ACL', () => {
        const altAcctObj = {
            'owner-id': accountToVet,
            'acl': {
                Canned: 'private',
                FULL_CONTROL: [],
                WRITE_ACP: [],
                READ: [],
                READ_ACP: [],
            },
        };
        const noAuthResults = requestTypes.map(type =>
            isObjAuthorized(bucket, altAcctObj, type, bucketOwnerCanonicalId, authInfo,
                log));
        assert.deepStrictEqual(noAuthResults, [false, false]);
        altAcctObj.acl.Canned = 'bucket-owner-read';
        const authResults = requestTypes.map(type =>
            isObjAuthorized(bucket, altAcctObj, type, bucketOwnerCanonicalId, authInfo,
                log));
        assert.deepStrictEqual(authResults, [true, true]);
    });

    it('should allow access to bucker owner when object owner is alt account if ' +
        'bucket-owner-full-control ACL', () => {
        const altAcctObj = {
            'owner-id': accountToVet,
            'acl': {
                Canned: 'private',
                FULL_CONTROL: [],
                WRITE_ACP: [],
                READ: [],
                READ_ACP: [],
            },
        };
        const noAuthResults = requestTypes.map(type =>
            isObjAuthorized(bucket, altAcctObj, type, bucketOwnerCanonicalId, authInfo,
                log));
        assert.deepStrictEqual(noAuthResults, [false, false]);
        altAcctObj.acl.Canned = 'bucket-owner-full-control';
        const authResults = requestTypes.map(type =>
            isObjAuthorized(bucket, altAcctObj, type, bucketOwnerCanonicalId, authInfo,
                log));
        assert.deepStrictEqual(authResults, [true, true]);
    });

    it('should allow access to account if ' +
        'account was granted FULL_CONTROL', () => {
        const noAuthResults = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, accountToVet, altAcctAuthInfo, log));
        assert.deepStrictEqual(noAuthResults, [false, false]);
        object.acl.FULL_CONTROL = [accountToVet];
        const authResults = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, accountToVet, altAcctAuthInfo, log));
        assert.deepStrictEqual(authResults, [true, true]);
    });

    it('should allow access to account if ' +
        'account was granted READ right', () => {
        const noAuthResults = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, accountToVet, altAcctAuthInfo, log));
        assert.deepStrictEqual(noAuthResults, [false, false]);
        object.acl.READ = [accountToVet];
        const authResults = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, accountToVet, altAcctAuthInfo, log));
        assert.deepStrictEqual(authResults, [true, true]);
    });

    it('should not allow access to public user if private canned ACL', () => {
        const results = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, accountToVet, altAcctAuthInfo, log));
        assert.deepStrictEqual(results, [false, false]);
    });

    it('should not allow access to just any user if private canned ACL', () => {
        const results = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, accountToVet, altAcctAuthInfo, log));
        assert.deepStrictEqual(results, [false, false]);
    });
});

describe('object authorization for objectPut and objectDelete', () => {
    it('should allow access to anyone since checks ' +
        'are done at bucket level', () => {
        const requestTypes = ['objectPut', 'objectDelete'];
        const results = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, accountToVet, altAcctAuthInfo, log));
        assert.deepStrictEqual(results, [true, true]);
        const publicUserResults = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, constants.publicId, null,
                log));
        assert.deepStrictEqual(publicUserResults, [true, true]);
    });
});

describe('object authorization for objectPutACL and objectGetACL', () => {
    // Reset the object ACLs
    afterEach(() => {
        object.acl = {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        };
    });

    const requestTypes = ['objectGet', 'objectHead'];

    it('should allow access to object owner', () => {
        const results = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, objectOwnerCanonicalId,
                authInfo, log));
        assert.deepStrictEqual(results, [true, true]);
    });

    it('should allow access to user in object owner account', () => {
        const results = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, objectOwnerCanonicalId,
                userAuthInfo, log));
        assert.deepStrictEqual(results, [true, true]);
    });

    it('should allow access to bucket owner when object owner is alt account if ' +
        'bucket-owner-full-control canned ACL set on object', () => {
        const altAcctObj = {
            'owner-id': accountToVet,
            'acl': {
                Canned: 'private',
                FULL_CONTROL: [],
                WRITE_ACP: [],
                READ: [],
                READ_ACP: [],
            },
        };
        const noAuthResults = requestTypes.map(type =>
            isObjAuthorized(bucket, altAcctObj, type, bucketOwnerCanonicalId, authInfo,
                log));
        assert.deepStrictEqual(noAuthResults, [false, false]);
        altAcctObj.acl.Canned = 'bucket-owner-full-control';
        const authorizedResults = requestTypes.map(type =>
            isObjAuthorized(bucket, altAcctObj, type, bucketOwnerCanonicalId, authInfo,
                null, log));
        assert.deepStrictEqual(authorizedResults, [true, true]);
    });

    it('should allow access to account if ' +
        'account was granted FULL_CONTROL right', () => {
        const noAuthResults = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, accountToVet, altAcctAuthInfo, log));
        assert.deepStrictEqual(noAuthResults, [false, false]);
        object.acl.FULL_CONTROL = [accountToVet];
        const authorizedResults = requestTypes.map(type =>
            isObjAuthorized(bucket, object, type, accountToVet, altAcctAuthInfo, log));
        assert.deepStrictEqual(authorizedResults, [true, true]);
    });

    it('should allow objectPutACL access to account if ' +
        'account was granted WRITE_ACP right', () => {
        const noAuthResult = isObjAuthorized(bucket, object, 'objectPutACL',
            accountToVet, altAcctAuthInfo, log);
        assert.strictEqual(noAuthResult, false);
        object.acl.WRITE_ACP = [accountToVet];
        const authorizedResult = isObjAuthorized(bucket, object, 'objectPutACL',
            accountToVet, altAcctAuthInfo, log);
        assert.strictEqual(authorizedResult, true);
    });

    it('should allow objectGetACL access to account if ' +
        'account was granted READ_ACP right', () => {
        const noAuthResult = isObjAuthorized(bucket, object, 'objectGetACL',
            accountToVet, altAcctAuthInfo, log);
        assert.strictEqual(noAuthResult, false);
        object.acl.READ_ACP = [accountToVet];
        const authorizedResult = isObjAuthorized(bucket, object, 'objectGetACL',
            accountToVet, altAcctAuthInfo, log);
        assert.strictEqual(authorizedResult, true);
    });
});

describe('without object metadata', () => {
    afterEach(() => {
        bucket.setFullAcl({
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        });
        bucket.setBucketPolicy(null);
    });

    const requestTypes = [
        'objectGet',
        'objectHead',
        'objectPutACL',
        'objectGetACL',
    ];

    const allowedAccess = [true, true, true, true];
    const deniedAccess = [false, false, false, false];

    const tests = [
        {
            it: 'should allow bucket owner',
            canned: 'private', id: bucketOwnerCanonicalId,
            aclParam: null,
            response: allowedAccess,
        },
        {
            it: 'should not allow public if canned private',
            canned: 'private', id: constants.publicId,
            aclParam: null,
            response: deniedAccess,
        },
        {
            it: 'should not allow other accounts if canned private',
            canned: 'private', id: accountToVet,
            aclParam: null,
            response: deniedAccess,
        },
        {
            it: 'should allow public if bucket is canned public-read',
            canned: 'public-read', id: constants.publicId,
            aclParam: null,
            response: allowedAccess,
        },
        {
            it: 'should allow public if bucket is canned public-read-write',
            canned: 'public-read-write', id: constants.publicId,
            aclParam: null,
            response: allowedAccess,
        },
        {
            it: 'should not allow public if bucket is canned ' +
            'authenticated-read',
            canned: 'authenticated-read', id: constants.publicId,
            aclParam: null,
            response: deniedAccess,
        },
        {
            it: 'should allow authenticated users if bucket is canned ' +
            'authenticated-read',
            canned: 'authenticated-read', id: accountToVet,
            aclParam: null,
            response: allowedAccess,
        },
        {
            it: 'should allow account if granted bucket READ',
            canned: '', id: accountToVet,
            aclParam: ['READ', accountToVet],
            response: allowedAccess,
        },
        {
            it: 'should allow account if granted bucket FULL_CONTROL',
            canned: '', id: accountToVet,
            aclParam: ['FULL_CONTROL', accountToVet],
            response: allowedAccess,
        },
        {
            it: 'should allow public if granted bucket read action in policy',
            canned: 'private', id: constants.publicId,
            aclParam: null,
            policy: {
                Version: '2012-10-17',
                Statement: [
                    {
                        Effect: 'Allow',
                        Resource: 'arn:aws:s3:::niftybucket',
                        Principal: '*',
                        Action: ['s3:ListBucket'],
                    },
                ],
            },
            response: allowedAccess,
        },
        {
            it: 'should not allow public if denied bucket read action in policy',
            canned: 'public-read', id: constants.publicId,
            aclParam: null,
            policy: {
                Version: '2012-10-17',
                Statement: [
                    {
                        Effect: 'Deny',
                        Resource: 'arn:aws:s3:::niftybucket',
                        Principal: '*',
                        Action: ['s3:ListBucket'],
                    },
                ],
            },
            response: deniedAccess,
        },
        {
            it: 'should allow account if granted bucket read action in policy',
            canned: 'private', id: accountToVet,
            aclParam: null,
            policy: {
                Version: '2012-10-17',
                Statement: [
                    {
                        Effect: 'Allow',
                        Resource: 'arn:aws:s3:::niftybucket',
                        Principal: { AWS: [altAcctAuthInfo.getArn()] },
                        Action: ['s3:ListBucket'],
                    },
                ],
            },
            response: allowedAccess,
            authInfo: altAcctAuthInfo,
        },
        {
            it: 'should not allow account if denied bucket read action in policy',
            canned: 'public-read', id: accountToVet,
            aclParam: null,
            policy: {
                Version: '2012-10-17',
                Statement: [
                    {
                        Effect: 'Deny',
                        Resource: 'arn:aws:s3:::niftybucket',
                        Principal: { CanonicalUser: [altAcctAuthInfo.getCanonicalID()] },
                        Action: ['s3:ListBucket'],
                    },
                ],
            },
            response: deniedAccess,
            authInfo: altAcctAuthInfo,
        },
    ];

    tests.forEach(value => {
        it(value.it, done => {
            const authInfoUser = value.authInfo ? value.authInfo : authInfo;

            if (value.aclParam) {
                bucket.setSpecificAcl(value.aclParam[1], value.aclParam[0]);
            }

            if (value.policy) {
                bucket.setBucketPolicy(value.policy);
            }

            bucket.setCannedAcl(value.canned);
            const results = requestTypes.map(type =>
                isObjAuthorized(bucket, null, type, value.id, authInfoUser, log));
            assert.deepStrictEqual(results, value.response);
            done();
        });
    });
});
