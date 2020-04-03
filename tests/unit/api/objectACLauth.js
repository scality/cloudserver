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
