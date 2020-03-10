const assert = require('assert');
const BucketInfo = require('arsenal').models.BucketInfo;
const constants = require('../../../constants');
const { isBucketAuthorized }
    = require('../../../lib/api/apiUtils/authorization/permissionChecks');
const { DummyRequestLogger, makeAuthInfo } = require('../helpers');

const creationDate = new Date().toJSON();
const accessKey = 'accessKey1';
const altAccessKey = 'accessKey2';
const authInfo = makeAuthInfo(accessKey);
const userAuthInfo = makeAuthInfo(accessKey, 'user');
const ownerCanonicalId = authInfo.getCanonicalID();
const altAcctAuthInfo = makeAuthInfo(altAccessKey);
const accountToVet = altAcctAuthInfo.getCanonicalID();

const bucket = new BucketInfo('niftyBucket', ownerCanonicalId,
    authInfo.getAccountDisplayName(), creationDate);
const log = new DummyRequestLogger();

describe('bucket authorization for bucketGet, bucketHead, ' +
    'objectGet, and objectHead', () => {
    // Reset the bucket ACLs
    afterEach(() => {
        bucket.setFullAcl({
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        });
    });
    const requestTypes = ['bucketGet', 'bucketHead', 'objectGet', 'objectHead'];

    const trueArray = [true, true, true, true];
    // An account can have the ability to do objectHead or objectGet even
    // if the account has no rights to the bucket holding the object.
    // So isBucketAuthorized should return true for 'objectGet' and 'objectHead'
    // requests but false for 'bucketGet' and 'bucketHead'
    const falseArrayBucketTrueArrayObject = [false, false, true, true];

    const orders = [
        {
            it: 'should allow access to bucket owner', canned: '',
            id: ownerCanonicalId, response: trueArray, auth: authInfo,
        },
        {
            it: 'should allow access to user in bucket owner account', canned: '',
            id: ownerCanonicalId, response: trueArray, auth: userAuthInfo,
        },
        {
            it: 'should allow access to anyone if canned public-read ACL',
            canned: 'public-read', id: accountToVet, response: trueArray,
            auth: altAcctAuthInfo,
        },
        {
            it: 'should allow access to anyone if canned public-read-write ACL',
            canned: 'public-read-write', id: accountToVet, response: trueArray,
            auth: altAcctAuthInfo,
        },
        {
            it: 'should not allow request on the bucket (bucketGet, bucketHead)'
            + ' but should allow request on the object (objectGet, objectHead)'
            + ' to public user if authenticated-read  ACL',
            canned: 'authenticated-read', id: constants.publicId,
            response: falseArrayBucketTrueArrayObject, auth: altAcctAuthInfo,
        },
        {
            it: 'should allow access to any authenticated user if authenticated'
            + '-read ACL', canned: 'authenticated-read', id: accountToVet,
            response: trueArray, auth: altAcctAuthInfo,
        },
        {
            it: 'should not allow request on the bucket (bucketGet, bucketHead)'
            + ' but should allow request on the object (objectGet, objectHead)'
            + ' to public user if private canned ACL',
            canned: '', id: accountToVet,
            response: falseArrayBucketTrueArrayObject, auth: altAcctAuthInfo,
        },
        {
            it: 'should not allow request on the bucket (bucketGet, bucketHead)'
            + ' but should allow request on the object (objectGet, objectHead)'
            + ' to just any user if private canned ACL',
            canned: '', id: accountToVet,
            response: falseArrayBucketTrueArrayObject, auth: altAcctAuthInfo,
        },
        {
            it: 'should allow access to user if account was granted'
            + ' FULL_CONTROL',
            canned: '', id: accountToVet, response: trueArray,
            aclParam: ['FULL_CONTROL', accountToVet], auth: altAcctAuthInfo,
        },
        {
            it: 'should not allow access to just any user if private'
            + ' canned ACL',
            canned: '', id: accountToVet, response: trueArray,
            aclParam: ['READ', accountToVet], auth: altAcctAuthInfo,
        },
    ];

    orders.forEach(value => {
        it(value.it, done => {
            if (value.aclParam) {
                bucket.setSpecificAcl(value.aclParam[1], value.aclParam[0]);
            }
            bucket.setCannedAcl(value.canned);
            const results = requestTypes.map(type =>
                isBucketAuthorized(bucket, type, value.id, value.auth, log));
            assert.deepStrictEqual(results, value.response);
            done();
        });
    });
});

describe('bucket authorization for bucketGetACL', () => {
    // Reset the bucket ACLs
    afterEach(() => {
        bucket.setFullAcl({
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        });
    });

    it('should allow access to bucket owner', () => {
        const result = isBucketAuthorized(bucket, 'bucketGetACL',
            ownerCanonicalId, authInfo);
        assert.strictEqual(result, true);
    });

    it('should allow access to user in bucket owner account', () => {
        const result = isBucketAuthorized(bucket, 'bucketGetACL',
            ownerCanonicalId, userAuthInfo);
        assert.strictEqual(result, true);
    });

    const orders = [
        {
            it: 'log group only if canned log-delivery-write acl',
            id: constants.logId, canned: 'log-delivery-write', auth: null,
        },
        {
            it: 'account only if account was granted FULL_CONTROL right',
            id: accountToVet, aclParam: ['FULL_CONTROL', accountToVet],
            auth: altAcctAuthInfo,
        },
        {
            it: 'account only if account was granted READ_ACP right',
            id: accountToVet, aclParam: ['READ_ACP', accountToVet],
            auth: altAcctAuthInfo,
        },
    ];
    orders.forEach(value => {
        it(`should allow access to ${value.it}`, done => {
            const noAuthResult = isBucketAuthorized(bucket, 'bucketGetACL',
                                                    value.id);
            assert.strictEqual(noAuthResult, false);
            if (value.aclParam) {
                bucket.setSpecificAcl(value.aclParam[1], value.aclParam[0]);
            } else if (value.canned) {
                bucket.setCannedAcl(value.canned);
            }
            const authorizedResult = isBucketAuthorized(bucket, 'bucketGetACL',
                value.id, value.auth);
            assert.strictEqual(authorizedResult, true);
            done();
        });
    });
});

describe('bucket authorization for bucketPutACL', () => {
    // Reset the bucket ACLs
    afterEach(() => {
        bucket.setFullAcl({
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        });
    });

    it('should allow access to bucket owner', () => {
        const result = isBucketAuthorized(bucket, 'bucketPutACL',
            ownerCanonicalId, authInfo);
        assert.strictEqual(result, true);
    });

    it('should allow access to user in bucket owner account', () => {
        const result = isBucketAuthorized(bucket, 'bucketPutACL',
            ownerCanonicalId, userAuthInfo);
        assert.strictEqual(result, true);
    });

    const orders = ['FULL_CONTROL', 'WRITE_ACP'];
    orders.forEach(value => {
        it('should allow access to account if ' +
           `account was granted ${value} right`, done => {
            const noAuthResult = isBucketAuthorized(bucket, 'bucketPutACL',
                accountToVet, altAcctAuthInfo);
            assert.strictEqual(noAuthResult, false);
            bucket.setSpecificAcl(accountToVet, value);
            const authorizedResult = isBucketAuthorized(bucket, 'bucketPutACL',
                accountToVet, altAcctAuthInfo);
            assert.strictEqual(authorizedResult, true);
            done();
        });
    });
});

describe('bucket authorization for bucketOwnerAction', () => {
    // Reset the bucket ACLs
    afterEach(() => {
        bucket.setFullAcl({
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        });
    });

    it('should allow access to bucket owner', () => {
        const result = isBucketAuthorized(bucket, 'bucketDeleteCors',
            ownerCanonicalId, authInfo);
        assert.strictEqual(result, true);
    });

    it('should allow access to user in bucket owner account', () => {
        const result = isBucketAuthorized(bucket, 'bucketDeleteCors',
            ownerCanonicalId, userAuthInfo);
        assert.strictEqual(result, true);
    });

    const orders = [
        {
            it: 'other account (even if other account has FULL_CONTROL rights'
            + ' in bucket)', id: accountToVet, canned: '',
            aclParam: ['FULL_CONTROL', accountToVet], auth: altAcctAuthInfo,
        },
        {
            it: 'public user (even if bucket is public read write)',
            id: constants.publicId, canned: 'public-read-write', auth: altAcctAuthInfo,
        },
    ];
    orders.forEach(value => {
        it(`should not allow access to ${value.it}`, done => {
            if (value.aclParam) {
                bucket.setSpecificAcl(value.aclParam[1], value.aclParam[0]);
            }
            bucket.setCannedAcl(value.canned);
            const result = isBucketAuthorized(bucket, 'bucketDeleteCors',
                value.id, value.auth);
            assert.strictEqual(result, false);
            done();
        });
    });
});

describe('bucket authorization for bucketDelete', () => {
    // Reset the bucket ACLs
    afterEach(() => {
        bucket.setFullAcl({
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        });
    });

    it('should allow access to bucket owner', () => {
        const result = isBucketAuthorized(bucket, 'bucketDelete',
            ownerCanonicalId, authInfo);
        assert.strictEqual(result, true);
    });

    it('should allow access to user in bucket owner account', () => {
        const result = isBucketAuthorized(bucket, 'bucketDelete',
            ownerCanonicalId, userAuthInfo);
        assert.strictEqual(result, true);
    });

    const orders = [
        {
            it: 'other account (even if other account has FULL_CONTROL rights '
            + 'in bucket)', id: accountToVet, canned: '',
            aclParam: ['FULL_CONTROL', accountToVet], auth: altAcctAuthInfo,
        },
        {
            it: 'public user (even if bucket is public read write)',
            id: constants.publicId, canned: 'public-read-write', auth: null,
        },
    ];
    orders.forEach(value => {
        it(`should not allow access to ${value.it}`, done => {
            if (value.aclParam) {
                bucket.setSpecificAcl(value.aclParam[1], value.aclParam[0]);
            }
            bucket.setCannedAcl(value.canned);
            const result = isBucketAuthorized(bucket, 'bucketDelete', value.id, value.auth);
            assert.strictEqual(result, false);
            done();
        });
    });
});

describe('bucket authorization for objectDelete and objectPut', () => {
    // Reset the bucket ACLs
    afterEach(() => {
        bucket.setFullAcl({
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        });
    });

    const requestTypes = ['objectDelete', 'objectPut'];

    it('should allow access to bucket owner', () => {
        const results = requestTypes.map(type =>
            isBucketAuthorized(bucket, type, ownerCanonicalId, authInfo));
        assert.deepStrictEqual(results, [true, true]);
    });

    it('should allow access to user in bucket owner account', () => {
        const results = requestTypes.map(type =>
            isBucketAuthorized(bucket, type, ownerCanonicalId, userAuthInfo));
        assert.strictEqual(results, [true, true]);
    });

    const orders = [
        {
            it: 'anyone if canned public-read-write ACL',
            canned: 'public-read-write', id: constants.publicId,
            response: [true, true],
        },
        {
            it: 'user if account was granted FULL_CONTROL', canned: '',
            id: accountToVet, response: [false, false],
            aclParam: ['FULL_CONTROL', accountToVet], auth: altAcctAuthInfo,
        },
        {
            it: 'user if account was granted WRITE right', canned: '',
            id: accountToVet, response: [false, false],
            aclParam: ['WRITE', accountToVet], auth: altAcctAuthInfo,
        },
    ];
    orders.forEach(value => {
        it(`should allow access to ${value.it}`, done => {
            bucket.setCannedAcl(value.canned);
            const noAuthResults = requestTypes.map(type =>
                isBucketAuthorized(bucket, type, value.id, value.auth));
            assert.deepStrictEqual(noAuthResults, value.response);
            if (value.aclParam) {
                bucket.setSpecificAcl(value.aclParam[1], value.aclParam[0]);
            }
            const authResults = requestTypes.map(type =>
                isBucketAuthorized(bucket, type, accountToVet, altAcctAuthInfo));
            assert.deepStrictEqual(authResults, [true, true]);
            done();
        });
    });
});

describe('bucket authorization for objectPutACL and objectGetACL', () => {
    it('should allow access to anyone since checks ' +
        'are done at object level', done => {
        const requestTypes = ['objectPutACL', 'objectGetACL'];
        const results = requestTypes.map(type =>
            isBucketAuthorized(bucket, type, accountToVet, altAcctAuthInfo));
        assert.deepStrictEqual(results, [true, true]);
        const publicUserResults = requestTypes.map(type =>
            isBucketAuthorized(bucket, type, constants.publicId));
        assert.deepStrictEqual(publicUserResults, [true, true]);
        done();
    });
});
