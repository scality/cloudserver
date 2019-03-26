const assert = require('assert');
const BucketInfo = require('arsenal').models.BucketInfo;
const constants = require('../../../constants');
const { isBucketAuthorized }
    = require('../../../lib/api/apiUtils/authorization/aclChecks');

const ownerCanonicalId = 'ownerCanonicalId';
const lifecycleServiceAccountId = '0123456789abcdef/lifecycle';
const unknownServiceAccountId = '0123456789abcdef/unknown';
const creationDate = new Date().toJSON();
const bucket = new BucketInfo('niftyBucket', ownerCanonicalId,
    'iAmTheOwnerDisplayName', creationDate);
const accountToVet = 'accountToVetId';

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
            id: ownerCanonicalId, response: trueArray,
        },
        {
            it: 'should allow access to lifecycle service account',
            canned: '', id: lifecycleServiceAccountId, response: trueArray,
        },
        {
            it: 'should allow public-user access for unknown ' +
                'service account and private canned ACL',
            canned: '', id: unknownServiceAccountId,
            response: falseArrayBucketTrueArrayObject,
        },
        {
            it: 'should allow access to anyone if canned public-read ACL',
            canned: 'public-read', id: accountToVet, response: trueArray,
        },
        {
            it: 'should allow access to anyone if canned public-read-write ACL',
            canned: 'public-read-write', id: accountToVet, response: trueArray,
        },
        {
            it: 'should not allow request on the bucket (bucketGet, bucketHead)'
            + ' but should allow request on the object (objectGet, objectHead)'
            + ' to public user if authenticated-read  ACL',
            canned: 'authenticated-read', id: constants.publicId,
            response: falseArrayBucketTrueArrayObject,
        },
        {
            it: 'should allow access to any authenticated user if authenticated'
            + '-read ACL', canned: 'authenticated-read', id: accountToVet,
            response: trueArray,
        },
        {
            it: 'should not allow request on the bucket (bucketGet, bucketHead)'
            + ' but should allow request on the object (objectGet, objectHead)'
            + ' to public user if private canned ACL',
            canned: '', id: accountToVet,
            response: falseArrayBucketTrueArrayObject,
        },
        {
            it: 'should not allow request on the bucket (bucketGet, bucketHead)'
            + ' but should allow request on the object (objectGet, objectHead)'
            + ' to just any user if private canned ACL',
            canned: '', id: accountToVet,
            response: falseArrayBucketTrueArrayObject,
        },
        {
            it: 'should allow access to user if account was granted'
            + ' FULL_CONTROL',
            canned: '', id: accountToVet, response: trueArray,
            aclParam: ['FULL_CONTROL', accountToVet],
        },
        {
            it: 'should not allow access to just any user if private'
            + ' canned ACL',
            canned: '', id: accountToVet, response: trueArray,
            aclParam: ['READ', accountToVet],
        },
    ];

    orders.forEach(value => {
        test(value.it, done => {
            if (value.aclParam) {
                bucket.setSpecificAcl(value.aclParam[1], value.aclParam[0]);
            }
            bucket.setCannedAcl(value.canned);
            const results = requestTypes.map(type =>
                isBucketAuthorized(bucket, type, value.id));
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

    test('should allow access to bucket owner', () => {
        const result = isBucketAuthorized(bucket, 'bucketGetACL',
            ownerCanonicalId);
        expect(result).toBe(true);
    });

    const orders = [
        {
            it: 'log group only if canned log-delivery-write acl',
            id: constants.logId, canned: 'log-delivery-write',
        },
        {
            it: 'account only if account was granted FULL_CONTROL right',
            id: accountToVet, aclParam: ['FULL_CONTROL', accountToVet],
        },
        {
            it: 'account only if account was granted READ_ACP right',
            id: accountToVet, aclParam: ['READ_ACP', accountToVet],
        },
    ];
    orders.forEach(value => {
        test(`should allow access to ${value.it}`, done => {
            const noAuthResult = isBucketAuthorized(bucket, 'bucketGetACL',
                                                    value.id);
            expect(noAuthResult).toBe(false);
            if (value.aclParam) {
                bucket.setSpecificAcl(value.aclParam[1], value.aclParam[0]);
            } else if (value.canned) {
                bucket.setCannedAcl(value.canned);
            }
            const authorizedResult = isBucketAuthorized(bucket, 'bucketGetACL',
                                                        value.id);
            expect(authorizedResult).toBe(true);
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

    test('should allow access to bucket owner', () => {
        const result = isBucketAuthorized(bucket, 'bucketPutACL',
            ownerCanonicalId);
        expect(result).toBe(true);
    });

    const orders = ['FULL_CONTROL', 'WRITE_ACP'];
    orders.forEach(value => {
        test('should allow access to account if ' +
           `account was granted ${value} right`, done => {
            const noAuthResult = isBucketAuthorized(bucket, 'bucketPutACL',
                accountToVet);
            expect(noAuthResult).toBe(false);
            bucket.setSpecificAcl(accountToVet, value);
            const authorizedResult = isBucketAuthorized(bucket, 'bucketPutACL',
                accountToVet);
            expect(authorizedResult).toBe(true);
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

    test('should allow access to bucket owner', () => {
        const result = isBucketAuthorized(bucket, 'bucketOwnerAction',
            ownerCanonicalId);
        expect(result).toBe(true);
    });

    const orders = [
        {
            it: 'other account (even if other account has FULL_CONTROL rights'
            + ' in bucket)', id: accountToVet, canned: '',
            aclParam: ['FULL_CONTROL', accountToVet],
        },
        {
            it: 'public user (even if bucket is public read write)',
            id: constants.publicId, canned: 'public-read-write',
        },
    ];
    orders.forEach(value => {
        test(`should not allow access to ${value.it}`, done => {
            if (value.aclParam) {
                bucket.setSpecificAcl(value.aclParam[1], value.aclParam[0]);
            }
            bucket.setCannedAcl(value.canned);
            const result = isBucketAuthorized(bucket, 'bucketOwnerAction',
                value.id);
            expect(result).toBe(false);
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

    test('should allow access to bucket owner', () => {
        const result = isBucketAuthorized(bucket, 'bucketDelete',
            ownerCanonicalId);
        expect(result).toBe(true);
    });

    const orders = [
        {
            it: 'other account (even if other account has FULL_CONTROL rights '
            + 'in bucket)', id: accountToVet, canned: '',
            aclParam: ['FULL_CONTROL', accountToVet],
        },
        {
            it: 'public user (even if bucket is public read write)',
            id: constants.publicId, canned: 'public-read-write',
        },
    ];
    orders.forEach(value => {
        test(`should not allow access to ${value.it}`, done => {
            if (value.aclParam) {
                bucket.setSpecificAcl(value.aclParam[1], value.aclParam[0]);
            }
            bucket.setCannedAcl(value.canned);
            const result = isBucketAuthorized(bucket, 'bucketDelete', value.id);
            expect(result).toBe(false);
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

    test('should allow access to bucket owner', () => {
        const results = requestTypes.map(type =>
            isBucketAuthorized(bucket, type, ownerCanonicalId));
        assert.deepStrictEqual(results, [true, true]);
    });

    test('should allow access to lifecycle service account', () => {
        // NOTE objectPut is not needed for lifecycle but still
        // allowed, we would want more fine-grained implementation of
        // ACLs for service accounts later.
        const results = requestTypes.map(type =>
            isBucketAuthorized(bucket, type, lifecycleServiceAccountId));
        assert.deepStrictEqual(results, [true, true]);
    });

    test('should deny access to unknown service account', () => {
        const results = requestTypes.map(type =>
            isBucketAuthorized(bucket, type, unknownServiceAccountId));
        assert.deepStrictEqual(results, [false, false]);
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
            aclParam: ['FULL_CONTROL', accountToVet],
        },
        {
            it: 'user if account was granted WRITE right', canned: '',
            id: accountToVet, response: [false, false],
            aclParam: ['WRITE', accountToVet],
        },
    ];
    orders.forEach(value => {
        test(`should allow access to ${value.it}`, done => {
            bucket.setCannedAcl(value.canned);
            const noAuthResults = requestTypes.map(type =>
                isBucketAuthorized(bucket, type, value.id));
            assert.deepStrictEqual(noAuthResults, value.response);
            if (value.aclParam) {
                bucket.setSpecificAcl(value.aclParam[1], value.aclParam[0]);
            }
            const authResults = requestTypes.map(type =>
                isBucketAuthorized(bucket, type, accountToVet));
            assert.deepStrictEqual(authResults, [true, true]);
            done();
        });
    });
});

describe('bucket authorization for objectPutACL and objectGetACL', () => {
    test('should allow access to anyone since checks ' +
        'are done at object level', done => {
        const requestTypes = ['objectPutACL', 'objectGetACL'];
        const results = requestTypes.map(type =>
            isBucketAuthorized(bucket, type, accountToVet));
        assert.deepStrictEqual(results, [true, true]);
        const publicUserResults = requestTypes.map(type =>
            isBucketAuthorized(bucket, type, constants.publicId));
        assert.deepStrictEqual(publicUserResults, [true, true]);
        done();
    });
});
