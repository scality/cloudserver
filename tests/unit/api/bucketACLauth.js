import assert from 'assert';
import BucketInfo from '../../../lib/metadata/BucketInfo';
import constants from '../../../constants';
import { isBucketAuthorized } from
    '../../../lib/api/apiUtils/authorization/aclChecks';

const ownerCanonicalId = 'ownerCanonicalId';
const creationDate = new Date().toJSON();
const bucket = new BucketInfo('niftyBucket', ownerCanonicalId,
    'iAmTheOwnerDisplayName', creationDate);
const accountToVet = 'accountToVetId';

describe('bucket authorization for bucketGet, bucketHead, ' +
    'objectGet, and objectHead', () => {
    // Reset the bucket ACLs
    afterEach(() => {
        bucket.acl = {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        };
    });
    const requestTypes = ['bucketGet', 'bucketHead', 'objectGet', 'objectHead'];

    const trueArray = [true, true, true, true];
    const falseArray = [false, false, false, false];

    const orders = [
        { it: 'should allow access to bucket owner', canned: '',
          id: ownerCanonicalId, response: trueArray },
        { it: 'should allow access to anyone if canned public-read ACL',
          canned: 'public-read', id: accountToVet, response: trueArray },
        { it: 'should allow access to anyone if canned public-read-write ACL',
          canned: 'public-read-write', id: accountToVet, response: trueArray },
        { it: 'should not allow access to public user if authenticated-read ' +
          'ACL', canned: 'authenticated-read', id: constants.publicId,
          response: falseArray },
        { it: 'should allow access to any authenticated user if authenticated' +
          '-read ACL', canned: 'authenticated-read', id: accountToVet,
          response: trueArray },
        { it: 'should not allow access to public user if private canned ACL',
          canned: '', id: accountToVet, response: falseArray },
        { it: 'should not allow access to just any user if private canned ACL',
          canned: '', id: accountToVet, response: falseArray },
        { it: 'should allow access to user if account was granted FULL_CONTROL',
          canned: '', id: accountToVet, response: trueArray,
          aclParam: ['FULL_CONTROL', [accountToVet]] },
        { it: 'should not allow access to just any user if private canned ACL',
          canned: '', id: accountToVet, response: trueArray,
          aclParam: ['READ', [accountToVet]] },
    ];

    orders.forEach(value => {
        it(value.it, done => {
            if (value.aclParam) {
                bucket.acl[value.aclParam[0]] = value.aclParam[1];
            }
            bucket.acl.Canned = value.canned;
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
        bucket.acl = {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        };
    });

    it('should allow access to bucket owner', () => {
        const result = isBucketAuthorized(bucket, 'bucketGetACL',
            ownerCanonicalId);
        assert.strictEqual(result, true);
    });

    const orders = [
        { it: 'log group only if canned log-delivery-write acl',
          id: constants.logId, aclParam: ['Canned', 'log-delivery-write'] },
        { it: 'account only if account was granted FULL_CONTROL right',
          id: accountToVet, aclParam: ['FULL_CONTROL', [accountToVet]] },
        { it: 'account only if account was granted READ_ACP right',
          id: accountToVet, aclParam: ['READ_ACP', [accountToVet]] },
    ];
    orders.forEach(value => {
        it(`should allow access to ${value.it}`, done => {
            const noAuthResult = isBucketAuthorized(bucket, 'bucketGetACL',
                                                    value.id);
            assert.strictEqual(noAuthResult, false);
            bucket.acl[value.aclParam[0]] = value.aclParam[1];
            const authorizedResult = isBucketAuthorized(bucket, 'bucketGetACL',
                                                        value.id);
            assert.strictEqual(authorizedResult, true);
            done();
        });
    });
});

describe('bucket authorization for bucketPutACL', () => {
    // Reset the bucket ACLs
    afterEach(() => {
        bucket.acl = {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        };
    });

    it('should allow access to bucket owner', () => {
        const result = isBucketAuthorized(bucket, 'bucketPutACL',
            ownerCanonicalId);
        assert.strictEqual(result, true);
    });

    const orders = ['FULL_CONTROL', 'WRITE_ACP'];
    orders.forEach(value => {
        it('should allow access to account if ' +
           `account was granted ${value} right`, done => {
            const noAuthResult = isBucketAuthorized(bucket, 'bucketPutACL',
                accountToVet);
            assert.strictEqual(noAuthResult, false);
            bucket.acl[value] = [accountToVet];
            const authorizedResult = isBucketAuthorized(bucket, 'bucketPutACL',
                accountToVet);
            assert.strictEqual(authorizedResult, true);
            done();
        });
    });
});

describe('bucket authorization for bucketDelete', () => {
    // Reset the bucket ACLs
    afterEach(() => {
        bucket.acl = {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        };
    });

    it('should allow access to bucket owner', () => {
        const result = isBucketAuthorized(bucket, 'bucketDelete',
            ownerCanonicalId);
        assert.strictEqual(result, true);
    });

    const orders = [
        { it: 'other account (even if other account has FULL_CONTROL rights ' +
          'in bucket)', id: accountToVet, aclParam: ['FULL_CONTROL',
                                                     [accountToVet]] },
        { it: 'public user (even if bucket is public read write)',
          id: constants.publicId, aclParam: ['Canned', 'public-read-write'] },
    ];
    orders.forEach(value => {
        it(`should not allow access to ${value.it}`, done => {
            bucket.acl[value.aclParam[0]] = value.aclParam[1];
            const result = isBucketAuthorized(bucket, 'bucketDelete', value.id);
            assert.strictEqual(result, false);
            done();
        });
    });
});

describe('bucket authorization for objectDelete and objectPut', () => {
    // Reset the bucket ACLs
    afterEach(() => {
        bucket.acl = {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        };
    });
    const requestTypes = ['objectDelete', 'objectPut'];

    it('should allow access to bucket owner', () => {
        const results = requestTypes.map(type =>
            isBucketAuthorized(bucket, type, ownerCanonicalId));
        assert.deepStrictEqual(results, [true, true]);
    });

    const orders = [
        { it: 'anyone if canned public-read-write ACL',
          canned: 'public-read-write', id: constants.publicId,
          response: [true, true], aclParam: ['WRITE', []] },
        { it: 'user if account was granted FULL_CONTROL', canned: '',
          id: accountToVet, response: [false, false],
          aclParam: ['FULL_CONTROL', [accountToVet]] },
        { it: 'user if account was granted WRITE right', canned: '',
          id: accountToVet, response: [false, false],
          aclParam: ['WRITE', [accountToVet]] },
    ];
    orders.forEach(value => {
        it(`should allow access to ${value.it}`, done => {
            bucket.acl.Canned = value.canned;
            const noAuthResults = requestTypes.map(type =>
                isBucketAuthorized(bucket, type, value.id));
            assert.deepStrictEqual(noAuthResults, value.response);
            bucket.acl[value.aclParam[0]] = value.aclParam[1];
            const authResults = requestTypes.map(type =>
                isBucketAuthorized(bucket, type, accountToVet));
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
            isBucketAuthorized(bucket, type, accountToVet));
        assert.deepStrictEqual(results, [true, true]);
        const publicUserResults = requestTypes.map(type =>
            isBucketAuthorized(bucket, type, constants.publicId));
        assert.deepStrictEqual(publicUserResults, [true, true]);
        done();
    });
});
