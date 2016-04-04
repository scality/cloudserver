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

    it('should allow access to bucket owner', () => {
        const results = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                ownerCanonicalId);
        });
        assert.deepStrictEqual(results, [true, true, true, true]);
    });

    it('should allow access to anyone if canned public-read ACL', () => {
        bucket.acl.Canned = 'public-read';
        const results = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                accountToVet);
        });
        assert.deepStrictEqual(results, [true, true, true, true]);
    });

    it('should allow access to anyone if canned public-read-write ACL', () => {
        bucket.acl.Canned = 'public-read-write';
        const results = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                accountToVet);
        });
        assert.deepStrictEqual(results, [true, true, true, true]);
    });

    it('should not allow access to public user if ' +
        'authenticated-read ACL', () => {
        bucket.acl.Canned = 'authenticated-read';
        const results = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                constants.publicId);
        });
        assert.deepStrictEqual(results, [false, false, false, false]);
    });

    it('should allow access to any authenticated user if ' +
        'authenticated-read ACL', () => {
        bucket.acl.Canned = 'authenticated-read';
        const results = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                accountToVet);
        });
        assert.deepStrictEqual(results, [true, true, true, true]);
    });

    it('should allow access to user if ' +
        'account was granted FULL_CONTROL', () => {
        bucket.acl.FULL_CONTROL = [accountToVet];
        const results = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                accountToVet);
        });
        assert.deepStrictEqual(results, [true, true, true, true]);
    });

    it('should allow access to user if ' +
        'account was granted READ right', () => {
        bucket.acl.READ = [accountToVet];
        const results = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                accountToVet);
        });
        assert.deepStrictEqual(results, [true, true, true, true]);
    });

    it('should not allow access to public user if private canned ACL', () => {
        const results = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                accountToVet);
        });
        assert.deepStrictEqual(results, [false, false, false, false]);
    });

    it('should not allow access to just any user if private canned ACL', () => {
        const results = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                accountToVet);
        });
        assert.deepStrictEqual(results, [false, false, false, false]);
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

    it('should allow access to log group only if ' +
        'canned log-delivery-write acl', () => {
        const noAuthResult = isBucketAuthorized(bucket, 'bucketGetACL',
            constants.logId);
        assert.strictEqual(noAuthResult, false);
        bucket.acl.Canned = 'log-delivery-write';
        const authorizedResult = isBucketAuthorized(bucket, 'bucketGetACL',
            constants.logId);
        assert.strictEqual(authorizedResult, true);
    });

    it('should allow access to account only if ' +
        'account was granted FULL_CONTROL right', () => {
        const noAuthResult = isBucketAuthorized(bucket, 'bucketGetACL',
            accountToVet);
        assert.strictEqual(noAuthResult, false);
        bucket.acl.FULL_CONTROL = [accountToVet];
        const authorizedResult = isBucketAuthorized(bucket, 'bucketGetACL',
            accountToVet);
        assert.strictEqual(authorizedResult, true);
    });

    it('should allow access to account only if ' +
        'account was granted READ_ACP right', () => {
        const noAuthResult = isBucketAuthorized(bucket, 'bucketGetACL',
            accountToVet);
        assert.strictEqual(noAuthResult, false);
        bucket.acl.READ_ACP = [accountToVet];
        const authorizedResult = isBucketAuthorized(bucket, 'bucketGetACL',
            accountToVet);
        assert.strictEqual(authorizedResult, true);
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

    it('should allow access to account if ' +
        'account was granted FULL_CONTROL right', () => {
        const noAuthResult = isBucketAuthorized(bucket, 'bucketPutACL',
            accountToVet);
        assert.strictEqual(noAuthResult, false);
        bucket.acl.FULL_CONTROL = [accountToVet];
        const authorizedResult = isBucketAuthorized(bucket, 'bucketPutACL',
            accountToVet);
        assert.strictEqual(authorizedResult, true);
    });

    it('should allow access to account if ' +
        'account was granted WRITE_ACP right', () => {
        const noAuthResult = isBucketAuthorized(bucket, 'bucketPutACL',
            accountToVet);
        assert.strictEqual(noAuthResult, false);
        bucket.acl.WRITE_ACP = [accountToVet];
        const authorizedResult = isBucketAuthorized(bucket, 'bucketPutACL',
            accountToVet);
        assert.strictEqual(authorizedResult, true);
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

    it('should not allow access to other account (even if ' +
        'other account has FULL_CONTROL rights in bucket)', () => {
        bucket.acl.FULL_CONTROL = [accountToVet];
        const result = isBucketAuthorized(bucket, 'bucketDelete',
            accountToVet);
        assert.strictEqual(result, false);
    });

    it('should not allow access to public user (even if ' +
        'bucket is public read write)', () => {
        bucket.acl.Canned = 'public-read-write';
        const result = isBucketAuthorized(bucket, 'bucketDelete',
            constants.publicId);
        assert.strictEqual(result, false);
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
        const results = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                ownerCanonicalId);
        });
        assert.deepStrictEqual(results, [true, true]);
    });

    it('should allow access to anyone if canned public-read-write ACL', () => {
        bucket.acl.Canned = 'public-read-write';
        const publicUserResults = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                constants.publicId);
        });
        assert.deepStrictEqual(publicUserResults, [true, true]);
        const anyAccountResults = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                accountToVet);
        });
        assert.deepStrictEqual(anyAccountResults, [true, true]);
    });

    it('should allow access to user if ' +
        'account was granted FULL_CONTROL', () => {
        const noAuthResults = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                accountToVet);
        });
        assert.deepStrictEqual(noAuthResults, [false, false]);
        bucket.acl.FULL_CONTROL = [accountToVet];
        const authResults = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                accountToVet);
        });
        assert.deepStrictEqual(authResults, [true, true]);
    });

    it('should allow access to user if ' +
        'account was granted WRITE right', () => {
        const noAuthResults = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                accountToVet);
        });
        assert.deepStrictEqual(noAuthResults, [false, false]);
        bucket.acl.WRITE = [accountToVet];
        const authResults = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                accountToVet);
        });
        assert.deepStrictEqual(authResults, [true, true]);
    });
});

describe('bucket authorization for objectPutACL and objectGetACL', () => {
    it('should allow access to anyone since checks ' +
        'are done at object level', () => {
        const requestTypes = ['objectPutACL', 'objectGetACL'];
        const results = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                accountToVet);
        });
        assert.deepStrictEqual(results, [true, true]);
        const publicUserResults = requestTypes.map((type) => {
            return isBucketAuthorized(bucket, type,
                constants.publicId);
        });
        assert.deepStrictEqual(publicUserResults, [true, true]);
    });
});
