const assert = require('assert');
const { BucketInfo, BucketPolicy } = require('arsenal').models;
const constants = require('../../../constants');
const { isBucketAuthorized, isObjAuthorized, validatePolicyResource }
    = require('../../../lib/api/apiUtils/authorization/permissionChecks');
const { DummyRequestLogger, makeAuthInfo } = require('../helpers');

const accessKey = 'accessKey1';
const altAccessKey = 'accessKey2';
const authInfo = makeAuthInfo(accessKey);
const user1AuthInfo = makeAuthInfo(accessKey, 'user');
const user2AuthInfo = makeAuthInfo(accessKey, 'user2');
const altAcctAuthInfo = makeAuthInfo(altAccessKey);
const altAcctUserAuthInfo = makeAuthInfo(altAccessKey, 'user1');
const bucketOwnerCanonicalId = authInfo.getCanonicalID();
const objectOwnerCanonicalId = user1AuthInfo.getCanonicalID();
const altAcctCanonicalId = altAcctAuthInfo.getCanonicalID();
const accountId = authInfo.getShortid();
const altAcctId = altAcctAuthInfo.getShortid();
const creationDate = new Date().toJSON();
const bucket = new BucketInfo('policyBucketAuthTester', bucketOwnerCanonicalId,
    authInfo.getAccountDisplayName(), creationDate);
const object = { 'owner-id': objectOwnerCanonicalId };
const bucAction = 'bucketHead';
const objAction = 'objectPut';
const basePolicyObj = {
    Version: '2012-10-17',
    Statement: {
        Effect: 'Allow',
        Principal: '*',
        Resource: `arn:aws:s3:::${bucket.getName()}`,
        Action: 's3:*',
    },
};
const bucketName = 'matchme';
const log = new DummyRequestLogger();

const authTests = [
    {
        name: 'should allow access if canonical user principal matches non-',
        bucketId: altAcctCanonicalId,
        bucketAuthInfo: altAcctAuthInfo,
        objectId: altAcctCanonicalId,
        objectAuthInfo: altAcctAuthInfo,
        keyToChange: 'Principal',
        bucketValue: { CanonicalUser: [altAcctCanonicalId] },
        objectValue: { CanonicalUser: [altAcctCanonicalId] },
        expected: true,
    },
    {
        name: 'should allow access if user arn principal matches non-',
        bucketId: objectOwnerCanonicalId,
        bucketAuthInfo: user1AuthInfo,
        objectId: objectOwnerCanonicalId,
        objectAuthInfo: user1AuthInfo,
        keyToChange: 'Principal',
        bucketValue: { AWS: user1AuthInfo.getArn() },
        objectValue: { AWS: user1AuthInfo.getArn() },
        expected: true,
    },
    {
        name: 'should allow access if account arn principal matches non-',
        bucketId: bucketOwnerCanonicalId,
        bucketAuthInfo: authInfo,
        objectId: bucketOwnerCanonicalId,
        objectAuthInfo: authInfo,
        keyToChange: 'Principal',
        bucketValue: { AWS: authInfo.getArn() },
        objectValue: { AWS: authInfo.getArn() },
        expected: true,
    },
    {
        name: 'should allow access if account id principal matches non-',
        bucketId: bucketOwnerCanonicalId,
        bucketAuthInfo: authInfo,
        objectId: bucketOwnerCanonicalId,
        objectAuthInfo: authInfo,
        keyToChange: 'Principal',
        bucketValue: { AWS: accountId },
        objectValue: { AWS: accountId },
        expected: true,
    },
    {
        name: 'should allow access if account id principal is contained in ' +
            'user arn of non-',
        bucketId: objectOwnerCanonicalId,
        bucketAuthInfo: user1AuthInfo,
        objectId: objectOwnerCanonicalId,
        objectAuthInfo: user1AuthInfo,
        keyToChange: 'Principal',
        bucketValue: { AWS: accountId },
        objectValue: { AWS: accountId },
        expected: true,
    },
    {
        name: 'should allow access if account id principal is contained in ' +
            'account arn of non-',
        bucketId: altAcctCanonicalId,
        bucketAuthInfo: altAcctAuthInfo,
        objectId: altAcctCanonicalId,
        objectAuthInfo: altAcctAuthInfo,
        keyToChange: 'Principal',
        bucketValue: { AWS: altAcctId },
        objectValue: { AWS: altAcctId },
        expected: true,
    },
    {
        name: 'should allow access if account arn principal is contained in ' +
            'user arn of non-',
        bucketId: objectOwnerCanonicalId,
        bucketAuthInfo: user1AuthInfo,
        objectId: objectOwnerCanonicalId,
        objectAuthInfo: user1AuthInfo,
        keyToChange: 'Principal',
        bucketValue: { AWS: authInfo.getArn() },
        objectValue: { AWS: authInfo.getArn() },
        expected: true,
    },
    {
        name: 'should allow access even if user arn principal doesn\'t match ' +
            'user arn of user in account of ',
        bucketId: objectOwnerCanonicalId,
        bucketAuthInfo: user1AuthInfo,
        objectId: objectOwnerCanonicalId,
        objectAuthInfo: user1AuthInfo,
        keyToChange: 'Principal',
        bucketValue: { AWS: altAcctUserAuthInfo.getArn() },
        objectValue: { AWS: altAcctUserAuthInfo.getArn() },
        expected: true,
    },
    {
        name: 'should deny access if account arn principal doesn\'t match ' +
            'user arn of non-',
        bucketId: altAcctCanonicalId,
        bucketAuthInfo: altAcctUserAuthInfo,
        objectId: altAcctCanonicalId,
        objectAuthInfo: altAcctUserAuthInfo,
        keyToChange: 'Principal',
        bucketValue: { AWS: authInfo.getArn() },
        objectValue: { AWS: authInfo.getArn() },
        expected: false,
    },
    {
        name: 'should deny access if user arn principal doesn\'t match ' +
            'user arn of non-',
        bucketId: altAcctCanonicalId,
        bucketAuthInfo: altAcctUserAuthInfo,
        objectId: altAcctCanonicalId,
        objectAuthInfo: altAcctUserAuthInfo,
        keyToChange: 'Principal',
        bucketValue: { AWS: user2AuthInfo.getArn() },
        objectValue: { AWS: user2AuthInfo.getArn() },
        expected: false,
    },
    {
        name: 'should deny access if principal doesn\'t match non-',
        bucketId: altAcctAuthInfo,
        bucketAuthInfo: altAcctAuthInfo,
        objectId: altAcctAuthInfo,
        objectAuthInfo: altAcctAuthInfo,
        keyToChange: 'Principal',
        bucketValue: { CanonicalUser: [bucketOwnerCanonicalId] },
        objectValue: { CanonicalUser: [objectOwnerCanonicalId] },
        expected: false,
    },
    {
        name: 'should allow access if principal and action match policy for ' +
            'non-',
        bucketId: altAcctCanonicalId,
        bucketAuthInfo: altAcctAuthInfo,
        objectId: altAcctCanonicalId,
        objectAuthInfo: altAcctAuthInfo,
        keyToChange: 'Action',
        bucketValue: ['s3:ListBucket'],
        objectValue: ['s3:PutObject'],
        expected: true,
    },
    {
        name: 'should deny access if principal matches but action does not ' +
            'match policy for non-',
        bucketId: altAcctCanonicalId,
        bucketAuthInfo: altAcctAuthInfo,
        objectId: altAcctCanonicalId,
        objectAuthInfo: altAcctAuthInfo,
        keyToChange: 'Action',
        bucketValue: ['s3:GetBucketLocation'],
        objectValue: ['s3:GetObject'],
        expected: false,
    },
    {
        name: 'should allow access even if bucket policy denies for ',
        bucketId: bucketOwnerCanonicalId,
        bucketAuthInfo: authInfo,
        objectId: bucketOwnerCanonicalId,
        objectAuthInfo: authInfo,
        keyToChange: 'Effect',
        bucketValue: 'Deny',
        objectValue: 'Deny',
        expected: true,
    },
    {
        name: 'should deny access even for users in account of ',
        bucketId: objectOwnerCanonicalId,
        bucketAuthInfo: user1AuthInfo,
        objectId: objectOwnerCanonicalId,
        objectAuthInfo: user1AuthInfo,
        keyToChange: 'Effect',
        bucketValue: 'Deny',
        objectValue: 'Deny',
        expected: false,
    },
];

const resourceTests = [
    {
        name: 'true if policy resource matches bucket arn',
        rValue: `arn:aws:s3:::${bucketName}`,
        expected: true,
    },
    {
        name: 'true if policy resource matches obj in bucket',
        rValue: `arn:aws:s3:::${bucketName}/*`,
        expected: true,
    },
    {
        name: 'false if policy resource is bucket name',
        rValue: bucketName,
        expected: false,
    },
    {
        name: 'false if policy resource does not match bucket arn',
        rValue: 'arn:aws:s3:::nomatch',
        expected: false,
    },
    {
        name: 'false if policy resource is array and any elements do not ' +
            'match bucket arn',
        rValue: [`arn:aws:s3:::${bucketName}`, 'arn:aws:s3:::nomatch'],
        expected: false,
    },
];

describe('bucket policy authorization', () => {
    describe('isBucketAuthorized with no policy set', () => {
        it('should allow access to bucket owner', done => {
            const allowed = isBucketAuthorized(bucket, 'bucketPut',
                bucketOwnerCanonicalId, null, log);
            assert.equal(allowed, true);
            done();
        });

        it('should deny access to non-bucket owner',
        done => {
            const allowed = isBucketAuthorized(bucket, 'bucketPut',
                altAcctCanonicalId, null, log);
            assert.equal(allowed, false);
            done();
        });
    });

    describe('isBucketAuthorized with bucket policy set', () => {
        beforeEach(function beFn() {
            this.currentTest.basePolicy = new BucketPolicy(JSON.stringify(
                basePolicyObj)).getBucketPolicy();
            bucket.setBucketPolicy(this.currentTest.basePolicy);
        });

        it('should allow access to non-bucket owner if principal is set to "*"',
        done => {
            const allowed = isBucketAuthorized(bucket, bucAction,
                altAcctCanonicalId, null, log);
            assert.equal(allowed, true);
            done();
        });

        it('should allow access to public user if principal is set to "*"',
        done => {
            const allowed = isBucketAuthorized(bucket, bucAction,
                constants.publicId, null, log);
            assert.equal(allowed, true);
            done();
        });

        authTests.forEach(t => {
            it(`${t.name}bucket owner`, function itFn(done) {
                const newPolicy = this.test.basePolicy;
                newPolicy.Statement[0][t.keyToChange] = t.bucketValue;
                bucket.setBucketPolicy(newPolicy);
                const allowed = isBucketAuthorized(bucket, bucAction,
                    t.bucketId, t.bucketAuthInfo, log);
                assert.equal(allowed, t.expected);
                done();
            });
        });

        it('should deny access to non-bucket owner if two statements apply ' +
        'to principal but one denies access', function itFn(done) {
            const newPolicy = this.test.basePolicy;
            newPolicy.Statement[1] = {
                Effect: 'Deny',
                Principal: { CanonicalUser: [altAcctCanonicalId] },
                Resource: `arn:aws:s3:::${bucket.getName()}`,
                Action: 's3:*',
            };
            bucket.setBucketPolicy(newPolicy);
            const allowed = isBucketAuthorized(bucket, bucAction,
                altAcctCanonicalId, null, log);
            assert.equal(allowed, false);
            done();
        });

        it('should deny access to non-bucket owner with an unsupported action type',
        done => {
            const allowed = isBucketAuthorized(bucket, 'unsupportedAction',
                altAcctCanonicalId, null, log);
            assert.equal(allowed, false);
            done();
        });
    });

    describe('isObjAuthorized with no policy set', () => {
        before(() => {
            bucket.setBucketPolicy(null);
        });

        it('should allow access to object owner', done => {
            const allowed = isObjAuthorized(bucket, object, objAction,
                objectOwnerCanonicalId, null, log);
            assert.equal(allowed, true);
            done();
        });

        it('should deny access to non-object owner',
        done => {
            const allowed = isObjAuthorized(bucket, object, objAction,
                altAcctCanonicalId, null, log);
            assert.equal(allowed, false);
            done();
        });
    });

    describe('isObjAuthorized with bucket policy set', () => {
        beforeEach(function beFn() {
            const newPolicyObj = basePolicyObj;
            newPolicyObj.Statement.Resource =
                `arn:aws:s3:::${bucket.getName()}/*`;
            this.currentTest.basePolicy = new BucketPolicy(JSON.stringify(
                newPolicyObj)).getBucketPolicy();
            bucket.setBucketPolicy(this.currentTest.basePolicy);
        });

        it('should allow access to non-object owner if principal is set to "*"',
        done => {
            const allowed = isObjAuthorized(bucket, object, objAction,
                altAcctCanonicalId, null, log);
            assert.equal(allowed, true);
            done();
        });

        it('should allow access to public user if principal is set to "*"',
        done => {
            const allowed = isObjAuthorized(bucket, object, objAction,
                constants.publicId, null, log);
            assert.equal(allowed, true);
            done();
        });

        authTests.forEach(t => {
            it(`${t.name}object owner`, function itFn(done) {
                const newPolicy = this.test.basePolicy;
                newPolicy.Statement[0][t.keyToChange] = t.objectValue;
                bucket.setBucketPolicy(newPolicy);
                const allowed = isObjAuthorized(bucket, object, objAction,
                    t.objectId, t.objectAuthInfo, log);
                assert.equal(allowed, t.expected);
                done();
            });
        });

        it('should deny access to non-object owner if two statements apply ' +
        'to principal but one denies access', function itFn(done) {
            const newPolicy = this.test.basePolicy;
            newPolicy.Statement[1] = {
                Effect: 'Deny',
                Principal: { CanonicalUser: [altAcctCanonicalId] },
                Resource: `arn:aws:s3:::${bucket.getName()}/*`,
                Action: 's3:*',
            };
            bucket.setBucketPolicy(newPolicy);
            const allowed = isObjAuthorized(bucket, object, objAction,
                altAcctCanonicalId, null, log);
            assert.equal(allowed, false);
            done();
        });

        it('should deny access to non-object owner with an unsupported action type',
        done => {
            const allowed = isObjAuthorized(bucket, object, 'unsupportedAction',
                altAcctCanonicalId, null, log);
            assert.equal(allowed, false);
            done();
        });
    });

    describe('validate policy resource', () => {
        resourceTests.forEach(t => {
            it(`should return ${t.name}`, done => {
                const newPolicy = basePolicyObj;
                newPolicy.Statement.Resource = t.rValue;
                newPolicy.Statement = [newPolicy.Statement];
                assert.equal(
                    validatePolicyResource(bucketName, newPolicy), t.expected);
                done();
            });
        });

        it('should return false if any statement resource does not match ' +
        'bucket arn', done => {
            const newPolicy = basePolicyObj;
            newPolicy.Statement = [newPolicy.Statement];
            newPolicy.Statement[1] = basePolicyObj.Statement;
            newPolicy.Statement[0].Resource = `arn:aws:s3:::${bucketName}`;
            assert.equal(validatePolicyResource(bucketName, newPolicy), false);
            done();
        });
    });
});
