const assert = require('assert');

const {
    isLifecycleSession,
    checkBucketPolicyResult,
    checkBucketPolicy,
    isBucketAuthorized,
    isObjAuthorized,
    evaluateBucketPolicyWithIAM,
} = require('../../../../lib/api/apiUtils/authorization/permissionChecks.js');
const { DummyRequestLogger } = require('../../helpers');

const stubLog = new DummyRequestLogger();

const ownerCanonicalId = '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
const otherCanonicalId = 'aaaaaaaabbbbbbbbccccccccddddddddeeeeeeeeffffffffgggggggghhhhhhh1';

describe('authInfoHelper', () => {
    const tests = [
        {
            arn: 'arn:aws:sts::257038443293:assumed-role/rolename/backbeat-lifecycle',
            description: 'a role assumed by lifecycle service',
            expectedResult: true,
        },
        {
            arn: undefined,
            description: 'undefined',
            expectedResult: false,
        },
        {
            arn: '',
            description: 'empty',
            expectedResult: false,
        },
        {
            arn: 'arn:aws:iam::257038443293:user/bart',
            description: 'a user',
            expectedResult: false,
        },
        {
            arn: 'arn:aws:sts::257038443293:assumed-role/rolename/other-service',
            description: 'a role assumed by another service',
            expectedResult: false,
        },
    ];

    tests.forEach(t => {
        it(`should return ${t.expectedResult} if arn is ${t.description}`, () => {
            const result = isLifecycleSession(t.arn);
            assert.equal(result, t.expectedResult);
        });
    });
});

describe('checkBucketPolicy Principal logic', () => {
    const tests = [
        {
            description: 'bucket owner with same canonicalID as requesters should return ALLOW',
            policy: { Statement: [] },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: '',
            bucketOwner: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            log: null,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.ALLOW,
        },
        {
            description: 'bucket owner with different canonicalID as requesters should return DEFAULT_DENY',
            policy: { Statement: [] },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: '',
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: null,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.DEFAULT_DENY,
        },
        {
            description: 'bucket owner and requester share the same account, Allow policy should return ALLOW',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            AWS: 'arn:aws:iam::123456789012:root',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.ALLOW,
        },
        {
            description:
                'bucket owner and requester share the same account, principal account ID, Allow policy should return ALLOW',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            AWS: '123456789012',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.ALLOW,
        },
        {
            description: 'bucket owner and requester share the same account, Deny policy should return DENY',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Deny',
                        Principal: {
                            AWS: 'arn:aws:iam::123456789012:root',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.EXPLICIT_DENY,
        },
        {
            description:
                "bucket owner and requester don't share the same account, Allow policy should return CROSS ACCOUNT",
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            AWS: 'arn:aws:iam::123456789012:root',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.CROSS_ACCOUNT_ALLOW,
        },
        {
            description:
                "bucket owner and requester don't share the same account, principal account ID, Allow policy should return CROSS ACCOUNT",
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            AWS: '123456789012',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.CROSS_ACCOUNT_ALLOW,
        },
        {
            description: "bucket owner and requester don't share the same account, Deny policy should return DENY",
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Deny',
                        Principal: {
                            AWS: 'arn:aws:iam::123456789012:root',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.EXPLICIT_DENY,
        },
        {
            description:
                "bucket owner and requester don't share the same account, requester is root, Allow policy should return ALLOW",
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            AWS: 'arn:aws:iam::123456789012:root',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:/accountName/',
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.ALLOW,
        },
        {
            description:
                "bucket owner and requester don't share the same account, requester is root, Deny policy should return DENY",
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Deny',
                        Principal: {
                            AWS: 'arn:aws:iam::123456789012:root',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:/accountName/',
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.EXPLICIT_DENY,
        },
        {
            description:
                "bucket owner and requester don't share the same account, requester and principal are users, Allow policy should return CROSS_ACCOUNT",
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            AWS: 'arn:aws:iam::123456789012:user/testuser',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.CROSS_ACCOUNT_ALLOW,
        },
        {
            description:
                "bucket owner and requester don't share the same account, requester and principal are users, Deny policy should return EXPLICIT_DENY",
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Deny',
                        Principal: {
                            AWS: 'arn:aws:iam::123456789012:user/testuser',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.EXPLICIT_DENY,
        },
        {
            description:
                'bucket owner and requester don\'t share the same account, wildcard "*" principal, Allow policy should return CROSS_ACCOUNT_ALLOW',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            AWS: '*',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.CROSS_ACCOUNT_ALLOW,
        },
        {
            description:
                'bucket owner and requester share the same account, wildcard "*" principal, Allow policy should return ALLOW',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            AWS: '*',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.ALLOW,
        },
        {
            description:
                'bucket owner and requester share the same account, wildcard "*" principal, string typeof principal , Allow policy should return ALLOW',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: '*',
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.ALLOW,
        },
        {
            description:
                'bucket owner and requester don\'t share the same account, wildcard "*" principal, string typeof principal, Allow policy should return CROSS_ACCOUNT_ALLOW',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: '*',
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.CROSS_ACCOUNT_ALLOW,
        },
        {
            description:
                "bucket owner and requester don't share the same account, no bucket policy for user, canonical user principal, Allow policy should return DEFAULT_DENY",
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            CanonicalUser: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.DEFAULT_DENY,
        },
        {
            description:
                "bucket owner and requester don't share the same account, canonical user principal, Allow policy should return CROSS_ACCOUNT_ALLOW",
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            CanonicalUser: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.CROSS_ACCOUNT_ALLOW,
        },
        {
            description: 'first policy allows second denies, should return EXPLICIT_DENY',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            CanonicalUser: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                    {
                        Sid: 'Example permissions',
                        Effect: 'Deny',
                        Principal: {
                            CanonicalUser: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.EXPLICIT_DENY,
        },
        {
            description: 'first policy denies second allows, should return EXPLICIT_DENY',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Deny',
                        Principal: {
                            CanonicalUser: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            CanonicalUser: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
                        },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
            arn: 'arn:aws:iam::123456789012:user/testuser',
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.EXPLICIT_DENY,
        },
        {
            description: 'anonymous user and wildcard policy should return Allow',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: '*',
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: undefined,
            arn: undefined,
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.ALLOW,
        },
        {
            description: 'anonymous user and wildcard AWS policy should return Allow',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: { AWS: '*' },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: undefined,
            arn: undefined,
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.ALLOW,
        },
        {
            description: 'anonymous user and wildcard CanonicalUser policy should return Allow',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: { CanonicalUser: '*' },
                        Action: ['s3:*'],
                        Resource: ['arn:aws:s3:::amzn-s3-demo-bucket'],
                    },
                ],
            },
            requestType: 'bucketGet',
            canonicalID: undefined,
            arn: undefined,
            bucketOwner: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            log: stubLog,
            request: null,
            actionImplicitDenies: { bucketGet: false },

            expectedResult: checkBucketPolicyResult.ALLOW,
        },
    ];

    tests.forEach(t => {
        it(t.description, () => {
            const res = checkBucketPolicy(
                t.policy,
                t.requestType,
                t.canonicalID,
                t.arn,
                t.bucketOwner,
                t.log,
                t.request,
                t.actionImplicitDenies,
            );
            assert.equal(res, t.expectedResult);
        });
    });
});

describe('aclRequired field in isBucketAuthorized', () => {
    function makeBucket(owner, acl, bucketPolicy) {
        return {
            getOwner: () => owner,
            getAcl: () => ({
                Canned: acl?.Canned || '',
                FULL_CONTROL: acl?.FULL_CONTROL || [],
                READ: acl?.READ || [],
                READ_ACP: acl?.READ_ACP || [],
                WRITE: acl?.WRITE || [],
                WRITE_ACP: acl?.WRITE_ACP || [],
            }),
            getBucketPolicy: () => bucketPolicy || null,
        };
    }

    function makeRequest() {
        return { serverAccessLog: {}, socket: { remoteAddress: '127.0.0.1' }, headers: {} };
    }

    function makeAuthInfo(canonicalID, arn, isIAMUser = false) {
        return {
            getCanonicalID: () => canonicalID,
            getArn: () => arn,
            isRequesterAnIAMUser: () => isIAMUser,
        };
    }

    it('should not set aclRequired when requester is bucket owner', () => {
        const request = makeRequest();
        const bucket = makeBucket(ownerCanonicalId);
        const authInfo = makeAuthInfo(ownerCanonicalId, 'arn:aws:iam::123456789012:/owner/');
        isBucketAuthorized(bucket, 'bucketGet', ownerCanonicalId, authInfo, stubLog, request, { bucketGet: false });
        assert.strictEqual(request.serverAccessLog.aclRequired, undefined);
    });

    it('should set aclRequired to Yes for non-owner with no bucket policy', () => {
        const request = makeRequest();
        const bucket = makeBucket(ownerCanonicalId, { READ: [otherCanonicalId] });
        const authInfo = makeAuthInfo(otherCanonicalId, 'arn:aws:iam::999999999999:user/other');
        isBucketAuthorized(bucket, 'bucketGet', otherCanonicalId, authInfo, stubLog, request, { bucketGet: false });
        assert.strictEqual(request.serverAccessLog.aclRequired, 'Yes');
    });

    it('should not set aclRequired when bucket policy explicitly allows', () => {
        const request = makeRequest();
        // Use an IAM user in the bucket owner's account so principal match is OK (not CROSS_ACCOUNT)
        // and the owner early-return doesn't fire (requester is IAM user, not account root)
        const iamUserCanonicalId = ownerCanonicalId;
        const bucket = makeBucket(
            ownerCanonicalId,
            {},
            {
                Statement: [
                    {
                        Effect: 'Allow',
                        Principal: { AWS: 'arn:aws:iam::123456789012:user/iamuser' },
                        Action: ['s3:ListBucket'],
                        Resource: ['arn:aws:s3:::test-bucket'],
                    },
                ],
            },
        );
        const authInfo = makeAuthInfo(iamUserCanonicalId, 'arn:aws:iam::123456789012:user/iamuser', true);
        isBucketAuthorized(bucket, 'bucketGet', iamUserCanonicalId, authInfo, stubLog, request, { bucketGet: false });
        assert.strictEqual(request.serverAccessLog.aclRequired, undefined);
    });

    it('should not set aclRequired when bucket policy explicitly denies', () => {
        const request = makeRequest();
        const iamUserCanonicalId = ownerCanonicalId;
        const bucket = makeBucket(
            ownerCanonicalId,
            { READ: [iamUserCanonicalId] },
            {
                Statement: [
                    {
                        Effect: 'Deny',
                        Principal: { AWS: 'arn:aws:iam::123456789012:user/iamuser' },
                        Action: ['s3:ListBucket'],
                        Resource: ['arn:aws:s3:::test-bucket'],
                    },
                ],
            },
        );
        const authInfo = makeAuthInfo(iamUserCanonicalId, 'arn:aws:iam::123456789012:user/iamuser', true);
        isBucketAuthorized(bucket, 'bucketGet', iamUserCanonicalId, authInfo, stubLog, request, { bucketGet: false });
        assert.strictEqual(request.serverAccessLog.aclRequired, undefined);
    });

    it('should set aclRequired to Yes when bucket policy returns DEFAULT_DENY', () => {
        const request = makeRequest();
        // Policy grants PutObject to a different principal — nothing matches
        // the bucketGet request from otherCanonicalId, so checkBucketPolicy
        // returns DEFAULT_DENY and falls back to ACL evaluation.
        const bucket = makeBucket(
            ownerCanonicalId,
            { READ: [otherCanonicalId] },
            {
                Statement: [
                    {
                        Effect: 'Allow',
                        Principal: { AWS: 'arn:aws:iam::111111111111:root' },
                        Action: ['s3:PutObject'],
                        Resource: ['arn:aws:s3:::test-bucket/*'],
                    },
                ],
            },
        );
        const authInfo = makeAuthInfo(otherCanonicalId, 'arn:aws:iam::999999999999:user/other');
        isBucketAuthorized(bucket, 'bucketGet', otherCanonicalId, authInfo, stubLog, request, { bucketGet: false });
        assert.strictEqual(request.serverAccessLog.aclRequired, 'Yes');
    });

    it('should handle missing serverAccessLog gracefully', () => {
        const request = {};
        const bucket = makeBucket(ownerCanonicalId, { READ: [otherCanonicalId] });
        const authInfo = makeAuthInfo(otherCanonicalId, 'arn:aws:iam::999999999999:user/other');
        assert.doesNotThrow(() => {
            isBucketAuthorized(bucket, 'bucketGet', otherCanonicalId, authInfo, stubLog, request, { bucketGet: false });
        });
    });
});

describe('aclRequired field in isObjAuthorized', () => {
    function makeBucket(owner, acl, bucketPolicy) {
        return {
            getOwner: () => owner,
            getName: () => 'test-bucket',
            getAcl: () => ({
                Canned: acl?.Canned || '',
                FULL_CONTROL: acl?.FULL_CONTROL || [],
                READ: acl?.READ || [],
                READ_ACP: acl?.READ_ACP || [],
                WRITE: acl?.WRITE || [],
                WRITE_ACP: acl?.WRITE_ACP || [],
            }),
            getBucketPolicy: () => bucketPolicy || null,
        };
    }

    function makeRequest() {
        return { serverAccessLog: {} };
    }

    function makeAuthInfo(canonicalID, arn) {
        return {
            getCanonicalID: () => canonicalID,
            getArn: () => arn,
            isRequesterAnIAMUser: () => false,
        };
    }

    function makeObjectMD(ownerId) {
        return {
            'owner-id': ownerId,
            acl: {
                Canned: '',
                FULL_CONTROL: [],
                READ: [],
                READ_ACP: [],
                WRITE: [],
                WRITE_ACP: [],
            },
        };
    }

    it('should not set aclRequired when requester is object owner', () => {
        const request = makeRequest();
        const bucket = makeBucket(ownerCanonicalId);
        const objectMD = makeObjectMD(otherCanonicalId);
        const authInfo = makeAuthInfo(otherCanonicalId, 'arn:aws:iam::999999999999:/account/');
        isObjAuthorized(bucket, objectMD, 'objectGet', otherCanonicalId, authInfo, stubLog, request, {
            objectGet: false,
        });
        assert.strictEqual(request.serverAccessLog.aclRequired, undefined);
    });

    it('should set aclRequired to Yes for non-owner with no bucket policy', () => {
        const request = makeRequest();
        const bucket = makeBucket(ownerCanonicalId);
        const objectMD = makeObjectMD(ownerCanonicalId);
        objectMD.acl.READ = [otherCanonicalId];
        const authInfo = makeAuthInfo(otherCanonicalId, 'arn:aws:iam::999999999999:user/other');
        isObjAuthorized(bucket, objectMD, 'objectGet', otherCanonicalId, authInfo, stubLog, request, {
            objectGet: false,
        });
        assert.strictEqual(request.serverAccessLog.aclRequired, 'Yes');
    });
});

describe('aclRequired field in evaluateBucketPolicyWithIAM', () => {
    function makeBucket(owner, bucketPolicy) {
        return {
            getOwner: () => owner,
            getAcl: () => ({
                Canned: '',
                FULL_CONTROL: [],
                READ: [],
                READ_ACP: [],
                WRITE: [],
                WRITE_ACP: [],
            }),
            getBucketPolicy: () => bucketPolicy || null,
        };
    }

    function makeAuthInfo(canonicalID, arn) {
        return {
            getCanonicalID: () => canonicalID,
            getArn: () => arn,
            isRequesterAnIAMUser: () => false,
        };
    }

    it('should not set aclRequired (ACLs are not actually consulted)', () => {
        const request = { serverAccessLog: {} };
        const bucket = makeBucket(ownerCanonicalId);
        const authInfo = makeAuthInfo(otherCanonicalId, 'arn:aws:iam::999999999999:user/other');
        evaluateBucketPolicyWithIAM(
            bucket,
            'objectDelete',
            otherCanonicalId,
            authInfo,
            { objectDelete: false },
            stubLog,
            request,
        );
        assert.strictEqual(request.serverAccessLog.aclRequired, undefined);
    });
});
