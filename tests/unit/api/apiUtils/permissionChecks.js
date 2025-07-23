const assert = require('assert');

const { isLifecycleSession, checkBucketPolicyResult, checkBucketPolicy } =
    require('../../../../lib/api/apiUtils/authorization/permissionChecks.js');
const { DummyRequestLogger } = require('../../helpers');

const stubLog = new DummyRequestLogger();

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
    /* eslint-disable max-len */
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
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
            description: 'bucket owner and requester share the same account, principal account ID, Allow policy should return ALLOW',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            AWS: '123456789012',
                        },
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
            description: 'bucket owner and requester don\'t share the same account, Allow policy should return CROSS ACCOUNT',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            AWS: 'arn:aws:iam::123456789012:root',
                        },
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
            description: 'bucket owner and requester don\'t share the same account, principal account ID, Allow policy should return CROSS ACCOUNT',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            AWS: '123456789012',
                        },
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
            description: 'bucket owner and requester don\'t share the same account, Deny policy should return DENY',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Deny',
                        Principal: {
                            AWS: 'arn:aws:iam::123456789012:root',
                        },
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
            description: 'bucket owner and requester don\'t share the same account, requester is root, Allow policy should return ALLOW',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            AWS: 'arn:aws:iam::123456789012:root',
                        },
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
            description: 'bucket owner and requester don\'t share the same account, requester is root, Deny policy should return DENY',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Deny',
                        Principal: {
                            AWS: 'arn:aws:iam::123456789012:root',
                        },
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
            description: 'bucket owner and requester don\'t share the same account, requester and principal are users, Allow policy should return CROSS_ACCOUNT',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            AWS: 'arn:aws:iam::123456789012:user/testuser',
                        },
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
            description: 'bucket owner and requester don\'t share the same account, requester and principal are users, Deny policy should return EXPLICIT_DENY',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Deny',
                        Principal: {
                            AWS: 'arn:aws:iam::123456789012:user/testuser',
                        },
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
            description: 'bucket owner and requester don\'t share the same account, wildcard "*" principal, Allow policy should return CROSS_ACCOUNT_ALLOW',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            AWS: '*',
                        },
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
            description: 'bucket owner and requester share the same account, wildcard "*" principal, Allow policy should return ALLOW',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            AWS: '*',
                        },
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
            description: 'bucket owner and requester share the same account, wildcard "*" principal, string typeof principal , Allow policy should return ALLOW',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: '*',
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
            description: 'bucket owner and requester don\'t share the same account, wildcard "*" principal, string typeof principal, Allow policy should return CROSS_ACCOUNT_ALLOW',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: '*',
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
            description: 'bucket owner and requester don\'t share the same account, no bucket policy for user, canonical user principal, Allow policy should return DEFAULT_DENY',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            CanonicalUser: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
                        },
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
            description: 'bucket owner and requester don\'t share the same account, canonical user principal, Allow policy should return CROSS_ACCOUNT_ALLOW',
            policy: {
                Statement: [
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            CanonicalUser: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
                        },
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
                    },
                    {
                        Sid: 'Example permissions',
                        Effect: 'Deny',
                        Principal: {
                            CanonicalUser: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
                        },
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
                    },
                    {
                        Sid: 'Example permissions',
                        Effect: 'Allow',
                        Principal: {
                            CanonicalUser: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be',
                        },
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
                        Action: [
                            's3:*',
                        ],
                        Resource: [
                            'arn:aws:s3:::amzn-s3-demo-bucket',
                        ],
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
    /* eslint-enable max-len */

    tests.forEach(t => {
        it(t.description, () => {
            const res = checkBucketPolicy(t.policy, t.requestType, t.canonicalID, t.arn, t.bucketOwner,
                t.log, t.request, t.actionImplicitDenies);
            assert.equal(res, t.expectedResult);
        });
    });
});
