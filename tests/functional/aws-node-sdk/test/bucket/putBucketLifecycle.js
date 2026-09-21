const assert = require('assert');
const { errors } = require('arsenal');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketLifecycleConfigurationCommand,
} = require('@aws-sdk/client-s3');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');
const { config } = require('../../../../../lib/Config');

const MAX_DAYS = 2147483647; // Max 32-bit signed binary integer.

const bucket = 'lifecycleputtestbucket';
const expirationRule = {
    ID: 'test-id',
    Status: 'Enabled',
    Prefix: '',
    Expiration: {
        Days: 1,
    },
};

// Check for the expected error response code and status code.
function assertError(err, expectedErr) {
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        assert.strictEqual(
            err.name,
            expectedErr,
            'incorrect error response ' + `code: should be '${expectedErr}' but got '${err.name}'`,
        );
        assert.strictEqual(
            err.$metadata.httpStatusCode,
            errors[expectedErr].code,
            'incorrect error status code: should be  ' +
                `${errors[expectedErr].code}, but got '${err.$metadata.httpStatusCode}'`,
        );
    }
}

function getLifecycleParams(paramToChange) {
    const newParam = {};
    const lifecycleConfig = {
        Rules: [expirationRule],
    };
    if (paramToChange) {
        newParam[paramToChange.key] = paramToChange.value;
        lifecycleConfig.Rules[0] = Object.assign({}, expirationRule, newParam);
    }
    return {
        Bucket: bucket,
        LifecycleConfiguration: lifecycleConfig,
    };
}

describe('aws-sdk test put bucket lifecycle', () => {
    let s3;
    let otherAccountS3;

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return done();
    });

    it('should return NoSuchBucket error if bucket does not exist', async () => {
        const params = getLifecycleParams();
        try {
            await s3.send(new PutBucketLifecycleConfigurationCommand(params));
            throw new Error('Expected NoSuchBucket error');
        } catch (err) {
            assertError(err, 'NoSuchBucket');
        }
    });

    describe('config rules', () => {
        beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: bucket })));

        afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

        it('should return AccessDenied if user is not bucket owner', async () => {
            const params = getLifecycleParams();
            try {
                await otherAccountS3.send(new PutBucketLifecycleConfigurationCommand(params));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                assertError(err, 'AccessDenied');
            }
        });

        it('should put lifecycle configuration on bucket', async () => {
            const params = getLifecycleParams();
            await s3.send(new PutBucketLifecycleConfigurationCommand(params));
        });

        it(
            'should not allow lifecycle configuration with duplicated rule id ' + 'and with Origin header set',
            async () => {
                const origin = 'http://www.allowedwebsite.com';
                const lifecycleConfig = {
                    Rules: [expirationRule, expirationRule],
                };
                const params = {
                    Bucket: bucket,
                    LifecycleConfiguration: lifecycleConfig,
                };

                const clientConfig = getConfig('default', { signatureVersion: 'v4' });
                const clientWithOrigin = new S3Client({
                    ...clientConfig,
                    requestHandler: {
                        handle: async request => {
                            if (!request.headers) {
                                // eslint-disable-next-line no-param-reassign
                                request.headers = {};
                            }
                            // eslint-disable-next-line no-param-reassign
                            request.headers.origin = origin;
                            return clientConfig.requestHandler.handle(request);
                        },
                    },
                });
                try {
                    await clientWithOrigin.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected InvalidRequest error');
                } catch (err) {
                    assertError(err, 'InvalidRequest');
                }
            },
        );

        it('should not allow lifecycle config with no Status', async () => {
            const params = getLifecycleParams({ key: 'Status', value: '' });
            try {
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                throw new Error('Expected MalformedXML error');
            } catch (err) {
                assertError(err, 'MalformedXML');
            }
        });

        it('should not allow lifecycle config with no Prefix or Filter', async () => {
            const params = getLifecycleParams({ key: 'Prefix', value: null });
            try {
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                throw new Error('Expected MalformedXML error');
            } catch (err) {
                assertError(err, 'MalformedXML');
            }
        });

        it('should not allow lifecycle config with empty action', async () => {
            const params = getLifecycleParams({ key: 'Expiration', value: {} });
            try {
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                throw new Error('Expected MalformedXML error');
            } catch (err) {
                assertError(err, 'MalformedXML');
            }
        });

        it('should not allow lifecycle config with ID longer than 255 char', async () => {
            const params = getLifecycleParams({ key: 'ID', value: 'a'.repeat(256) });
            try {
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                throw new Error('Expected InvalidArgument error');
            } catch (err) {
                assertError(err, 'InvalidArgument');
            }
        });

        it('should allow lifecycle config with Prefix length < 1024', async () => {
            const params = getLifecycleParams({ key: 'Prefix', value: 'a'.repeat(1023) });
            await s3.send(new PutBucketLifecycleConfigurationCommand(params));
        });

        it('should allow lifecycle config with Prefix length === 1024', async () => {
            const params = getLifecycleParams({ key: 'Prefix', value: 'a'.repeat(1024) });
            await s3.send(new PutBucketLifecycleConfigurationCommand(params));
        });

        it('should not allow lifecycle config with Prefix length > 1024', async () => {
            const params = getLifecycleParams({ key: 'Prefix', value: 'a'.repeat(1025) });
            try {
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                throw new Error('Expected InvalidRequest error');
            } catch (err) {
                assertError(err, 'InvalidRequest');
            }
        });

        it('should not allow lifecycle config with Filter.Prefix length > 1024', async () => {
            const params = getLifecycleParams({
                key: 'Filter',
                value: { Prefix: 'a'.repeat(1025) },
            });
            delete params.LifecycleConfiguration.Rules[0].Prefix;
            try {
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                throw new Error('Expected InvalidRequest error');
            } catch (err) {
                assertError(err, 'InvalidRequest');
            }
        });

        it('should not allow lifecycle config with Filter.And.Prefix length ' + '> 1024', async () => {
            const params = getLifecycleParams({
                key: 'Filter',
                value: {
                    And: {
                        Prefix: 'a'.repeat(1025),
                        Tags: [{ Key: 'a', Value: 'b' }],
                    },
                },
            });
            delete params.LifecycleConfiguration.Rules[0].Prefix;
            try {
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                throw new Error('Expected InvalidRequest error');
            } catch (err) {
                assertError(err, 'InvalidRequest');
            }
        });

        it('should allow lifecycle config with Tag.Key length < 128', async () => {
            const params = getLifecycleParams({
                key: 'Filter',
                value: { Tag: { Key: 'a'.repeat(127), Value: 'bar' } },
            });
            delete params.LifecycleConfiguration.Rules[0].Prefix;
            await s3.send(new PutBucketLifecycleConfigurationCommand(params));
        });

        it('should allow lifecycle config with Tag.Key length === 128', async () => {
            const params = getLifecycleParams({
                key: 'Filter',
                value: { Tag: { Key: 'a'.repeat(128), Value: 'bar' } },
            });
            delete params.LifecycleConfiguration.Rules[0].Prefix;
            await s3.send(new PutBucketLifecycleConfigurationCommand(params));
        });

        it('should not allow lifecycle config with Tag.Key length > 128', async () => {
            const params = getLifecycleParams({
                key: 'Filter',
                value: { Tag: { Key: 'a'.repeat(129), Value: 'bar' } },
            });
            delete params.LifecycleConfiguration.Rules[0].Prefix;
            try {
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                throw new Error('Expected InvalidRequest error');
            } catch (err) {
                assertError(err, 'InvalidRequest');
            }
        });

        it('should allow lifecycle config with Tag.Value length < 256', async () => {
            const params = getLifecycleParams({
                key: 'Filter',
                value: { Tag: { Key: 'a', Value: 'b'.repeat(255) } },
            });
            delete params.LifecycleConfiguration.Rules[0].Prefix;
            await s3.send(new PutBucketLifecycleConfigurationCommand(params));
        });

        it('should allow lifecycle config with Tag.Value length === 256', async () => {
            const params = getLifecycleParams({
                key: 'Filter',
                value: { Tag: { Key: 'a', Value: 'b'.repeat(256) } },
            });
            delete params.LifecycleConfiguration.Rules[0].Prefix;
            await s3.send(new PutBucketLifecycleConfigurationCommand(params));
        });

        it('should not allow lifecycle config with Tag.Value length > 256', async () => {
            const params = getLifecycleParams({
                key: 'Filter',
                value: { Tag: { Key: 'a', Value: 'b'.repeat(257) } },
            });
            delete params.LifecycleConfiguration.Rules[0].Prefix;
            try {
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                throw new Error('Expected InvalidRequest error');
            } catch (err) {
                assertError(err, 'InvalidRequest');
            }
        });

        it('should not allow lifecycle config with Prefix and Filter', async () => {
            const params = getLifecycleParams({ key: 'Filter', value: { Prefix: 'foo' } });
            try {
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                throw new Error('Expected MalformedXML error');
            } catch (err) {
                assertError(err, 'MalformedXML');
            }
        });

        it('should allow lifecycle config without ID', async () => {
            const params = getLifecycleParams({ key: 'ID', value: '' });
            await s3.send(new PutBucketLifecycleConfigurationCommand(params));
        });

        it('should allow lifecycle config with multiple actions', async () => {
            const params = getLifecycleParams({
                key: 'NoncurrentVersionExpiration',
                value: { NoncurrentDays: 1 },
            });
            await s3.send(new PutBucketLifecycleConfigurationCommand(params));
        });

        describe('with Rule.Filter not Rule.Prefix', () => {
            before(done => {
                expirationRule.Prefix = null;
                done();
            });

            it('should allow config with empty Filter', async () => {
                const params = getLifecycleParams({ key: 'Filter', value: {} });
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
            });

            it('should not allow config with And & Prefix', async () => {
                const params = getLifecycleParams({ key: 'Filter', value: { Prefix: 'foo', And: {} } });
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected MalformedXML error');
                } catch (err) {
                    assertError(err, 'MalformedXML');
                }
            });

            it('should not allow config with And & Tag', async () => {
                const params = getLifecycleParams({
                    key: 'Filter',
                    value: { Tag: { Key: 'foo', Value: 'bar' }, And: {} },
                });
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected MalformedXML error');
                } catch (err) {
                    assertError(err, 'MalformedXML');
                }
            });

            it('should not allow config with Prefix & Tag', async () => {
                const params = getLifecycleParams({
                    key: 'Filter',
                    value: { Tag: { Key: 'foo', Value: 'bar' }, Prefix: 'foo' },
                });
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected MalformedXML error');
                } catch (err) {
                    assertError(err, 'MalformedXML');
                }
            });

            it('should allow config with only Prefix', async () => {
                const params = getLifecycleParams({ key: 'Filter', value: { Prefix: 'foo' } });
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
            });

            it('should allow config with only Tag', async () => {
                const params = getLifecycleParams({
                    key: 'Filter',
                    value: { Tag: { Key: 'foo', Value: 'ba' } },
                });
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
            });

            it('should not allow config with And.Prefix & no And.Tags', async () => {
                const params = getLifecycleParams({ key: 'Filter', value: { And: { Prefix: 'foo' } } });
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected MalformedXML error');
                } catch (err) {
                    assertError(err, 'MalformedXML');
                }
            });

            it('should not allow config with only one And.Tags', async () => {
                const params = getLifecycleParams({
                    key: 'Filter',
                    value: { And: { Tags: [{ Key: 'f', Value: 'b' }] } },
                });
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected MalformedXML error');
                } catch (err) {
                    assertError(err, 'MalformedXML');
                }
            });

            it('should allow config with And.Tags & no And.Prefix', async () => {
                const params = getLifecycleParams({
                    key: 'Filter',
                    value: {
                        And: {
                            Tags: [
                                { Key: 'foo', Value: 'bar' },
                                { Key: 'foo2', Value: 'bar2' },
                            ],
                        },
                    },
                });
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
            });

            it('should allow config with And.Tags & And.Prefix', async () => {
                const params = getLifecycleParams({
                    key: 'Filter',
                    value: {
                        And: {
                            Prefix: 'foo',
                            Tags: [
                                { Key: 'foo', Value: 'bar' },
                                { Key: 'foo2', Value: 'bar2' },
                            ],
                        },
                    },
                });
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
            });
        });

        describe.skip('with NoncurrentVersionTransitions', () => {
            function getParams(noncurrentVersionTransition) {
                return {
                    Bucket: bucket,
                    LifecycleConfiguration: {
                        Rules: [
                            {
                                ID: 'test',
                                Status: 'Enabled',
                                Prefix: '',
                                noncurrentVersionTransition,
                            },
                        ],
                    },
                };
            }

            it('should allow config', async () => {
                const noncurrentVersionTransition = {
                    NoncurrentDays: 1,
                };
                const params = getParams(noncurrentVersionTransition);
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
            });

            it(`should not allow NoncurrentDays value exceeding ${MAX_DAYS}`, async () => {
                const noncurrentVersionExpiration = {
                    NoncurrentDays: MAX_DAYS + 1,
                };
                const params = getParams(noncurrentVersionExpiration);
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected MalformedXML error');
                } catch (err) {
                    assert.strictEqual(err.name, 'MalformedXML');
                }
            });

            it('should not allow negative NoncurrentDays', async () => {
                const noncurrentVersionExpiration = {
                    NoncurrentDays: -1,
                };
                const params = getParams(noncurrentVersionExpiration);
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected InvalidArgument error');
                } catch (err) {
                    assert.strictEqual(err.name, 'InvalidArgument');
                    assert.strictEqual(
                        err.message,
                        "'NoncurrentDays' in NoncurrentVersionExpiration " + 'action must be nonnegative',
                    );
                }
            });

            it('should not allow config missing NoncurrentDays', async () => {
                const noncurrentVersionExpiration = {};
                const params = getParams(noncurrentVersionExpiration);
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected MalformedXML error');
                } catch (err) {
                    assert.strictEqual(err.name, 'MalformedXML');
                }
            });
        });
        // NoncurrentVersionTransitions not implemented
        describe.skip('with NoncurrentVersionTransitions', () => {
            function getParams(noncurrentVersionTransitions) {
                return {
                    Bucket: bucket,
                    LifecycleConfiguration: {
                        Rules: [
                            {
                                ID: 'test',
                                Status: 'Enabled',
                                Prefix: '',
                                NoncurrentVersionTransitions: noncurrentVersionTransitions,
                            },
                        ],
                    },
                };
            }

            it('should allow config', async () => {
                const noncurrentVersionTransitions = [
                    {
                        NoncurrentDays: 1,
                        StorageClass: 'us-east-2',
                    },
                ];
                const params = getParams(noncurrentVersionTransitions);
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                } catch (err) {
                    if (err.name === 'NotImplemented') {
                        this.skip();
                    }
                    throw err;
                }
            });

            it.skip('should not allow duplicate StorageClass', async () => {
                const noncurrentVersionTransitions = [
                    {
                        NoncurrentDays: 1,
                        StorageClass: 'us-east-2',
                    },
                    {
                        NoncurrentDays: 2,
                        StorageClass: 'us-east-2',
                    },
                ];
                const params = getParams(noncurrentVersionTransitions);
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected error');
                } catch (err) {
                    if (err.name === 'NotImplemented') {
                        // CloudServer returns NotImplemented for NoncurrentVersionTransitions
                        assert(err.name === 'NotImplemented');
                        return;
                    }
                    assert.strictEqual(err.name, 'InvalidRequest');
                    assert.strictEqual(
                        err.message,
                        "'StorageClass' must be different for " +
                            "'NoncurrentVersionTransition' actions in same " +
                            "'Rule' with prefix ''",
                    );
                }
            });

            it('should not allow unknown StorageClass', async () => {
                const noncurrentVersionTransitions = [
                    {
                        NoncurrentDays: 1,
                        StorageClass: 'unknown',
                    },
                ];
                const params = getParams(noncurrentVersionTransitions);
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected MalformedXML error');
                } catch (err) {
                    assert(
                        err.name === 'MalformedXML' || err.name === 'NotImplemented',
                        `Expected MalformedXML or NotImplemented, got ${err.name}`,
                    );
                }
            });

            it(`should not allow NoncurrentDays value exceeding ${MAX_DAYS}`, async () => {
                const noncurrentVersionTransitions = [
                    {
                        NoncurrentDays: MAX_DAYS + 1,
                        StorageClass: 'us-east-2',
                    },
                ];
                const params = getParams(noncurrentVersionTransitions);
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected MalformedXML error');
                } catch (err) {
                    assert(
                        err.name === 'MalformedXML' || err.name === 'NotImplemented',
                        `Expected MalformedXML or NotImplemented, got ${err.name}`,
                    );
                }
            });

            it('should not allow negative NoncurrentDays', async () => {
                const noncurrentVersionTransitions = [
                    {
                        NoncurrentDays: -1,
                        StorageClass: 'us-east-2',
                    },
                ];
                const params = getParams(noncurrentVersionTransitions);
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected error');
                } catch (err) {
                    assert(
                        err.name === 'InvalidArgument' || err.name === 'NotImplemented',
                        `Expected InvalidArgument or NotImplemented, got ${err.name}`,
                    );
                    if (err.name === 'InvalidArgument') {
                        assert.strictEqual(
                            err.message,
                            "'NoncurrentDays' in NoncurrentVersionTransition " + 'action must be nonnegative',
                        );
                    }
                }
            });

            it('should not allow config missing NoncurrentDays', async () => {
                const noncurrentVersionTransitions = [
                    {
                        StorageClass: 'us-east-2',
                    },
                ];
                const params = getParams(noncurrentVersionTransitions);
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected error');
                } catch (err) {
                    assert(
                        err.name === 'MalformedXML' || err.name === 'NotImplemented',
                        `Expected MalformedXML or NotImplemented, got ${err.name}`,
                    );
                }
            });

            it('should not allow config missing StorageClass', async () => {
                const noncurrentVersionTransitions = [
                    {
                        NoncurrentDays: 1,
                    },
                ];
                const params = getParams(noncurrentVersionTransitions);
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected error');
                } catch (err) {
                    assert(
                        err.name === 'MalformedXML' || err.name === 'NotImplemented',
                        `Expected MalformedXML or NotImplemented, got ${err.name}`,
                    );
                }
            });
        });

        // use env S3_CONFIG_FILE with tests (needed in S3C Integration tests)
        const isTransitionSupported = config.supportedLifecycleRules.includes('Transition');

        (isTransitionSupported ? describe.skip : describe)('with Transitions NOT supported', () => {
            it('should return NotImplemented if Transitions rule', async () => {
                const params = {
                    Bucket: bucket,
                    LifecycleConfiguration: {
                        Rules: [
                            {
                                ID: 'test',
                                Status: 'Enabled',
                                Prefix: '',
                                Transitions: [
                                    {
                                        Days: 2,
                                        StorageClass: 'us-east-2',
                                    },
                                ],
                            },
                        ],
                    },
                };
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected NotImplemented error');
                } catch (err) {
                    assert.strictEqual(err.$metadata.httpStatusCode, 501);
                    assert.strictEqual(err.name, 'NotImplemented');
                }
            });
        });

        (isTransitionSupported ? describe : describe.skip)('with Transitions supported', () => {
            function getParams(transitions) {
                return {
                    Bucket: bucket,
                    LifecycleConfiguration: {
                        Rules: [
                            {
                                ID: 'test',
                                Status: 'Enabled',
                                Prefix: '',
                                Transitions: transitions,
                            },
                        ],
                    },
                };
            }

            it('should allow config', async () => {
                const transitions = [
                    {
                        Days: 1,
                        StorageClass: 'us-east-2',
                    },
                ];
                const params = getParams(transitions);
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
            });

            it('should not allow duplicate StorageClass', async () => {
                const transitions = [
                    {
                        Days: 1,
                        StorageClass: 'us-east-2',
                    },
                    {
                        Days: 2,
                        StorageClass: 'us-east-2',
                    },
                ];
                const params = getParams(transitions);
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected InvalidRequest error');
                } catch (err) {
                    assert.strictEqual(err.name, 'InvalidRequest');
                    assert.strictEqual(
                        err.message,
                        "'StorageClass' must be different for 'Transition' " + "actions in same 'Rule' with prefix ''",
                    );
                }
            });

            it('should allow Date', async () => {
                const transitions = [
                    {
                        Date: new Date('2016-01-01T00:00:00.000Z'),
                        StorageClass: 'us-east-2',
                    },
                ];
                const params = getParams(transitions);
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
            });

            it('should not allow speficying both Days and Date value', async () => {
                const transitions = [
                    {
                        Date: new Date('2016-01-01T00:00:00.000Z'),
                        Days: 1,
                        StorageClass: 'us-east-2',
                    },
                ];
                const params = getParams(transitions);
                try {
                    await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                    throw new Error('Expected MalformedXML error');
                } catch (err) {
                    assert.strictEqual(err.name, 'MalformedXML');
                }
            });

            // TODO: Upgrade to aws-sdk >= 2.60.0 for correct Date field support
            it.skip('should not allow speficying both Days and Date value ' + 'across transitions', done => {
                const transitions = [
                    {
                        Date: '2016-01-01T00:00:00.000Z',
                        StorageClass: 'us-east-2',
                    },
                    {
                        Days: 1,
                        StorageClass: 'zenko',
                    },
                ];
                const params = getParams(transitions);
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.code, 'InvalidRequest');
                    assert.strictEqual(
                        err.message,
                        "Found mixed 'Date' and 'Days' based Transition " + "actions in lifecycle rule for prefix ''",
                    );
                    done();
                });
            });

            it(
                'should not allow speficying both Days and Date value ' + 'across transitions and expiration',
                async () => {
                    const transitions = [
                        {
                            Days: 1,
                            StorageClass: 'us-east-2',
                        },
                    ];
                    const params = getParams(transitions);
                    params.LifecycleConfiguration.Rules[0].Expiration = {
                        Date: new Date('2016-01-01T00:00:00.000Z'), // Use proper Date object
                    };
                    try {
                        await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                        throw new Error('Expected InvalidRequest error');
                    } catch (err) {
                        assert.strictEqual(err.name, 'InvalidRequest');
                        assert.strictEqual(
                            err.message,
                            "Found mixed 'Date' and 'Days' based Expiration and " +
                                "Transition actions in lifecycle rule for prefix ''",
                        );
                    }
                },
            );
        });

        // NoncurrentVersionTransitions not implemented
        describe.skip('with NoncurrentVersionTransitions and Transitions', () => {
            it('should allow config', async () => {
                const params = {
                    Bucket: bucket,
                    LifecycleConfiguration: {
                        Rules: [
                            {
                                ID: 'test',
                                Status: 'Enabled',
                                Prefix: '',
                                NoncurrentVersionTransitions: [
                                    {
                                        NoncurrentDays: 1,
                                        StorageClass: 'us-east-2',
                                    },
                                ],
                                Transitions: [
                                    {
                                        Days: 1,
                                        StorageClass: 'us-east-2',
                                    },
                                ],
                            },
                        ],
                    },
                };
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
            });
        });

        it.skip('should not allow config when specifying ' + 'NoncurrentVersionTransitions', async () => {
            const params = {
                Bucket: bucket,
                LifecycleConfiguration: {
                    Rules: [
                        {
                            ID: 'test',
                            Status: 'Enabled',
                            Prefix: '',
                            NoncurrentVersionTransitions: [
                                {
                                    NoncurrentDays: 1,
                                    StorageClass: 'us-east-2',
                                },
                            ],
                        },
                    ],
                },
            };
            try {
                await s3.send(new PutBucketLifecycleConfigurationCommand(params));
                throw new Error('Expected NotImplemented error');
            } catch (err) {
                assert.strictEqual(err.$metadata.httpStatusCode, 501);
                assert.strictEqual(err.name, 'NotImplemented');
            }
        });
    });
});
