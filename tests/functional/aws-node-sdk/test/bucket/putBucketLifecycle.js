const assert = require('assert');
const { errors } = require('arsenal');
const { S3 } = require('aws-sdk');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

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
function assertError(err, expectedErr, cb) {
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        assert.strictEqual(err.code, expectedErr, 'incorrect error response ' +
            `code: should be '${expectedErr}' but got '${err.code}'`);
        assert.strictEqual(err.statusCode, errors[expectedErr].code,
            'incorrect error status code: should be  ' +
            `${errors[expectedErr].code}, but got '${err.statusCode}'`);
    }
    cb();
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
        s3 = new S3(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return done();
    });

    it('should return NoSuchBucket error if bucket does not exist', done => {
        const params = getLifecycleParams();
        s3.putBucketLifecycleConfiguration(params, err =>
            assertError(err, 'NoSuchBucket', done));
    });

    describe('config rules', () => {
        beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        it('should return AccessDenied if user is not bucket owner', done => {
            const params = getLifecycleParams();
            otherAccountS3.putBucketLifecycleConfiguration(params,
            err => assertError(err, 'AccessDenied', done));
        });

        it('should put lifecycle configuration on bucket', done => {
            const params = getLifecycleParams();
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, null, done));
        });

        it('should not allow lifecycle config with no Status', done => {
            const params = getLifecycleParams({ key: 'Status', value: '' });
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should not allow lifecycle config with no Prefix or Filter',
        done => {
            const params = getLifecycleParams({ key: 'Prefix', value: null });
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should not allow lifecycle config with empty action', done => {
            const params = getLifecycleParams({ key: 'Expiration', value: {} });
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should not allow lifecycle config with ID longer than 255 char',
        done => {
            const params =
                getLifecycleParams({ key: 'ID', value: 'a'.repeat(256) });
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, 'InvalidArgument', done));
        });

        it('should not allow lifecycle config with Prefix and Filter', done => {
            const params = getLifecycleParams(
                { key: 'Filter', value: { Prefix: 'foo' } });
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, 'MalformedXML', done));
        });

        it('should allow lifecycle config without ID', done => {
            const params = getLifecycleParams({ key: 'ID', value: '' });
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, null, done));
        });

        it('should allow lifecycle config with multiple actions', done => {
            const params = getLifecycleParams({
                key: 'NoncurrentVersionExpiration',
                value: { NoncurrentDays: 1 },
            });
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, null, done));
        });


        describe('with Rule.Filter not Rule.Prefix', () => {
            before(done => {
                expirationRule.Prefix = null;
                done();
            });

            it('should allow config with empty Filter', done => {
                const params = getLifecycleParams({ key: 'Filter', value: {} });
                s3.putBucketLifecycleConfiguration(params, err =>
                    assertError(err, null, done));
            });

            it('should not allow config with And & Prefix', done => {
                const params = getLifecycleParams(
                    { key: 'Filter', value: { Prefix: 'foo', And: {} } });
                s3.putBucketLifecycleConfiguration(params, err =>
                    assertError(err, 'MalformedXML', done));
            });

            it('should not allow config with And & Tag', done => {
                const params = getLifecycleParams({
                    key: 'Filter',
                    value: { Tag: { Key: 'foo', Value: 'bar' }, And: {} },
                });
                s3.putBucketLifecycleConfiguration(params, err =>
                    assertError(err, 'MalformedXML', done));
            });

            it('should not allow config with Prefix & Tag', done => {
                const params = getLifecycleParams({
                    key: 'Filter',
                    value: { Tag: { Key: 'foo', Value: 'bar' }, Prefix: 'foo' },
                });
                s3.putBucketLifecycleConfiguration(params, err =>
                    assertError(err, 'MalformedXML', done));
            });

            it('should allow config with only Prefix', done => {
                const params = getLifecycleParams(
                    { key: 'Filter', value: { Prefix: 'foo' } });
                s3.putBucketLifecycleConfiguration(params, err =>
                    assertError(err, null, done));
            });

            it('should allow config with only Tag', done => {
                const params = getLifecycleParams({
                    key: 'Filter',
                    value: { Tag: { Key: 'foo', Value: 'ba' } },
                });
                s3.putBucketLifecycleConfiguration(params, err =>
                    assertError(err, null, done));
            });

            it('should not allow config with And.Prefix & no And.Tags',
            done => {
                const params = getLifecycleParams(
                    { key: 'Filter', value: { And: { Prefix: 'foo' } } });
                s3.putBucketLifecycleConfiguration(params, err =>
                    assertError(err, 'MalformedXML', done));
            });

            it('should not allow config with only one And.Tags', done => {
                const params = getLifecycleParams({
                    key: 'Filter',
                    value: { And: { Tags: [{ Key: 'f', Value: 'b' }] } },
                });
                s3.putBucketLifecycleConfiguration(params, err =>
                    assertError(err, 'MalformedXML', done));
            });

            it('should allow config with And.Tags & no And.Prefix',
            done => {
                const params = getLifecycleParams({
                    key: 'Filter',
                    value: { And: { Tags:
                        [{ Key: 'foo', Value: 'bar' },
                        { Key: 'foo2', Value: 'bar2' }],
                    } },
                });
                s3.putBucketLifecycleConfiguration(params, err =>
                    assertError(err, null, done));
            });

            it('should allow config with And.Prefix & And.Tags', done => {
                const params = getLifecycleParams({
                    key: 'Filter',
                    value: { And: { Prefix: 'foo',
                        Tags: [
                            { Key: 'foo', Value: 'bar' },
                            { Key: 'foo2', Value: 'bar2' }],
                    } },
                });
                s3.putBucketLifecycleConfiguration(params, err =>
                    assertError(err, null, done));
            });
        });

        describe('with NoncurrentVersionTransitions', () => {
            // Get lifecycle request params with NoncurrentVersionTransitions.
            function getParams(noncurrentVersionTransitions) {
                const rule = {
                    ID: 'test',
                    Status: 'Enabled',
                    Prefix: '',
                    NoncurrentVersionTransitions: noncurrentVersionTransitions,
                };
                return {
                    Bucket: bucket,
                    LifecycleConfiguration: { Rules: [rule] },
                };
            }

            it('should allow NoncurrentDays and StorageClass', done => {
                const noncurrentVersionTransitions = [{
                    NoncurrentDays: 0,
                    StorageClass: 'us-east-2',
                }];
                const params = getParams(noncurrentVersionTransitions);
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.ifError(err);
                    done();
                });
            });

            it('should not allow duplicate StorageClass', done => {
                const noncurrentVersionTransitions = [{
                    NoncurrentDays: 1,
                    StorageClass: 'us-east-2',
                }, {
                    NoncurrentDays: 2,
                    StorageClass: 'us-east-2',
                }];
                const params = getParams(noncurrentVersionTransitions);
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.code, 'InvalidRequest');
                    assert.strictEqual(err.message,
                    "'StorageClass' must be different for " +
                    "'NoncurrentVersionTransition' actions in same " +
                    "'Rule' with prefix ''");
                    done();
                });
            });

            it('should not allow unknown StorageClass',
            done => {
                const noncurrentVersionTransitions = [{
                    NoncurrentDays: 1,
                    StorageClass: 'unknown',
                }];
                const params = getParams(noncurrentVersionTransitions);
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.code, 'MalformedXML');
                    done();
                });
            });

            it(`should not allow NoncurrentDays value exceeding ${MAX_DAYS}`,
            done => {
                const noncurrentVersionTransitions = [{
                    NoncurrentDays: MAX_DAYS + 1,
                    StorageClass: 'us-east-2',
                }];
                const params = getParams(noncurrentVersionTransitions);
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.code, 'MalformedXML');
                    done();
                });
            });

            it('should not allow negative NoncurrentDays',
            done => {
                const noncurrentVersionTransitions = [{
                    NoncurrentDays: -1,
                    StorageClass: 'us-east-2',
                }];
                const params = getParams(noncurrentVersionTransitions);
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.code, 'InvalidArgument');
                    assert.strictEqual(err.message,
                    "'NoncurrentDays' in NoncurrentVersionTransition " +
                    'action must be nonnegative');
                    done();
                });
            });

            it('should not allow config missing NoncurrentDays',
            done => {
                const noncurrentVersionTransitions = [{
                    StorageClass: 'us-east-2',
                }];
                const params = getParams(noncurrentVersionTransitions);
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.code, 'MalformedXML');
                    done();
                });
            });

            it('should not allow config missing StorageClass',
            done => {
                const noncurrentVersionTransitions = [{
                    NoncurrentDays: 1,
                }];
                const params = getParams(noncurrentVersionTransitions);
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.code, 'MalformedXML');
                    done();
                });
            });
        });

        describe('with Transitions', () => {
            // Get lifecycle request params with Transitions.
            function getParams(transitions) {
                const rule = {
                    ID: 'test',
                    Status: 'Enabled',
                    Prefix: '',
                    Transitions: transitions,
                };
                return {
                    Bucket: bucket,
                    LifecycleConfiguration: { Rules: [rule] },
                };
            }

            it('should allow Days', done => {
                const transitions = [{
                    Days: 0,
                    StorageClass: 'us-east-2',
                }];
                const params = getParams(transitions);
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.ifError(err);
                    done();
                });
            });

            it(`should not allow Days value exceeding ${MAX_DAYS}`, done => {
                const transitions = [{
                    Days: MAX_DAYS + 1,
                    StorageClass: 'us-east-2',
                }];
                const params = getParams(transitions);
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.code, 'MalformedXML');
                    done();
                });
            });

            it('should not allow negative Days value', done => {
                const transitions = [{
                    Days: -1,
                    StorageClass: 'us-east-2',
                }];
                const params = getParams(transitions);
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.code, 'InvalidArgument');
                    assert.strictEqual(err.message,
                        "'Days' in Transition action must be nonnegative");
                    done();
                });
            });

            it('should not allow duplicate StorageClass', done => {
                const transitions = [{
                    Days: 1,
                    StorageClass: 'us-east-2',
                }, {
                    Days: 2,
                    StorageClass: 'us-east-2',
                }];
                const params = getParams(transitions);
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.code, 'InvalidRequest');
                    assert.strictEqual(err.message,
                        "'StorageClass' must be different for 'Transition' " +
                        "actions in same 'Rule' with prefix ''");
                    done();
                });
            });

            // TODO: Upgrade to aws-sdk >= 2.60.0 for correct Date field support
            it.skip('should allow Date', done => {
                const transitions = [{
                    Date: '2016-01-01T00:00:00.000Z',
                    StorageClass: 'us-east-2',
                }];
                const params = getParams(transitions);
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.ifError(err);
                    done();
                });
            });

            // TODO: Upgrade to aws-sdk >= 2.60.0 for correct Date field support
            it.skip('should not allow speficying both Days and Date value',
            done => {
                const transitions = [{
                    Date: '2016-01-01T00:00:00.000Z',
                    Days: 1,
                    StorageClass: 'us-east-2',
                }];
                const params = getParams(transitions);
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.code, 'MalformedXML');
                    done();
                });
            });

            it('should not allow speficying both Days and Date value across ' +
            'transitions', done => {
                const transitions = [{
                    Date: '2016-01-01T00:00:00.000Z',
                    StorageClass: 'us-east-2',
                }, {
                    Days: 1,
                    StorageClass: 'zenko',
                }];
                const params = getParams(transitions);
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.code, 'InvalidRequest');
                    assert.strictEqual(err.message,
                        "Found mixed 'Date' and 'Days' based Transition " +
                        "actions in lifecycle rule for prefix ''");
                    done();
                });
            });

            it('should not allow speficying both Days and Date value across ' +
            'transitions and expiration', done => {
                const transitions = [{
                    Days: 1,
                    StorageClass: 'us-east-2',
                }];
                const params = getParams(transitions);
                params.LifecycleConfiguration.Rules[0].Expiration = { Date: 0 };
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.code, 'InvalidRequest');
                    assert.strictEqual(err.message,
                        "Found mixed 'Date' and 'Days' based Expiration and " +
                        "Transition actions in lifecycle rule for prefix ''");
                    done();
                });
            });
        });

        describe('with NoncurrentVersionTransitions and Transitions', () => {
            it('should allow config', done => {
                const params = {
                    Bucket: bucket,
                    LifecycleConfiguration: {
                        Rules: [{
                            ID: 'test',
                            Status: 'Enabled',
                            Prefix: '',
                            NoncurrentVersionTransitions: [{
                                NoncurrentDays: 1,
                                StorageClass: 'us-east-2',
                            }],
                            Transitions: [{
                                Days: 1,
                                StorageClass: 'us-east-2',
                            }],
                        }],
                    },
                };
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.ifError(err);
                    done();
                });
            });
        });
    });
});
