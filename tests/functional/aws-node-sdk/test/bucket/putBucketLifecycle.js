const assert = require('assert');
const { errors } = require('arsenal');
const { S3 } = require('aws-sdk');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'lifecycleputtestbucket';
const basicRule = {
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
        Rules: [basicRule],
    };
    if (paramToChange) {
        newParam[paramToChange.key] = paramToChange.value;
        lifecycleConfig.Rules[0] = Object.assign({}, basicRule, newParam);
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

        it('should not allow lifecycle configuration with duplicated rule id ' +
        'and with Origin header set', done => {
            const origin = 'http://www.allowedwebsite.com';

            const lifecycleConfig = {
                Rules: [basicRule, basicRule],
            };
            const params = {
                Bucket: bucket,
                LifecycleConfiguration: lifecycleConfig,
            };
            const request = s3.putBucketLifecycleConfiguration(params);
            // modify underlying http request object created by aws sdk to add
            // origin header
            request.on('build', () => {
                request.httpRequest.headers.origin = origin;
            });
            request.on('success', response => {
                assert(!response, 'expected error but got success');
                return done();
            });
            request.on('error', err => {
                assertError(err, 'InvalidRequest', done);
            });
            request.send();
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

        it('should allow lifecycle config with Prefix length < 1024', done => {
            const params =
                getLifecycleParams({ key: 'Prefix', value: 'a'.repeat(1023) });
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, null, done));
        });

        it('should allow lifecycle config with Prefix length === 1024',
        done => {
            const params =
                getLifecycleParams({ key: 'Prefix', value: 'a'.repeat(1024) });
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, null, done));
        });

        it('should not allow lifecycle config with Prefix length > 1024',
        done => {
            const params =
                getLifecycleParams({ key: 'Prefix', value: 'a'.repeat(1025) });
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, 'InvalidRequest', done));
        });

        it('should not allow lifecycle config with Filter.Prefix length > 1024',
        done => {
            const params = getLifecycleParams({
                key: 'Filter',
                value: { Prefix: 'a'.repeat(1025) },
            });
            delete params.LifecycleConfiguration.Rules[0].Prefix;
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, 'InvalidRequest', done));
        });

        it('should not allow lifecycle config with Filter.And.Prefix length ' +
        '> 1024', done => {
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
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, 'InvalidRequest', done));
        });

        it('should allow lifecycle config with Tag.Key length < 128', done => {
            const params = getLifecycleParams({
                key: 'Filter',
                value: { Tag: { Key: 'a'.repeat(127), Value: 'bar' } },
            });
            delete params.LifecycleConfiguration.Rules[0].Prefix;
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, null, done));
        });

        it('should allow lifecycle config with Tag.Key length === 128',
        done => {
            const params = getLifecycleParams({
                key: 'Filter',
                value: { Tag: { Key: 'a'.repeat(128), Value: 'bar' } },
            });
            delete params.LifecycleConfiguration.Rules[0].Prefix;
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, null, done));
        });

        it('should not allow lifecycle config with Tag.Key length > 128',
        done => {
            const params = getLifecycleParams({
                key: 'Filter',
                value: { Tag: { Key: 'a'.repeat(129), Value: 'bar' } },
            });
            delete params.LifecycleConfiguration.Rules[0].Prefix;
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, 'InvalidRequest', done));
        });

        it('should allow lifecycle config with Tag.Value length < 256',
        done => {
            const params = getLifecycleParams({
                key: 'Filter',
                value: { Tag: { Key: 'a', Value: 'b'.repeat(255) } },
            });
            delete params.LifecycleConfiguration.Rules[0].Prefix;
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, null, done));
        });

        it('should allow lifecycle config with Tag.Value length === 256',
        done => {
            const params = getLifecycleParams({
                key: 'Filter',
                value: { Tag: { Key: 'a', Value: 'b'.repeat(256) } },
            });
            delete params.LifecycleConfiguration.Rules[0].Prefix;
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, null, done));
        });

        it('should not allow lifecycle config with Tag.Value length > 256',
        done => {
            const params = getLifecycleParams({
                key: 'Filter',
                value: { Tag: { Key: 'a', Value: 'b'.repeat(257) } },
            });
            delete params.LifecycleConfiguration.Rules[0].Prefix;
            s3.putBucketLifecycleConfiguration(params, err =>
                assertError(err, 'InvalidRequest', done));
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
                basicRule.Prefix = null;
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
            it('should return NotImplemented if NoncurrentVersionTransitions rule', done => {
                const params = {
                    Bucket: bucket,
                    LifecycleConfiguration: {
                        Rules: [{
                            ID: 'test',
                            Status: 'Enabled',
                            Prefix: '',
                            NoncurrentVersionTransitions: [{
                                NoncurrentDays: 2,
                                StorageClass: 'us-east-2',
                            }],
                        }],
                    },
                };
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.statusCode, 501);
                    assert.strictEqual(err.code, 'NotImplemented');
                    done();
                });
            });

            it('should return NotImplemented if rules include NoncurrentVersionTransitions', done => {
                const params = {
                    Bucket: bucket,
                    LifecycleConfiguration: {
                        Rules: [{
                            ID: 'id2',
                            Status: 'Enabled',
                            Prefix: '',
                            Expiration: {
                                Days: 1,
                            },
                        }, {
                            ID: 'id1',
                            Status: 'Enabled',
                            Prefix: '',
                            NoncurrentVersionTransitions: [{
                                NoncurrentDays: 2,
                                StorageClass: 'us-east-2',
                            }],
                        }],
                    },
                };
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.statusCode, 501);
                    assert.strictEqual(err.code, 'NotImplemented');
                    done();
                });
            });
        });

        describe('with Transitions', () => {
            it('should return NotImplemented if Transitions rule', done => {
                const params = {
                    Bucket: bucket,
                    LifecycleConfiguration: {
                        Rules: [{
                            ID: 'test',
                            Status: 'Enabled',
                            Prefix: '',
                            Transitions: [{
                                Days: 2,
                                StorageClass: 'us-east-2',
                            }],
                        }],
                    },
                };
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.statusCode, 501);
                    assert.strictEqual(err.code, 'NotImplemented');
                    done();
                });
            });

            it('should return NotImplemented if rules include Transitions', done => {
                const params = {
                    Bucket: bucket,
                    LifecycleConfiguration: {
                        Rules: [{
                            ID: 'id2',
                            Status: 'Enabled',
                            Prefix: '',
                            Expiration: {
                                Days: 1,
                            },
                        }, {
                            ID: 'id1',
                            Status: 'Enabled',
                            Prefix: '',
                            Transitions: [{
                                Days: 2,
                                StorageClass: 'us-east-2',
                            }],
                        }],
                    },
                };
                s3.putBucketLifecycleConfiguration(params, err => {
                    assert.strictEqual(err.statusCode, 501);
                    assert.strictEqual(err.code, 'NotImplemented');
                    done();
                });
            });
        });
    });
});
