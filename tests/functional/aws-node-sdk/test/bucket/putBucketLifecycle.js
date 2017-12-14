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
    });
});
