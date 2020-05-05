const assert = require('assert');
const { S3 } = require('aws-sdk');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');
const assertError = require('../../lib/utility/assertError');

const bucket = 'lifecycletestbucket';
const lifecycleConfig = {
    Rules: [{
        ID: 'test-id',
        Status: 'Enabled',
        Prefix: '',
        Expiration: {
            Days: 1,
        },
    }],
};
const expectedConfig = {
    Expiration: { Days: 1 },
    ID: 'test-id',
    Filter: {},
    Status: 'Enabled',
    Transitions: [],
    NoncurrentVersionTransitions: [],
};

describe('aws-sdk test get bucket lifecycle', () => {
    let s3;
    let otherAccountS3;

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return done();
    });

    it('should return NoSuchBucket error if bucket does not exist', done => {
        s3.getBucketLifecycleConfiguration({ Bucket: bucket }, err =>
            assertError(err, 'NoSuchBucket', done));
    });

    describe('config rules', () => {
        beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        it('should return AccessDenied if user is not bucket owner', done => {
            otherAccountS3.getBucketLifecycleConfiguration({ Bucket: bucket },
            err => assertError(err, 'AccessDenied', done));
        });

        it('should return NoSuchLifecycleConfiguration error if no lifecycle ' +
        'put to bucket', done => {
            s3.getBucketLifecycleConfiguration({ Bucket: bucket }, err => {
                assertError(err, 'NoSuchLifecycleConfiguration', done);
            });
        });

        it('should get bucket lifecycle config', done => {
            s3.putBucketLifecycleConfiguration({
                Bucket: bucket,
                LifecycleConfiguration: lifecycleConfig,
            }, err => {
                assert.equal(err, null, `Err putting lifecycle config: ${err}`);
                s3.getBucketLifecycleConfiguration({ Bucket: bucket },
                (err, res) => {
                    assert.equal(err, null, 'Error getting lifecycle config: ' +
                        `${err}`);
                    assert.deepStrictEqual(res.Rules[0], expectedConfig);
                    done();
                });
            });
        });
    });
});
