const assert = require('assert');
const { errors } = require('arsenal');
const { S3 } = require('aws-sdk');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'lifecycledeletetestbucket';
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
        expect(err).toBe(null);
    } else {
        expect(err.code).toBe(expectedErr);
        expect(err.statusCode).toBe(errors[expectedErr].code);
    }
    cb();
}

describe('aws-sdk test delete bucket lifecycle', () => {
    let s3;
    let otherAccountS3;

    beforeAll(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return done();
    });

    test('should return NoSuchBucket error if bucket does not exist', done => {
        s3.deleteBucketLifecycle({ Bucket: bucket }, err =>
            assertError(err, 'NoSuchBucket', done));
    });

    describe('config rules', () => {
        beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        test('should return AccessDenied if user is not bucket owner', done => {
            otherAccountS3.deleteBucketLifecycle({ Bucket: bucket },
            err => assertError(err, 'AccessDenied', done));
        });

        test('should return no error if no lifecycle config on bucket', done => {
            s3.deleteBucketLifecycle({ Bucket: bucket }, err =>
                assertError(err, null, done));
        });

        test('should delete lifecycle configuration from bucket', done => {
            const params = { Bucket: bucket,
                LifecycleConfiguration: { Rules: [basicRule] } };
            s3.putBucketLifecycleConfiguration(params, err => {
                expect(err).toEqual(null);
                s3.deleteBucketLifecycle({ Bucket: bucket }, err => {
                    expect(err).toEqual(null);
                    s3.getBucketLifecycleConfiguration({ Bucket: bucket },
                    err =>
                        assertError(err, 'NoSuchLifecycleConfiguration', done));
                });
            });
        });
    });
});
