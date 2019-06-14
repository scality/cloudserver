const assert = require('assert');
const { errors } = require('arsenal');
const { S3 } = require('aws-sdk');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'getbucketpolicy-testbucket';
const bucketPolicy = {
    Version: '2012-10-17',
    Statement: [{
        Sid: 'test-id',
        Effect: 'Allow',
        Principle: '*',
        Action: 's3:putBucketPolicy',
        Resource: 'arn:aws:s3::getbucketpolicy-testbucket',
    }],
};
const expectedPolicy = {
    Sid: 'test-id',
    Effect: 'Allow',
    Principle: '*',
    Action: 's3:putBucketPolicy',
    Resource: 'arn:aws:s3::getbucketpolicy-testbucket',
};

// Check for the expected error response code and status code.
function assertError(err, expectedErr, cb) {
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        assert.strictEqual(err.code, expectedErr, 'incorrect error response ' +
            `code: should be '${expectedErr}' but got '${err.code}'`);
        assert.strictEqual(err.statusCode, errors[expectedErr].code,
            'incorrect error status code: should be 400 but got ' +
            `'${err.statusCode}'`);
    }
    cb();
}

const describeSkipUntilImpl =
    process.env.BUCKET_POLICY ? describe : describe.skip;

describeSkipUntilImpl('aws-sdk test get bucket policy', () => {
    let s3;
    let otherAccountS3;

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return done();
    });

    it('should return NoSuchBucket error if bucket does not exist', done => {
        s3.getBucketPolicy({ Bucket: bucket }, err =>
            assertError(err, 'NoSuchBucket', done));
    });

    describe('policy rules', () => {
        beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        it('should return AccessDenied if user is not bucket owner', done => {
            otherAccountS3.getBucketPolicy({ Bucket: bucket },
            err => assertError(err, 'AccessDenied', done));
        });

        it('should return NoSuchBucketPolicy error if no policy put to bucket',
        done => {
            s3.getBucketPolicy({ Bucket: bucket }, err => {
                assertError(err, 'NoSuchBucketPolicy', done);
            });
        });

        it('should get bucket policy', done => {
            s3.putBucketPolicy({
                Bucket: bucket,
                Policy: bucketPolicy,
            }, err => {
                assert.equal(err, null, `Err putting bucket policy: ${err}`);
                s3.getBucketPolicy({ Bucket: bucket },
                (err, res) => {
                    assert.equal(err, null, 'Error getting bucket policy: ' +
                        `${err}`);
                    assert.deepStrictEqual(res.Statement[0], expectedPolicy);
                    done();
                });
            });
        });
    });
});
