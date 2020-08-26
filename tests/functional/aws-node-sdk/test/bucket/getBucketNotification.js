const assert = require('assert');
const { errors } = require('arsenal');
const { S3 } = require('aws-sdk');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'notificationtestbucket';
const notificationConfig = {
    QueueConfigurations: [{
        Events: ['s3:ObjectCreated:*'],
        QueueArn: 'arn:scality:bucketnotif:::target1',
        Id: 'test-id',
    }],
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

describe('aws-sdk test get bucket notification', () => {
    let s3;
    let otherAccountS3;

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return done();
    });

    it('should return NoSuchBucket error if bucket does not exist', done => {
        s3.getBucketNotificationConfiguration({ Bucket: bucket }, err =>
            assertError(err, 'NoSuchBucket', done));
    });

    describe('config rules', () => {
        beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        it('should return AccessDenied if user is not bucket owner', done => {
            otherAccountS3.getBucketNotificationConfiguration({ Bucket: bucket },
            err => assertError(err, 'AccessDenied', done));
        });

        it('should not return an error if no notification configuration ' +
        'put to bucket', done => {
            s3.getBucketNotificationConfiguration({ Bucket: bucket }, err => {
                assert.ifError(err);
                done();
            });
        });

        it('should get bucket notification config', done => {
            s3.putBucketNotificationConfiguration({
                Bucket: bucket,
                NotificationConfiguration: notificationConfig,
            }, err => {
                assert.equal(err, null, `Err putting notification config: ${err}`);
                s3.getBucketNotificationConfiguration({ Bucket: bucket },
                (err, res) => {
                    assert.equal(err, null, `Error getting notification config: ${err}`);
                    assert.deepStrictEqual(res.NotificationConfiguration, notificationConfig);
                    done();
                });
            });
        });
    });
});
