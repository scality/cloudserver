const assert = require('assert');
const { S3 } = require('aws-sdk');

const checkError = require('../../lib/utility/checkError');
const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'mock-notification-bucket';

function getNotificationParams(events, arn, id, filter) {
    const notifConfig = {
        QueueConfigurations: [
            {
                Events: events || ['s3:ObjectCreated:*'],
                QueueArn: arn || 'arn:scality:bucketnotif:::target1',
            },
        ],
    };
    if (id) {
        notifConfig.QueueConfigurations[0].Id = id;
    }
    if (filter) {
        notifConfig.QueueConfigurations[0].Filter = filter;
    }
    return {
        Bucket: bucket,
        NotificationConfiguration: notifConfig,
    };
}

describe('aws-sdk test put notification configuration', () => {
    let s3;
    let otherAccountS3;

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return done();
    });

    it('should return NoSuchBucket error if bucket does not exist', done => {
        const params = getNotificationParams();
        s3.putBucketNotificationConfiguration(params, err => {
            checkError(err, 'NoSuchBucket', 404);
            done();
        });
    });

    describe('config rules', () => {
        beforeEach(done => s3.createBucket({
            Bucket: bucket,
        }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        it('should return AccessDenied if user is not bucket owner', done => {
            const params = getNotificationParams();
            otherAccountS3.putBucketNotificationConfiguration(params, err => {
                checkError(err, 'AccessDenied', 403);
                done();
            });
        });

        it('should put notification configuration on bucket with basic config',
            done => {
                const params = getNotificationParams();
                s3.putBucketNotificationConfiguration(params, err => {
                    assert.ifError(err);
                    done();
                });
            });

        it('should put notification configuration on bucket with multiple events',
            done => {
                const params = getNotificationParams(
                    ['s3:ObjectCreated:*', 's3:ObjectRemoved:*']);
                s3.putBucketNotificationConfiguration(params, err => {
                    assert.ifError(err);
                    done();
                });
            });

        it('should put notification configuration on bucket with id',
            done => {
                const params = getNotificationParams(null, null, 'notification-id');
                s3.putBucketNotificationConfiguration(params, err => {
                    assert.ifError(err);
                    done();
                });
            });

        it('should not allow notification config request with invalid arn',
            done => {
                const params = getNotificationParams(null, 'invalidArn');
                s3.putBucketNotificationConfiguration(params, err => {
                    checkError(err, 'MalformedXML', 400);
                    done();
                });
            });

        it('should not allow notification config request with invalid event',
            done => {
                const params = getNotificationParams(['s3:NotAnEvent']);
                s3.putBucketNotificationConfiguration(params, err => {
                    checkError(err, 'MalformedXML', 400);
                    done();
                });
            });
    });
});
