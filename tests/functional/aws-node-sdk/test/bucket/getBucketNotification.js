const assert = require('assert');
const { errors } = require('arsenal');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    GetBucketNotificationConfigurationCommand,
    PutBucketNotificationConfigurationCommand,
} = require('@aws-sdk/client-s3');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'notificationtestbucket';
const notificationConfig = {
    QueueConfigurations: [
        {
            Events: ['s3:ObjectCreated:*'],
            QueueArn: 'arn:scality:bucketnotif:::target1',
            Id: 'test-id',
        },
    ],
};

// Check for the expected error response code and status code.
function assertError(err, expectedErr) {
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        assert.strictEqual(err.name, expectedErr);
        assert.strictEqual(err.$metadata.httpStatusCode, errors[expectedErr].code);
    }
}

describe('aws-sdk test get bucket notification', () => {
    let s3;
    let otherAccountS3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
    });

    it('should return NoSuchBucket error if bucket does not exist', async () => {
        try {
            await s3.send(new GetBucketNotificationConfigurationCommand({ Bucket: bucket }));
            throw new Error('Expected NoSuchBucket error');
        } catch (err) {
            assertError(err, 'NoSuchBucket');
        }
    });

    describe('config rules', () => {
        beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: bucket })));

        afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

        it('should return AccessDenied if user is not bucket owner', async () => {
            try {
                await otherAccountS3.send(new GetBucketNotificationConfigurationCommand({ Bucket: bucket }));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                assertError(err, 'AccessDenied');
            }
        });

        it('should not return an error if no notification configuration ' + 'put to bucket', () =>
            s3.send(new GetBucketNotificationConfigurationCommand({ Bucket: bucket })),
        );

        it('should get bucket notification config', async () => {
            await s3.send(
                new PutBucketNotificationConfigurationCommand({
                    Bucket: bucket,
                    NotificationConfiguration: notificationConfig,
                }),
            );
            const res = await s3.send(new GetBucketNotificationConfigurationCommand({ Bucket: bucket }));
            assert.deepStrictEqual(res.QueueConfigurations, notificationConfig.QueueConfigurations);
        });
    });
});
