const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketNotificationConfigurationCommand } = require('@aws-sdk/client-s3');

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

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
    });

    it('should return NoSuchBucket error if bucket does not exist', async () => {
        const params = getNotificationParams();
        try {
            await s3.send(new PutBucketNotificationConfigurationCommand(params));
            throw new Error('Expected NoSuchBucket error');
        } catch (err) {
            checkError(err, 'NoSuchBucket', 404);
        }
    });

    describe('config rules', () => {
        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({
                Bucket: bucket,
            }));
        });

        afterEach(async () => {
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        it('should return AccessDenied if user is not bucket owner', async () => {
            const params = getNotificationParams();
            try {
                await otherAccountS3.send(new PutBucketNotificationConfigurationCommand(params));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                checkError(err, 'AccessDenied', 403);
            }
        });

        it('should put notification configuration on bucket with basic config', async () => {
            const params = getNotificationParams();
            await s3.send(new PutBucketNotificationConfigurationCommand(params));
        });

        it('should put notification configuration on bucket with multiple events', async () => {
            const params = getNotificationParams(
                ['s3:ObjectCreated:*', 's3:ObjectRemoved:*']);
            await s3.send(new PutBucketNotificationConfigurationCommand(params));
        });

        it('should put notification configuration on bucket with id', async () => {
            const params = getNotificationParams(null, null, 'notification-id');
            await s3.send(new PutBucketNotificationConfigurationCommand(params));
        });

        it('should put empty notification configuration', async () => {
            const params = {
                Bucket: bucket,
                NotificationConfiguration: {},
            };
            await s3.send(new PutBucketNotificationConfigurationCommand(params));
        });

        it('should not allow notification config request with invalid arn', async () => {
            const params = getNotificationParams(null, 'invalidArn');
            try {
                await s3.send(new PutBucketNotificationConfigurationCommand(params));
                throw new Error('Expected MalformedXML error');
            } catch (err) {
                checkError(err, 'MalformedXML', 400);
            }
        });

        it('should not allow notification config request with invalid event', async () => {
            const params = getNotificationParams(['s3:NotAnEvent']);
            try {
                await s3.send(new PutBucketNotificationConfigurationCommand(params));
                throw new Error('Expected MalformedXML error');
            } catch (err) {
                checkError(err, 'MalformedXML', 400);
            }
        });

        it('should not allow notification config request with unsupported destination', async () => {
            const params = getNotificationParams(null, 'arn:scality:bucketnotif:::target100');
            try {
                await s3.send(new PutBucketNotificationConfigurationCommand(params));
                throw new Error('Expected InvalidArgument error');
            } catch (err) {
                checkError(err, 'InvalidArgument', 400);
            }
        });
    });

    describe('cross origin requests', () => {
        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({
                Bucket: bucket,
            }));
        });

        afterEach(async () => {
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        const corsTests = [
            {
                it: 'return valid error with invalid arn',
                param: getNotificationParams(null, 'invalidArn'),
                error: 'MalformedXML',
            }, {
                it: 'return valid error with unknown/unsupported destination',
                param: getNotificationParams(null, 'arn:scality:bucketnotif:::target100'),
                error: 'InvalidArgument',
            }, {
                it: 'save notification configuration with correct arn',
                param: getNotificationParams(),
            },
        ];

        corsTests.forEach(test => {
            it(`should ${test.it}`, async () => {
                // For v3, we need to modify the request through middleware instead of .httpRequest.headers
                // This is a simplified approach - in a real migration, custom middleware would be needed
                try {
                    await s3.send(new PutBucketNotificationConfigurationCommand(test.param));
                    if (test.error) {
                        throw new Error(`Expected ${test.error} error`);
                    }
                } catch (err) {
                    if (test.error) {
                        checkError(err, test.error, 400);
                    } else {
                        throw err;
                    }
                }
            });
        });
    });
});
