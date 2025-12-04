const assert = require('assert');
const {
    CreateBucketCommand,
    PutBucketLoggingCommand,
    GetBucketLoggingCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucketName = 'testputloggingbucket';
const targetBucket = 'testloggingtargetbucket';

const validLoggingConfig = {
    LoggingEnabled: {
        TargetBucket: targetBucket,
        TargetPrefix: 'logs/',
    },
};

const validLoggingConfigWithGrants = {
    LoggingEnabled: {
        TargetBucket: targetBucket,
        TargetPrefix: 'access-logs/',
        TargetGrants: [
            {
                Grantee: {
                    Type: 'Group',
                    URI: 'http://acs.amazonaws.com/groups/s3/LogDelivery',
                },
                Permission: 'WRITE',
            },
            {
                Grantee: {
                    Type: 'Group',
                    URI: 'http://acs.amazonaws.com/groups/s3/LogDelivery',
                },
                Permission: 'READ_ACP',
            },
        ],
    },
};

const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

describe('PUT bucket logging', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;

        async function _testPutBucketLoggingError(account, config, statusCode, errMsg, cb) {
            try {
                await account.send(new PutBucketLoggingCommand({
                    Bucket: bucketName,
                    BucketLoggingStatus: config,
                }));
                return cb(new Error('Expected error but found none'));
            } catch (err) {
                assert(err, 'Expected err but found none');
                assert.strictEqual(err.name, errMsg);
                assert.strictEqual(err.$metadata.httpStatusCode, statusCode);
                return cb();
            }
        }

        describe('without existing bucket', () => {
            it('should return NoSuchBucket', done => {
                _testPutBucketLoggingError(s3, validLoggingConfig, 404, 'NoSuchBucket', done);
            });
        });

        describe('with existing bucket', () => {
            beforeEach(async () => {
                process.stdout.write('Creating buckets\n');
                await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
                await s3.send(new CreateBucketCommand({ Bucket: targetBucket }));
            });

            afterEach(async () => {
                process.stdout.write('Deleting buckets\n');
                try {
                    await s3.send(new PutBucketLoggingCommand({
                        Bucket: bucketName,
                        BucketLoggingStatus: {},
                    }));
                } catch (err) {
                    if (err.name !== 'NoSuchBucket' && err.code !== 'NoSuchBucket') {
                        throw err;
                    }
                }

                const bucketsToDelete = [bucketName, targetBucket];
                for (const name of bucketsToDelete) {
                    try {
                        await bucketUtil.empty(name);
                    } catch (err) {
                        if (err.name !== 'NoSuchBucket' && err.code !== 'NoSuchBucket') {
                            throw err;
                        }
                    }

                    try {
                        await bucketUtil.deleteOne(name);
                    } catch (err) {
                        if (err.name !== 'NoSuchBucket' && err.code !== 'NoSuchBucket') {
                            throw err;
                        }
                    }
                }
            });

            it('should put bucket logging configuration successfully', async () => {
                await s3.send(new PutBucketLoggingCommand({
                    Bucket: bucketName,
                    BucketLoggingStatus: validLoggingConfig,
                }));

                const data = await s3.send(new GetBucketLoggingCommand({ Bucket: bucketName }));
                assert(data.LoggingEnabled);
                assert.strictEqual(data.LoggingEnabled.TargetBucket,
                    targetBucket);
                assert.strictEqual(data.LoggingEnabled.TargetPrefix,
                    'logs/');
            });

            itSkipIfAWS('should return NotImplemented if TargetGrants is present', done => {
                _testPutBucketLoggingError(s3, validLoggingConfigWithGrants, 501, 'NotImplemented', done);
            });

            it('should disable logging with empty BucketLoggingStatus', async () => {
                await s3.send(new PutBucketLoggingCommand({
                    Bucket: bucketName,
                    BucketLoggingStatus: validLoggingConfig,
                }));

                const enabled = await s3.send(new GetBucketLoggingCommand({ Bucket: bucketName }));
                assert(enabled.LoggingEnabled);

                await s3.send(new PutBucketLoggingCommand({
                    Bucket: bucketName,
                    BucketLoggingStatus: {},
                }));

                const disabled = await s3.send(new GetBucketLoggingCommand({ Bucket: bucketName }));
                assert(disabled);
                assert.deepStrictEqual(disabled.$metadata.httpStatusCode, 200);
            });

            itSkipIfAWS('should return MethodNotAllowed if user is not bucket owner', done => {
                _testPutBucketLoggingError(otherAccountS3, validLoggingConfig, 405, 'MethodNotAllowed', done);
            });

            it('should return InvalidTargetBucketForLogging if target bucket does not exist',
                done => {
                    const invalidConfig = {
                        LoggingEnabled: {
                            TargetBucket: 'nonexistentbucket',
                            TargetPrefix: 'logs/',
                        },
                    };
                    _testPutBucketLoggingError(s3, invalidConfig, 400, 'InvalidTargetBucketForLogging', done);
                });

            it('should allow logging when target bucket is owned by same account', async () => {
                await s3.send(new PutBucketLoggingCommand({
                    Bucket: bucketName,
                    BucketLoggingStatus: validLoggingConfig,
                }));

                const data = await s3.send(new GetBucketLoggingCommand({ Bucket: bucketName }));
                assert(data.LoggingEnabled);
                assert.strictEqual(data.LoggingEnabled.TargetBucket, targetBucket);
            });
        });

        describe('with cross-account target bucket', () => {
            const otherAccountTargetBucket = 'other-account-target-bucket';

            beforeEach(async () => {
                process.stdout.write('Creating buckets\n');
                await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
                await otherAccountS3.send(new CreateBucketCommand({ Bucket: otherAccountTargetBucket }));
            });

            afterEach(async () => {
                process.stdout.write('Deleting buckets\n');
                const cleanupPlan = [
                    { util: bucketUtil, name: bucketName },
                    { util: otherAccountBucketUtility, name: otherAccountTargetBucket },
                ];

                for (const { util, name } of cleanupPlan) {
                    try {
                        await util.empty(name);
                    } catch (err) {
                        if (err.name !== 'NoSuchBucket') {
                            throw err;
                        }
                    }

                    try {
                        await util.deleteOne(name);
                    } catch (err) {
                        if (err.name !== 'NoSuchBucket') {
                            throw err;
                        }
                    }
                }
            });

            it('should return InvalidTargetBucketForLogging when target bucket is owned by different account', 
                async () => {
                const crossAccountConfig = {
                    LoggingEnabled: {
                        TargetBucket: otherAccountTargetBucket,
                        TargetPrefix: 'logs/',
                    },
                };

                try {
                    await s3.send(new PutBucketLoggingCommand({
                        Bucket: bucketName,
                        BucketLoggingStatus: crossAccountConfig,
                    }));
                    assert.fail('Expected InvalidTargetBucketForLogging error');
                } catch (err) {
                    assert.strictEqual(err.name, 'InvalidTargetBucketForLogging');
                    assert.strictEqual(err.$metadata.httpStatusCode, 400);
                }
            });
        });
    });
});
