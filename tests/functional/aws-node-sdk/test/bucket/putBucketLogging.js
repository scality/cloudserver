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
            beforeEach(done => {
                process.stdout.write('Creating buckets\n');
                s3.send(new CreateBucketCommand({ Bucket: bucketName }))
                    .then(() => s3.send(new CreateBucketCommand({ Bucket: targetBucket })))
                    .then(() => done())
                    .catch(done);
            });

            afterEach(done => {
                process.stdout.write('Deleting buckets\n');
                bucketUtil.deleteOne(bucketName)
                    .then(() => bucketUtil.deleteOne(targetBucket))
                    .then(() => done())
                    .catch(err => {
                        if (err && err.name !== 'NoSuchBucket') {
                            return done(err);
                        }
                        return done();
                    });
            });

            it('should put bucket logging configuration successfully', done => {
                s3.send(new PutBucketLoggingCommand({
                    Bucket: bucketName,
                    BucketLoggingStatus: validLoggingConfig,
                }))
                    .then(() => 
                        // Verify the config was set by getting it back
                         s3.send(new GetBucketLoggingCommand({ Bucket: bucketName }))
                    )
                    .then(data => {
                        assert(data.LoggingEnabled);
                        assert.strictEqual(data.LoggingEnabled.TargetBucket,
                            targetBucket);
                        assert.strictEqual(data.LoggingEnabled.TargetPrefix,
                            'logs/');
                        done();
                    })
                    .catch(done);
            });

            it('should return NotImplemented if TargetGrants is present', done => {
                _testPutBucketLoggingError(s3, validLoggingConfigWithGrants, 501, 'NotImplemented', done);
            });

            it('should disable logging with empty BucketLoggingStatus', done => {
                // First enable logging
                s3.send(new PutBucketLoggingCommand({
                    Bucket: bucketName,
                    BucketLoggingStatus: validLoggingConfig,
                }))
                    .then(() => 
                        // Verify it was enabled
                         s3.send(new GetBucketLoggingCommand({ Bucket: bucketName }))
                    )
                    .then(data => {
                        assert(data.LoggingEnabled);
                        // Now disable logging
                        return s3.send(new PutBucketLoggingCommand({
                            Bucket: bucketName,
                            BucketLoggingStatus: {},
                        }));
                    })
                    .then(() => 
                        // Verify it was disabled
                         s3.send(new GetBucketLoggingCommand({ Bucket: bucketName }))
                    )
                    .then(data => {
                        assert(data);
                        assert.deepStrictEqual(data.$metadata.httpStatusCode, 200);
                        done();
                    })
                    .catch(err => {
                        done(err);
                    });
            });

            it('should return MethodNotAllowed if user is not bucket owner', done => {
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
        });
    });
});
