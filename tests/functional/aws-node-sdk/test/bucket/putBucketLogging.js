const assert = require('assert');

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

        function _testPutBucketLoggingError(account, config, statusCode, errMsg, cb) {
            account.putBucketLogging({
                Bucket: bucketName,
                BucketLoggingStatus: config,
            }, err => {
                assert(err, 'Expected err but found none');
                assert.strictEqual(err.code, errMsg);
                assert.strictEqual(err.statusCode, statusCode);
                cb();
            });
        }

        describe('without existing bucket', () => {
            it('should return NoSuchBucket', done => {
                _testPutBucketLoggingError(s3, validLoggingConfig, 404, 'NoSuchBucket', done);
            });
        });

        describe('with existing bucket', () => {
            beforeEach(done => {
                process.stdout.write('Creating buckets\n');
                return s3.createBucket({ Bucket: bucketName }, err => {
                    if (err) {
                        return done(err);
                    }
                    return s3.createBucket({ Bucket: targetBucket }, done);
                });
            });

            afterEach(done => {
                process.stdout.write('Deleting buckets\n');
                bucketUtil.deleteOne(bucketName).then(() => bucketUtil.deleteOne(targetBucket)).then(() => done())
                    .catch(err => {
                        if (err && err.code !== 'NoSuchBucket') {
                            return done(err);
                        }
                        return done();
                    });
            });

            it('should put bucket logging configuration successfully', done => {
                s3.putBucketLogging({
                    Bucket: bucketName,
                    BucketLoggingStatus: validLoggingConfig,
                }, err => {
                    assert.ifError(err);
                    // Verify the config was set by getting it back
                    s3.getBucketLogging({ Bucket: bucketName }, (err, data) => {
                        assert.ifError(err);
                        assert(data.LoggingEnabled);
                        assert.strictEqual(data.LoggingEnabled.TargetBucket,
                            targetBucket);
                        assert.strictEqual(data.LoggingEnabled.TargetPrefix,
                            'logs/');
                        return done();
                    });
                });
            });

            it('should return NotImplemented if TargetGrants is present', done => {
                _testPutBucketLoggingError(s3, validLoggingConfigWithGrants, 501, 'NotImplemented', done);
            });

            it('should disable logging with empty BucketLoggingStatus', done => {
                // First enable logging
                s3.putBucketLogging({
                    Bucket: bucketName,
                    BucketLoggingStatus: validLoggingConfig,
                }, err => {
                    assert.strictEqual(err, null);
                    // Verify it was enabled
                    s3.getBucketLogging({ Bucket: bucketName }, (err, data) => {
                        assert.strictEqual(err, null);
                        assert(data.LoggingEnabled);
                        // Now disable logging
                        s3.putBucketLogging({
                            Bucket: bucketName,
                            BucketLoggingStatus: {},
                        }, err => {
                            assert.strictEqual(err, null,
                                `Found unexpected err ${err}`);
                            // Verify it was disabled
                            s3.getBucketLogging({ Bucket: bucketName },
                                (err, data) => {
                                    assert.strictEqual(err, null);
                                    assert(data);
                                    assert.deepStrictEqual(data, {});
                                    return done();
                                });
                        });
                    });
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
                    return _testPutBucketLoggingError(s3, invalidConfig, 400, 'InvalidTargetBucketForLogging', done);
                });
        });
    });
});

