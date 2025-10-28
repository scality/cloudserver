const assert = require('assert');
const {
    CreateBucketCommand,
    GetBucketLoggingCommand,
    PutBucketLoggingCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucketName = 'testgetloggingbucket';
const targetBucket = 'testloggingtargetbucket';

const validLoggingConfig = {
    LoggingEnabled: {
        TargetBucket: targetBucket,
        TargetPrefix: 'logs/',
    },
};

describe('GET bucket logging', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

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

        describe('without existing bucket', () => {
            it('should return NoSuchBucket', done => {
                s3.send(new GetBucketLoggingCommand({ Bucket: bucketName }))
                    .then(() => {
                        done(new Error('Expected error but succeeded'));
                    })
                    .catch(err => {
                        assert(err);
                        assert.strictEqual(err.name, 'NoSuchBucket');
                        assert.strictEqual(err.$metadata.httpStatusCode, 404);
                        done();
                    });
            });
        });

        describe('on bucket without logging configuration', () => {
            before(done => {
                process.stdout.write('Creating bucket without logging\n');
                s3.send(new CreateBucketCommand({ Bucket: bucketName }))
                    .then(() => done())
                    .catch(err => {
                        process.stdout.write('error creating bucket', err);
                        done(err);
                    });
            });

            it('should return empty BucketLoggingStatus', done => {
                s3.send(new GetBucketLoggingCommand({ Bucket: bucketName }))
                    .then(data => {
                        // When no logging is configured, AWS returns empty object
                        assert(data);
                        assert.strictEqual(Object.keys(data).length, 1, 'Expected data to have only $metadata key');
                        assert(data.$metadata);
                        done();
                    })
                    .catch(err => {
                        done(err);
                    });
            });
        });

        describe('with existing logging configuration', () => {
            before(done => {
                process.stdout.write('Creating buckets and setting logging\n');
                s3.send(new CreateBucketCommand({ Bucket: bucketName }))
                    .then(() => s3.send(new CreateBucketCommand({ Bucket: targetBucket })))
                    .then(() => s3.send(new PutBucketLoggingCommand({
                        Bucket: bucketName,
                        BucketLoggingStatus: validLoggingConfig,
                    })))
                    .then(() => done())
                    .catch(done);
            });

            it('should return bucket logging configuration successfully', done => {
                s3.send(new GetBucketLoggingCommand({ Bucket: bucketName }))
                    .then(data => {
                        assert(data.LoggingEnabled);
                        assert.strictEqual(data.LoggingEnabled.TargetBucket, targetBucket);
                        assert.strictEqual(data.LoggingEnabled.TargetPrefix, 'logs/');
                        done();
                    })
                    .catch(err => {
                        done(err);
                    });
            });
        });
    });
});
