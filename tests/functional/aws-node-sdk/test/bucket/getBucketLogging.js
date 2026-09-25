const assert = require('assert');
const { CreateBucketCommand, GetBucketLoggingCommand, PutBucketLoggingCommand } = require('@aws-sdk/client-s3');

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

function ignoreNoSuchBucket(err) {
    if (err.name !== 'NoSuchBucket') {
        throw err;
    }
}

function cleanUp(bucketUtil) {
    return Promise.all([
        bucketUtil.deleteOne(bucketName).catch(ignoreNoSuchBucket),
        bucketUtil.deleteOne(targetBucket).catch(ignoreNoSuchBucket),
    ]);
}

describe('GET bucket logging', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        after(() => cleanUp(bucketUtil));

        describe('without existing bucket', () => {
            afterEach(() => cleanUp(bucketUtil));

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
            afterEach(() => cleanUp(bucketUtil));

            beforeEach(done => {
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
            afterEach(() => cleanUp(bucketUtil));

            beforeEach(done => {
                process.stdout.write('Creating buckets and setting logging\n');
                s3.send(new CreateBucketCommand({ Bucket: bucketName }))
                    .then(() => s3.send(new CreateBucketCommand({ Bucket: targetBucket })))
                    .then(() =>
                        s3.send(
                            new PutBucketLoggingCommand({
                                Bucket: bucketName,
                                BucketLoggingStatus: validLoggingConfig,
                            }),
                        ),
                    )
                    .then(() => done(), done);
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
