const assert = require('assert');

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

function cleanUp(bucketUtil, cb) {
    Promise.all([
        bucketUtil.deleteOne(bucketName).catch(err => {
            if (err && err.code !== 'NoSuchBucket') {
                throw err;
            }
        }),
        bucketUtil.deleteOne(targetBucket).catch(err => {
            if (err && err.code !== 'NoSuchBucket') {
                throw err;
            }
        }),
    ]).then(() => cb()).catch(err => cb(err));
}

describe('GET bucket logging', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        after(done => { cleanUp(bucketUtil, done); });

        describe('without existing bucket', () => {
            afterEach(done => { cleanUp(bucketUtil, done); });

            it('should return NoSuchBucket', done => {
                s3.getBucketLogging({ Bucket: bucketName }, err => {
                    assert(err);
                    assert.strictEqual(err.code, 'NoSuchBucket');
                    assert.strictEqual(err.statusCode, 404);
                    return done();
                });
            });
        });

        describe('on bucket without logging configuration', () => {
            afterEach(done => { cleanUp(bucketUtil, done); });

            beforeEach(done => {
                process.stdout.write('Creating bucket without logging\n');
                s3.createBucket({ Bucket: bucketName }, err => {
                    if (err) {
                        process.stdout.write('error creating bucket', err);
                        return done(err);
                    }
                    return done();
                });
            });

            it('should return empty BucketLoggingStatus', done => {
                s3.getBucketLogging({ Bucket: bucketName }, (err, data) => {
                    assert.strictEqual(err, null,
                        `Found unexpected err ${err}`);
                    // When no logging is configured, AWS returns empty object
                    assert(data);
                    assert.strictEqual(Object.keys(data).length, 0, 'Expected data to have no keys');
                    return done();
                });
            });
        });

        describe('with existing logging configuration', () => {
            afterEach(done => { cleanUp(bucketUtil, done); });

            beforeEach(done => {
                process.stdout.write('Creating buckets and setting logging\n');
                return s3.createBucket({ Bucket: bucketName }, err => {
                    if (err) {
                        return done(err);
                    }
                    return s3.createBucket({ Bucket: targetBucket }, err => {
                        if (err) {
                            return done(err);
                        }
                        return s3.putBucketLogging({
                            Bucket: bucketName,
                            BucketLoggingStatus: validLoggingConfig,
                        }, done);
                    });
                });
            });

            it('should return bucket logging configuration successfully', done => {
                s3.getBucketLogging({ Bucket: bucketName }, (err, data) => {
                    assert.strictEqual(err, null,
                        `Found unexpected err ${err}`);
                    assert(data.LoggingEnabled);
                    assert.strictEqual(data.LoggingEnabled.TargetBucket,
                        targetBucket);
                    assert.strictEqual(data.LoggingEnabled.TargetPrefix, 'logs/');
                    return done();
                });
            });
        });
    });
});

