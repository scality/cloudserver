const assert = require('assert');
const { S3 } = require('aws-sdk');

const checkError = require('../../lib/utility/checkError');
const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'mock-bucket';

function getObjectLockParams(status, mode, days, years) {
    const objectLockConfig = {
        ObjectLockEnabled: status,
        Rule: {
            DefaultRetention: {
                Mode: mode,
            },
        },
    };
    if (days) {
        objectLockConfig.Rule.DefaultRetention.Days = days;
    }
    if (years) {
        objectLockConfig.Rule.DefaultRetention.Years = years;
    }
    return {
        Bucket: bucket,
        ObjectLockConfiguration: objectLockConfig,
    };
}

describe('aws-sdk test put object lock configuration', () => {
    let s3;
    let otherAccountS3;

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return done();
    });

    it('should return NoSuchBucket error if bucket does not exist', done => {
        const params = getObjectLockParams('Enabled', 'GOVERNANCE', 1);
        s3.putObjectLockConfiguration(params, err => {
            checkError(err, 'NoSuchBucket', 404);
            done();
        });
    });

    describe('on object lock disabled bucket', () => {
        beforeEach(done => s3.createBucket({
            Bucket: bucket,
        }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        it('should return InvalidBucketState error', done => {
            const params = getObjectLockParams('Enabled', 'GOVERNANCE', 1);
            s3.putObjectLockConfiguration(params, err => {
                checkError(err, 'InvalidBucketState', 409);
                done();
            });
        });

        it('should return InvalidBucketState error without Rule', done => {
            const params = {
                Bucket: bucket,
                ObjectLockConfiguration: {
                    ObjectLockEnabled: 'Enabled',
                },
            };
            s3.putObjectLockConfiguration(params, err => {
                checkError(err, 'InvalidBucketState', 409);
                done();
            });
        });
    });

    describe('config rules', () => {
        beforeEach(done => s3.createBucket({
            Bucket: bucket,
            ObjectLockEnabledForBucket: true,
        }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        it('should return AccessDenied if user is not bucket owner', done => {
            const params = getObjectLockParams('Enabled', 'GOVERNANCE', 1);
            otherAccountS3.putObjectLockConfiguration(params, err => {
                checkError(err, 'AccessDenied', 403);
                done();
            });
        });

        it('should put object lock configuration on bucket with Governance mode',
            done => {
                const params = getObjectLockParams('Enabled', 'GOVERNANCE', 30);
                s3.putObjectLockConfiguration(params, err => {
                    assert.ifError(err);
                    done();
                });
            });

        it('should put object lock configuration on bucket with Compliance mode',
            done => {
                const params = getObjectLockParams('Enabled', 'COMPLIANCE', 30);
                s3.putObjectLockConfiguration(params, err => {
                    assert.ifError(err);
                    done();
                });
            });

        it('should put object lock configuration on bucket with year retention type',
            done => {
                const params = getObjectLockParams('Enabled', 'COMPLIANCE', null, 2);
                s3.putObjectLockConfiguration(params, err => {
                    assert.ifError(err);
                    done();
                });
            });

        it('should not allow object lock config request with zero day retention',
            done => {
                const params = getObjectLockParams('Enabled', 'GOVERNANCE', null, 0);
                s3.putObjectLockConfiguration(params, err => {
                    checkError(err, 'MalformedXML', 400);
                    done();
                });
            });

        it('should not allow object lock config request with negative retention',
            done => {
                const params = getObjectLockParams('Enabled', 'GOVERNANCE', -1);
                s3.putObjectLockConfiguration(params, err => {
                    checkError(err, 'InvalidArgument', 400);
                    done();
                });
            });

        it('should not allow object lock config request with both Days and Years',
            done => {
                const params = getObjectLockParams('Enabled', 'GOVERNANCE', 1, 1);
                s3.putObjectLockConfiguration(params, err => {
                    checkError(err, 'MalformedXML', 400);
                    done();
                });
            });

        it('should not allow object lock config request without days or years',
            done => {
                const params = getObjectLockParams('Enabled', 'GOVERNANCE');
                s3.putObjectLockConfiguration(params, err => {
                    checkError(err, 'MalformedXML', 400);
                    done();
                });
            });

        it('should not allow object lock config request with invalid ObjectLockEnabled',
            done => {
                const params = getObjectLockParams('enabled', 'GOVERNANCE', 10);
                s3.putObjectLockConfiguration(params, err => {
                    checkError(err, 'MalformedXML', 400);
                    done();
                });
            });

        it('should not allow object lock config request with invalid mode',
            done => {
                const params = getObjectLockParams('Enabled', 'Governance', 10);
                s3.putObjectLockConfiguration(params, err => {
                    checkError(err, 'MalformedXML', 400);
                    done();
                });
            });
    });
});
