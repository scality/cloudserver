const assert = require('assert');
const { S3 } = require('aws-sdk');

const checkError = require('../../lib/utility/checkError');
const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'mock-bucket';

const objectLockConfig = {
    ObjectLockEnabled: 'Enabled',
    Rule: {
        DefaultRetention: {
            Mode: 'GOVERNANCE',
            Days: 30,
        },
    },
};

describe('aws-sdk test get bucket object lock', () => {
    let s3;
    let otherAccountS3;

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return done();
    });

    it('should return NoSuchBucket error if bucket does not exist', done => {
        s3.getObjectLockConfiguration({ Bucket: bucket }, err => {
            checkError(err, 'NoSuchBucket', 404);
            done();
        });
    });

    describe('request to object lock disabled bucket', () => {
        beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        it('should return ObjectLockConfigurationNotFoundError', done => {
            s3.getObjectLockConfiguration({ Bucket: bucket }, err => {
                checkError(err, 'ObjectLockConfigurationNotFoundError', 404);
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
            otherAccountS3.getObjectLockConfiguration({ Bucket: bucket }, err => {
                checkError(err, 'AccessDenied', 403);
                done();
            });
        });

        it('should get bucket object lock config', done => {
            s3.putObjectLockConfiguration({
                Bucket: bucket,
                ObjectLockConfiguration: objectLockConfig,
            }, err => {
                assert.ifError(err);
                s3.getObjectLockConfiguration({ Bucket: bucket }, (err, res) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(res, {
                        ObjectLockConfiguration: objectLockConfig,
                    });
                    done();
                });
            });
        });
    });
});
