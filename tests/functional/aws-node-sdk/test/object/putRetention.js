const assert = require('assert');
const moment = require('moment');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const checkError = require('../../lib/utility/checkError');
const changeObjectLock = require('../../lib/utility/objectLock-util');

const bucketName = 'lockenabledputbucket';
const unlockedBucket = 'locknotenabledputbucket';
const objectName = 'putobjectretentionobject';

const retentionConfig = {
    Mode: 'GOVERNANCE',
    RetainUntilDate: moment().add(1, 'Days').toISOString(),
};

describe('PUT object retention', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;
        let versionId;

        beforeEach(() => {
            process.stdout.write('Putting buckets and objects\n');
            return s3.createBucketPromise(
                { Bucket: bucketName, ObjectLockEnabledForBucket: true })
            .then(() => s3.createBucketPromise({ Bucket: unlockedBucket }))
            .then(() => s3.putObjectPromise({ Bucket: unlockedBucket, Key: objectName }))
            .then(() => s3.putObjectPromise({ Bucket: bucketName, Key: objectName }))
            .then(res => {
                versionId = res.VersionId;
            })
            .catch(err => {
                process.stdout.write('Error in beforeEach\n');
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying and deleting buckets\n');
            return bucketUtil.empty(bucketName)
            .then(() => bucketUtil.empty(unlockedBucket))
            .then(() => bucketUtil.deleteMany([bucketName, unlockedBucket]))
            .catch(err => {
                process.stdout.write('Error in afterEach\n');
                throw err;
            });
        });

        it('should return AccessDenied putting retention with another account',
        done => {
            otherAccountS3.putObjectRetention({
                Bucket: bucketName,
                Key: objectName,
                Retention: retentionConfig,
            }, err => {
                checkError(err, 'AccessDenied', 403);
                done();
            });
        });

        it('should return NoSuchKey error if key does not exist', done => {
            s3.putObjectRetention({
                Bucket: bucketName,
                Key: 'thiskeydoesnotexist',
                Retention: retentionConfig,
            }, err => {
                checkError(err, 'NoSuchKey', 404);
                done();
            });
        });

        it('should return NoSuchVersion error if version does not exist', done => {
            s3.putObjectRetention({
                Bucket: bucketName,
                Key: objectName,
                VersionId: '000000000000',
                Retention: retentionConfig,
            }, err => {
                checkError(err, 'NoSuchVersion', 404);
                done();
            });
        });

        it('should return InvalidRequest error putting retention to object ' +
        'in bucket with no object lock enabled', done => {
            s3.putObjectRetention({
                Bucket: unlockedBucket,
                Key: objectName,
                Retention: retentionConfig,
            }, err => {
                checkError(err, 'InvalidRequest', 400);
                done();
            });
        });

        it('should return MethodNotAllowed if object version is delete marker',
        done => {
            s3.deleteObject({ Bucket: bucketName, Key: objectName }, err => {
                assert.ifError(err);
                s3.putObjectRetention({
                    Bucket: bucketName,
                    Key: objectName,
                    Retention: retentionConfig,
                }, err => {
                    checkError(err, 'MethodNotAllowed', 405);
                    done();
                });
            });
        });

        it('should put object retention', done => {
            s3.putObjectRetention({
                Bucket: bucketName,
                Key: objectName,
                Retention: retentionConfig,
            }, err => {
                assert.ifError(err);
                changeObjectLock([
                    { bucket: bucketName, key: objectName, versionId }], '', done);
            });
        });
    });
});
