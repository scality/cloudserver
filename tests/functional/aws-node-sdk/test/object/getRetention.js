const assert = require('assert');
const Promise = require('bluebird');
const moment = require('moment');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const checkError = require('../../lib/utility/checkError');
const changeObjectLock = require('../../../../utilities/objectLock-util');

const changeLockPromise = Promise.promisify(changeObjectLock);

const bucketName = 'lockenabledbucket';
const unlockedBucket = 'locknotenabledbucket';
const objectName = 'putobjectretentionobject';
const noRetentionObject = 'objectwithnoretention';

const retainDate = moment().add(1, 'days').toISOString();

const retentionConfig = {
    Mode: 'GOVERNANCE',
    RetainUntilDate: retainDate,
};

// aws sdk manipulates dates by removing milliseconds
// and converting date strings to date objects
function manipulateDate() {
    const noMillis = `${retainDate.slice(0, 19)}.000Z`;
    return new Date(noMillis);
}

const expectedConfig = {
    Mode: 'GOVERNANCE',
    RetainUntilDate: manipulateDate(),
};

describe('GET object retention', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;
        let versionId;

        beforeEach(() => {
            process.stdout.write('Putting buckets and objects\n');
            return s3.createBucket(
                { Bucket: bucketName, ObjectLockEnabledForBucket: true }).promise()
            .then(() => s3.createBucket({ Bucket: unlockedBucket }).promise())
            .then(() => s3.putObject({ Bucket: unlockedBucket, Key: objectName }).promise())
            .then(() => s3.putObject({ Bucket: bucketName, Key: noRetentionObject }).promise())
            .then(() => s3.putObject({ Bucket: bucketName, Key: objectName }).promise())
            .then(res => {
                versionId = res.VersionId;
                process.stdout.write('Putting object retention\n');
                return s3.putObjectRetention({
                    Bucket: bucketName,
                    Key: objectName,
                    Retention: retentionConfig,
                }).promise();
            })
            .catch(err => {
                process.stdout.write('Error in beforeEach\n');
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Removing object lock\n');
            return changeLockPromise([{ bucket: bucketName, key: objectName, versionId }], '')
            .then(() => {
                process.stdout.write('Emptying and deleting buckets\n');
                return bucketUtil.empty(bucketName);
            })
            .then(() => bucketUtil.empty(unlockedBucket))
            .then(() => bucketUtil.deleteMany([bucketName, unlockedBucket]))
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            });
        });

        it('should return AccessDenied putting retention with another account',
        done => {
            otherAccountS3.getObjectRetention({
                Bucket: bucketName,
                Key: objectName,
            }, err => {
                checkError(err, 'AccessDenied', 403);
                done();
            });
        });

        it('should return NoSuchKey error if key does not exist', done => {
            s3.getObjectRetention({
                Bucket: bucketName,
                Key: 'thiskeydoesnotexist',
            }, err => {
                checkError(err, 'NoSuchKey', 404);
                done();
            });
        });

        it('should return NoSuchVersion error if version does not exist', done => {
            s3.getObjectRetention({
                Bucket: bucketName,
                Key: objectName,
                VersionId: '000000000000',
            }, err => {
                checkError(err, 'NoSuchVersion', 404);
                done();
            });
        });

        it('should return MethodNotAllowed if object version is delete marker',
        done => {
            s3.deleteObject({ Bucket: bucketName, Key: objectName }, (err, res) => {
                assert.ifError(err);
                s3.getObjectRetention({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: res.VersionId,
                }, err => {
                    checkError(err, 'MethodNotAllowed', 405);
                    done();
                });
            });
        });

        it('should return InvalidRequest error getting retention to object ' +
        'in bucket with no object lock enabled', done => {
            s3.getObjectRetention({
                Bucket: unlockedBucket,
                Key: objectName,
            }, err => {
                checkError(err, 'InvalidRequest', 400);
                done();
            });
        });

        it('should return NoSuchObjectLockConfiguration if no retention set',
        done => {
            s3.getObjectRetention({
                Bucket: bucketName,
                Key: noRetentionObject,
            }, err => {
                checkError(err, 'NoSuchObjectLockConfiguration', 404);
                done();
            });
        });

        it('should get object retention', done => {
            s3.getObjectRetention({
                Bucket: bucketName,
                Key: objectName,
            }, (err, res) => {
                assert.ifError(err);
                assert.deepStrictEqual(res.Retention, expectedConfig);
                changeObjectLock([
                    { bucket: bucketName, key: objectName, versionId }], '', done);
            });
        });
    });
});
