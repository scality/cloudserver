const assert = require('assert');
const moment = require('moment');
const {
    CreateBucketCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    PutObjectRetentionCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const checkError = require('../../lib/utility/checkError');
const changeObjectLock = require('../../../../utilities/objectLock-util');

const bucketName = 'lockenabledputbucket';
const unlockedBucket = 'locknotenabledputbucket';
const objectName = 'putobjectretentionobject';

const retentionConfig = {
    Mode: 'GOVERNANCE',
    RetainUntilDate: moment().add(1, 'd').add(123, 'ms'),
};

const isCEPH = process.env.CI_CEPH !== undefined;
const describeSkipIfCeph = isCEPH ? describe.skip : describe;

describeSkipIfCeph('PUT object retention', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;
        let versionId;

        beforeEach(async () => {
            process.stdout.write('Putting buckets and objects\n');
            try {
                await s3.send(new CreateBucketCommand({
                    Bucket: bucketName,
                    ObjectLockEnabledForBucket: true,
                }));
                await s3.send(new CreateBucketCommand({ Bucket: unlockedBucket }));
                await s3.send(new PutObjectCommand({ Bucket: unlockedBucket, Key: objectName }));
                const putRes = await s3.send(new PutObjectCommand({ Bucket: bucketName, Key: objectName }));
                versionId = putRes.VersionId;
            } catch (err) {
                process.stdout.write(`Error in beforeEach ${err}\n`);
                throw err;
            }
        });

        afterEach(async () => {
            process.stdout.write('Emptying and deleting buckets\n');
            try {
                await bucketUtil.empty(bucketName);
                await bucketUtil.empty(unlockedBucket);
                await bucketUtil.deleteMany([bucketName, unlockedBucket]);
            } catch (err) {
                process.stdout.write(`Error in afterEach ${err}\n`);
                throw err;
            }
        });

        it('should return AccessDenied putting retention with another account', async () => {
            try {
                await otherAccountS3.send(new PutObjectRetentionCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Retention: retentionConfig,
                }));
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'AccessDenied', 403);
            }
        });

        it('should return NoSuchKey error if key does not exist', async () => {
            try {
                await s3.send(new PutObjectRetentionCommand({
                    Bucket: bucketName,
                    Key: 'thiskeydoesnotexist',
                    Retention: retentionConfig,
                }));
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'NoSuchKey', 404);
            }
        });

        it('should return NoSuchVersion error if version does not exist', async () => {
            try {
                await s3.send(new PutObjectRetentionCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: '012345678901234567890123456789012',
                    Retention: retentionConfig,
                }));
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'NoSuchVersion', 404);
            }
        });

        it('should return InvalidRequest error putting retention to object in bucket with no object lock ' +
            'enabled', async () => {
            try {
                await s3.send(new PutObjectRetentionCommand({
                    Bucket: unlockedBucket,
                    Key: objectName,
                    Retention: retentionConfig,
                }));
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'InvalidRequest', 400);
            }
        });

        it('should return MethodNotAllowed if object version is delete marker', async () => {
            await s3.send(new DeleteObjectCommand({ Bucket: bucketName, Key: objectName }));
            try {
                await s3.send(new PutObjectRetentionCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Retention: retentionConfig,
                }));
                assert.fail('Expected error');
            } catch (err) {
                checkError(err, 'MethodNotAllowed', 405);
            }
        });

        it('should put object retention', async () => {
            await s3.send(new PutObjectRetentionCommand({
                Bucket: bucketName,
                Key: objectName,
                Retention: retentionConfig,
            }));
            await new Promise((resolve, reject) => {
                changeObjectLock([{ bucket: bucketName, key: objectName, versionId }], '', err => {
                    if (err) {reject(err);}
                    else {resolve();}
                });
            });
        });

        it('should support request with versionId parameter', async () => {
            await s3.send(new PutObjectRetentionCommand({
                Bucket: bucketName,
                Key: objectName,
                Retention: retentionConfig,
                VersionId: versionId,
            }));
            await new Promise((resolve, reject) => {
                changeObjectLock([{ bucket: bucketName, key: objectName, versionId }], '', err => {
                    if (err) {reject(err);}
                    else {resolve();}
                });
            });
        });
    });
});
