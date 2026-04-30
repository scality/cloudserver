const { promisify } = require('util');
const assert = require('assert');
const {
    CreateBucketCommand,
    PutObjectCommand,
    PutObjectLegalHoldCommand,
    GetObjectLegalHoldCommand,
    DeleteObjectCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const checkError = require('../../lib/utility/checkError');
const changeObjectLock = require('../../../../utilities/objectLock-util');

const changeLockPromise = promisify(changeObjectLock);

const bucket = 'mock-bucket-lock';
const unlockedBucket = 'mock-bucket-no-lock';
const key = 'mock-object-legalhold';
const keyNoHold = 'mock-object-no-legalhold';


describe('GET object legal hold', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;
        let versionId;

        beforeEach(async () => {
            process.stdout.write('Putting buckets and objects\n');
            process.stdout.write('Putting object legal hold\n');
            await s3.send(new CreateBucketCommand({
                Bucket: bucket,
                ObjectLockEnabledForBucket: true,
            }));
            await s3.send(new CreateBucketCommand({ Bucket: unlockedBucket }));
            await s3.send(new PutObjectCommand({ Bucket: unlockedBucket, Key: key }));
            await s3.send(new PutObjectCommand({ Bucket: bucket, Key: keyNoHold }));

            const res = await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key }));
            versionId = res.VersionId;
            process.stdout.write('Putting object legal hold\n');
            await s3.send(new PutObjectLegalHoldCommand({
                Bucket: bucket,
                Key: key,
                LegalHold: { Status: 'ON' },
            }));
        });

        afterEach(() => {
            process.stdout.write('Removing object lock\n');
            return changeLockPromise([{ bucket, key, versionId }], {})
            .then(() => {
                process.stdout.write('Emptying and deleting buckets\n');
                return bucketUtil.empty(bucket);
            })
            .then(() => bucketUtil.empty(unlockedBucket))
            .then(() => bucketUtil.deleteMany([bucket, unlockedBucket]))
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            });
        });

        it('should return AccessDenied getting legal hold with another account', 
            () => otherAccountS3.send(new GetObjectLegalHoldCommand({
                Bucket: bucket,
                Key: key,
            })).then(() => {
                throw new Error('Expected AccessDenied error');
            }).catch(err => {
                checkError(err, 'AccessDenied', 403);
            })
        );

        it('should return MethodNotAllowed if object version is delete marker', () => s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
            })).then(res => s3.send(new GetObjectLegalHoldCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: res.VersionId,
                })).then(() => {
                    throw new Error('Expected NoSuchKey error');
                }).catch(err => {
                    checkError(err, 'MethodNotAllowed', 405);
                })).catch(err => {
                assert.ifError(err);
            })
        );

        it('should return NoSuchKey if latest version is delete marker', () => s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
            })).then(() => s3.send(new GetObjectLegalHoldCommand({
                    Bucket: bucket,
                    Key: key,
                })).then(() => {
                    throw new Error('Expected NoSuchKey error');
                }).catch(err => {
                    checkError(err, 'NoSuchKey', 404);
                })
            ).catch(err => {
                assert.ifError(err);
            })
        );

        it('should return InvalidRequest error getting legal hold of object ' +
        'inside object lock disabled bucket', () => s3.send(new GetObjectLegalHoldCommand({
                Bucket: unlockedBucket,
                Key: key,
            })).then(() => {
                throw new Error('Expected InvalidRequest error');
            }).catch(err => {
                checkError(err, 'InvalidRequest', 400);
            })
        );

       it('should return NoSuchObjectLockConfiguration if no legal hold set', () => 
            s3.send(new GetObjectLegalHoldCommand({
               Bucket: bucket,
               Key: keyNoHold,
            })).then(() => {
                throw new Error('Expected NoSuchObjectLockConfiguration error');
            }).catch(err => {
                checkError(err, 'NoSuchObjectLockConfiguration', 404);
            })
        );

       it('should get object legal hold', async () => {
            const res = await s3.send(new GetObjectLegalHoldCommand({
                Bucket: bucket,
                Key: key,
            }));
            
            assert.deepStrictEqual(res.LegalHold, { Status: 'ON' });
            await changeLockPromise([{ bucket, key, versionId }], {});
        });
    });
});
