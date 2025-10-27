const { promisify } = require('util');
const assert = require('assert');
const moment = require('moment');
const {
    CreateBucketCommand,
    PutObjectCommand,
    PutObjectRetentionCommand,
    GetObjectRetentionCommand,
    DeleteObjectCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const checkError = require('../../lib/utility/checkError');
const changeObjectLock = require('../../../../utilities/objectLock-util');

const changeLockPromise = promisify(changeObjectLock);

const bucketName = 'lockenabledbucket';
const unlockedBucket = 'locknotenabledbucket';
const objectName = 'putobjectretentionobject';
const noRetentionObject = 'objectwithnoretention';
const retainDate = moment().add(1, 'days').toISOString();

const retentionConfig = {
    Mode: 'GOVERNANCE',
    RetainUntilDate: new Date(retainDate),
};

const expectedConfig = {
    Mode: 'GOVERNANCE',
    RetainUntilDate: new Date(retainDate),
};

const isCEPH = process.env.CI_CEPH !== undefined;
const describeSkipIfCeph = isCEPH ? describe.skip : describe;

describeSkipIfCeph('GET object retention', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;
        let versionId;

        beforeEach(async () => {
            process.stdout.write('Putting buckets and objects\n');
            await s3.send(new CreateBucketCommand({
                Bucket: bucketName,
                ObjectLockEnabledForBucket: true,
            }));
            await s3.send(new CreateBucketCommand({ Bucket: unlockedBucket }));
            await s3.send(new PutObjectCommand({ Bucket: unlockedBucket, Key: objectName }));
            await s3.send(new PutObjectCommand({ Bucket: bucketName, Key: noRetentionObject }));
            
            const res = await s3.send(new PutObjectCommand({ Bucket: bucketName, Key: objectName }));
            versionId = res.VersionId;
            
            process.stdout.write('Putting object retention\n');
            await s3.send(new PutObjectRetentionCommand({
                Bucket: bucketName,
                Key: objectName,
                Retention: retentionConfig,
            }));
        });

        afterEach(async () => {
            await changeLockPromise([{ bucket: bucketName, key: objectName, versionId }], '');
            await bucketUtil.empty(bucketName);
            await bucketUtil.empty(unlockedBucket);
            await bucketUtil.deleteMany([bucketName, unlockedBucket]);
        });

        it('should return AccessDenied putting retention with another account',
        async () => {
            try {
                await otherAccountS3.send(new GetObjectRetentionCommand({
                    Bucket: bucketName,
                    Key: objectName,
                }));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                checkError(err, 'AccessDenied', 403);
            }
        });

        it('should return NoSuchKey error if key does not exist', async () => {
            try {
                await s3.send(new GetObjectRetentionCommand({
                    Bucket: bucketName,
                    Key: 'thiskeydoesnotexist',
                }));
                throw new Error('Expected NoSuchKey error');
            } catch (err) {
                checkError(err, 'NoSuchKey', 404);
            }
        });

        it('should return NoSuchVersion error if version does not exist', async () => {
            try {
                await s3.send(new GetObjectRetentionCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: '012345678901234567890123456789012',
                }));
                throw new Error('Expected NoSuchVersion error');
            } catch (err) {
                checkError(err, 'NoSuchVersion', 404);
            }
        });

        it('should return MethodNotAllowed if object version is delete marker',
        async () => {
            const res = await s3.send(new DeleteObjectCommand({ Bucket: bucketName, Key: objectName }));
            try {
                await s3.send(new GetObjectRetentionCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: res.VersionId,
                }));
                throw new Error('Expected MethodNotAllowed error');
            } catch (err) {
                checkError(err, 'MethodNotAllowed', 405);
            }
        });

        it('should return InvalidRequest error getting retention to object ' +
        'in bucket with no object lock enabled', async () => {
            try {
                await s3.send(new GetObjectRetentionCommand({
                    Bucket: unlockedBucket,
                    Key: objectName,
                }));
                throw new Error('Expected InvalidRequest error');
            } catch (err) {
                checkError(err, 'InvalidRequest', 400);
            }
        });

        it('should return NoSuchObjectLockConfiguration if no retention set',
        async () => {
            try {
                await s3.send(new GetObjectRetentionCommand({
                    Bucket: bucketName,
                    Key: noRetentionObject,
                }));
                throw new Error('Expected NoSuchObjectLockConfiguration error');
            } catch (err) {
                checkError(err, 'NoSuchObjectLockConfiguration', 404);
            }
        });

        it('should get object retention', async () => {
            const res = await s3.send(new GetObjectRetentionCommand({
                Bucket: bucketName,
                Key: objectName,
            }));
            assert.deepStrictEqual(res.Retention, expectedConfig);
            await changeLockPromise([
                { bucket: bucketName, key: objectName, versionId }], '');
        });
    });
});
