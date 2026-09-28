const assert = require('assert');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    GetObjectLockConfigurationCommand,
    PutObjectLockConfigurationCommand,
} = require('@aws-sdk/client-s3');

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
        s3 = new S3Client(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return done();
    });

    it('should return NoSuchBucket error if bucket does not exist', async () => {
        try {
            await s3.send(new GetObjectLockConfigurationCommand({ Bucket: bucket }));
            throw new Error('Expected NoSuchBucket error');
        } catch (err) {
            checkError(err, 'NoSuchBucket', 404);
        }
    });

    describe('request to object lock disabled bucket', () => {
        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        });

        afterEach(async () => {
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        it('should return ObjectLockConfigurationNotFoundError', async () => {
            try {
                await s3.send(new GetObjectLockConfigurationCommand({ Bucket: bucket }));
                throw new Error('Expected ObjectLockConfigurationNotFoundError');
            } catch (err) {
                checkError(err, 'ObjectLockConfigurationNotFoundError', 404);
            }
        });
    });

    describe('config rules', () => {
        beforeEach(() =>
            s3.send(
                new CreateBucketCommand({
                    Bucket: bucket,
                    ObjectLockEnabledForBucket: true,
                }),
            ),
        );

        afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

        it('should return AccessDenied if user is not bucket owner', async () => {
            try {
                await otherAccountS3.send(new GetObjectLockConfigurationCommand({ Bucket: bucket }));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                checkError(err, 'AccessDenied', 403);
            }
        });

        it('should get bucket object lock config', async () => {
            await s3.send(
                new PutObjectLockConfigurationCommand({
                    Bucket: bucket,
                    ObjectLockConfiguration: objectLockConfig,
                }),
            );
            const res = await s3.send(new GetObjectLockConfigurationCommand({ Bucket: bucket }));
            assert.deepStrictEqual(res.ObjectLockConfiguration, objectLockConfig);
        });
    });
});
