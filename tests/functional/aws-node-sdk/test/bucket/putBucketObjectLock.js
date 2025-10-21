const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectLockConfigurationCommand } = require('@aws-sdk/client-s3');

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

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
    });

    it('should return NoSuchBucket error if bucket does not exist', async () => {
        const params = getObjectLockParams('Enabled', 'GOVERNANCE', 1);
        try {
            await s3.send(new PutObjectLockConfigurationCommand(params));
            throw new Error('Expected NoSuchBucket error');
        } catch (err) {
            checkError(err, 'NoSuchBucket', 404);
        }
    });

    describe('on object lock disabled bucket', () => {
        beforeEach(() => s3.send(new CreateBucketCommand({Bucket: bucket})));

        afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

        it('should return InvalidBucketState error', async () => {
            const params = getObjectLockParams('Enabled', 'GOVERNANCE', 1);
            try {
                await s3.send(new PutObjectLockConfigurationCommand(params));
                throw new Error('Expected InvalidBucketState error');
            } catch (err) {
                checkError(err, 'InvalidBucketState', 409);
            }
        });

        it('should return InvalidBucketState error without Rule', async () => {
            const params = {
                Bucket: bucket,
                ObjectLockConfiguration: {
                    ObjectLockEnabled: 'Enabled',
                },
            };
            try {
                await s3.send(new PutObjectLockConfigurationCommand(params));
                throw new Error('Expected InvalidBucketState error');
            } catch (err) {
                checkError(err, 'InvalidBucketState', 409);
            }
        });
    });

    describe('config rules', () => {
        beforeEach(() => s3.send(new CreateBucketCommand({
            Bucket: bucket,
            ObjectLockEnabledForBucket: true,
        })));

        afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

        it('should return AccessDenied if user is not bucket owner', async () => {
            const params = getObjectLockParams('Enabled', 'GOVERNANCE', 1);
            try {
                await otherAccountS3.send(new PutObjectLockConfigurationCommand(params));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                checkError(err, 'AccessDenied', 403);
            }
        });

        it('should put object lock configuration on bucket with Governance mode', async () => {
            const params = getObjectLockParams('Enabled', 'GOVERNANCE', 30);
            await s3.send(new PutObjectLockConfigurationCommand(params));
        });

        it('should put object lock configuration on bucket with Compliance mode', async () => {
            const params = getObjectLockParams('Enabled', 'COMPLIANCE', 30);
            await s3.send(new PutObjectLockConfigurationCommand(params));
        });

        it('should put object lock configuration on bucket with year retention type', async () => {
            const params = getObjectLockParams('Enabled', 'COMPLIANCE', null, 2);
            await s3.send(new PutObjectLockConfigurationCommand(params));
        });

        it('should not allow object lock config request with zero day retention', async () => {
            const params = getObjectLockParams('Enabled', 'GOVERNANCE', null, 0);
            try {
                await s3.send(new PutObjectLockConfigurationCommand(params));
                throw new Error('Expected MalformedXML error');
            } catch (err) {
                checkError(err, 'MalformedXML', 400);
            }
        });

        it('should not allow object lock config request with negative retention', async () => {
            const params = getObjectLockParams('Enabled', 'GOVERNANCE', -1);
            try {
                await s3.send(new PutObjectLockConfigurationCommand(params));
                throw new Error('Expected InvalidArgument error');
            } catch (err) {
                checkError(err, 'InvalidArgument', 400);
            }
        });

        it('should not allow object lock config request with both Days and Years', async () => {
            const params = getObjectLockParams('Enabled', 'GOVERNANCE', 1, 1);
            try {
                await s3.send(new PutObjectLockConfigurationCommand(params));
                throw new Error('Expected MalformedXML error');
            } catch (err) {
                checkError(err, 'MalformedXML', 400);
            }
        });

        it('should not allow object lock config request without days or years', async () => {
            const params = getObjectLockParams('Enabled', 'GOVERNANCE');
            try {
                await s3.send(new PutObjectLockConfigurationCommand(params));
                throw new Error('Expected MalformedXML error');
            } catch (err) {
                checkError(err, 'MalformedXML', 400);
            }
        });

        it('should not allow object lock config request with invalid ObjectLockEnabled', async () => {
            const params = getObjectLockParams('enabled', 'GOVERNANCE', 10);
            try {
                await s3.send(new PutObjectLockConfigurationCommand(params));
                throw new Error('Expected MalformedXML error');
            } catch (err) {
                checkError(err, 'MalformedXML', 400);
            }
        });

        it('should not allow object lock config request with invalid mode', async () => {
            const params = getObjectLockParams('Enabled', 'Governance', 10);
            try {
                await s3.send(new PutObjectLockConfigurationCommand(params));
                throw new Error('Expected MalformedXML error');
            } catch (err) {
                checkError(err, 'MalformedXML', 400);
            }
        });
    });
});
