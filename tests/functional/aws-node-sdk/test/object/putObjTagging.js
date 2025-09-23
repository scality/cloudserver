const assert = require('assert');
const {
    CreateBucketCommand,
    PutObjectCommand,
    PutObjectTaggingCommand,
    GetObjectTaggingCommand,
    PutBucketAclCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const checkError = require('../../lib/utility/checkError');

const bucketName = 'testputtaggingbucket';
const objectName = 'testputtaggingobject';
const objectNameAcl = 'testputtaggingobjectacl';

const taggingConfig = { TagSet: [
    {
        Key: 'key1',
        Value: 'value1',
    },
    {
        Key: 'key2',
        Value: 'value2',
    },
] };

const taggingConfigBad = { TagSet: [
    {
        Key: 'key1',
        Value: 'value1',
    },
    {
        Key: 'key2',
        Value: 'value2',
    },
    {
        Key: 'key1',
        Value: 'value1',
    },
] };

const taggingConfigNoValue = { TagSet: [
    {
        Key: 'key1',
    },
] };

const taggingConfigNoKey = { TagSet: [
    {
        Value: 'value1',
    },
] };

describe('PUT object taggings', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
            await s3.send(new PutObjectCommand({ Bucket: bucketName, Key: objectName }));
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket');
            return bucketUtil.empty(bucketName)
            .then(() => {
                process.stdout.write('Deleting bucket');
                return bucketUtil.deleteOne(bucketName);
            })
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            });
        });

        it('should put tag set', async () => {
            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
                Tagging: taggingConfig,
            }));

            const data = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
            }));

            assert.deepStrictEqual(data.TagSet, taggingConfig.TagSet);
        });

        it('should return InvalidRequest putting tag set with duplicate key',
        async () => {
            try {
                await s3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Tagging: taggingConfigBad,
                }));
                throw new Error('Expected InvalidRequest error');
            } catch (err) {
                checkError(err, 'InvalidTag', 400);
            }
        });

        it('should return MalformedXML putting tag set with no value for key',
        async () => {
            try {
                await s3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Tagging: taggingConfigNoValue,
                }));
                throw new Error('Expected MalformedXML error');
            } catch (err) {
                checkError(err, 'MalformedXML', 400);
            }
        });

        it('should return MalformedXML putting tag set with no key for value',
        async () => {
            try {
                await s3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Tagging: taggingConfigNoKey,
                }));
                throw new Error('Expected MalformedXML error');
            } catch (err) {
                checkError(err, 'MalformedXML', 400);
            }
        });

        it('should return NoSuchKey putting tag set to a non-existing object',
        async () => {
            try {
                await s3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: 'nonexisting',
                    Tagging: taggingConfig,
                }));
                throw new Error('Expected NoSuchKey error');
            } catch (err) {
                checkError(err, 'NoSuchKey', 404);
            }
        });

        it('should return 403 AccessDenied putting tag set with another ' +
        'account', async () => {
            try {
                await otherAccountS3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Tagging: taggingConfig,
                }));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                checkError(err, 'AccessDenied', 403);
            }
        });

        it('should return 403 AccessDenied putting tag set with a different ' +
        'account to an object with ACL "public-read-write"',
        async () => {
            await s3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectName,
                ACL: 'public-read-write',
            }));

            try {
                await otherAccountS3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Tagging: taggingConfig,
                }));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                checkError(err, 'AccessDenied', 403);
            }
        });

        it('should return 403 AccessDenied putting tag set to an object' +
        ' in a bucket created with a different account',
        async () => {
            await s3.send(new PutBucketAclCommand({
                Bucket: bucketName,
                ACL: 'public-read-write',
            }));
            await otherAccountS3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectNameAcl,
            }));

            try {
                await otherAccountS3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectNameAcl,
                    Tagging: taggingConfig,
                }));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                checkError(err, 'AccessDenied', 403);
            }
        });

        it('should put tag set to an object in a bucket created with same ' +
        'account even though object put by other account', async () => {
            await s3.send(new PutBucketAclCommand({
                Bucket: bucketName,
                ACL: 'public-read-write',
            }));
            await otherAccountS3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectNameAcl,
            }));

            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectNameAcl,
                Tagging: taggingConfig,
            }));
        });
    });
});
