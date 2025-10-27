const assert = require('assert');
const {
    CreateBucketCommand,
    PutObjectCommand,
    PutObjectTaggingCommand,
    GetObjectTaggingCommand,
    PutBucketAclCommand,
    DeleteObjectTaggingCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const checkError = require('../../lib/utility/checkError');

const bucketName = 'testtaggingbucket';
const objectName = 'testtaggingobject';
const objectNameAcl = 'testtaggingobjectacl';

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

describe('GET object taggings', () => {
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

        it('should return appropriate tags after putting tags', async () => {
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

        it('should return no tag after putting and deleting tags', async () => {
            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
                Tagging: taggingConfig,
            }));
            await s3.send(new DeleteObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
            }));
            const data = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
            }));
            assert.deepStrictEqual(data.TagSet, []);
        });

        it('should return empty array after putting no tag',
        async () => {
            const data = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
            }));

            assert.deepStrictEqual(data.TagSet, []);
        });

        it('should return NoSuchKey getting tag set to a non-existing object',
        async () => {
            try {
                await s3.send(new GetObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: 'nonexisting',
                }));
                throw new Error('Expected NoSuchKey error');
            } catch (err) {
                checkError(err, 'NoSuchKey', 404);
            }
        });

        it('should return 403 AccessDenied getting tag set with another ' +
        'account', async () => {
            try {
                await otherAccountS3.send(new GetObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                }));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                checkError(err, 'AccessDenied', 403);
            }
        });

        it('should return 403 AccessDenied getting tag with a different ' +
        'account to an object with ACL "public-read-write"',
        async () => {
            try {
                await s3.send(new PutBucketAclCommand({
                    Bucket: bucketName,
                    ACL: 'public-read-write',
                }));
                await otherAccountS3.send(new GetObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                }));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                checkError(err, 'AccessDenied', 403);
            }
        });

        it('should return 403 AccessDenied getting tag set to an object' +
        ' in a bucket created with a different account', async () => {
            await s3.send(new PutBucketAclCommand({
                Bucket: bucketName,
                ACL: 'public-read-write',
            }));
            await otherAccountS3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectNameAcl,
            }));

            try {
                await otherAccountS3.send(new GetObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectNameAcl,
                }));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                checkError(err, 'AccessDenied', 403);
            }
        });

        it('should get tag to an object in a bucket created with same ' +
        'account', async () => {
            await s3.send(new PutBucketAclCommand({
                Bucket: bucketName,
                ACL: 'public-read-write',
            }));
            await otherAccountS3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectNameAcl,
            }));

            const data = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectNameAcl,
            }));

            assert.deepStrictEqual(data.TagSet, []);
        });
    });
});
