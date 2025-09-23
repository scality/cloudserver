const assert = require('assert');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    GetObjectTaggingCommand,
    PutObjectTaggingCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const getConfig = require('../support/config');
const {
    removeAllVersions,
    versioningEnabled,
    versioningSuspended,
} = require('../../lib/utility/versioning-util.js');
const checkError = require('../../lib/utility/checkError');

const bucket = 'versioning-test-bucket';

describe('versioning on object get tagging', () => {
    withV4(sigCfg => {
        let s3;

        before(async () => {
            s3 = new S3Client(getConfig('default', sigCfg));
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        });

        after(async () => {
            await removeAllVersions({ Bucket: bucket });
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        beforeEach(async () => {
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucket,
                VersioningConfiguration: versioningEnabled,
            }));
        });

        afterEach(async () => {
            await removeAllVersions({ Bucket: bucket });
        });

        it('should get tags for current version when no version specified', async () => {
            const key = 'key';
            const taggingConfig = {
                TagSet: [
                    { Key: 'key1', Value: 'value1' },
                    { Key: 'key2', Value: 'value2' },
                ],
            };
            
            // Put first version
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body1',
            }));
            const versionId1 = putResult1.VersionId;

            // Tag first version
            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
                Tagging: taggingConfig,
            }));

            // Put second version (becomes current)
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2',
            }));

            // Get tags without specifying version (should get current version, no tags)
            const getResult = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
            }));

            assert.deepStrictEqual(getResult.TagSet, []);
        });

        it('should get tags for specific version', async () => {
            const key = 'key';
            const taggingConfig = {
                TagSet: [
                    { Key: 'key1', Value: 'value1' },
                    { Key: 'key2', Value: 'value2' },
                ],
            };
            
            // Put first version
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body1',
            }));
            const versionId1 = putResult1.VersionId;

            // Tag first version
            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
                Tagging: taggingConfig,
            }));

            // Put second version
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2',
            }));

            // Get tags for specific first version
            const getResult = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
            }));

            assert.deepStrictEqual(getResult.TagSet, taggingConfig.TagSet);
        });

        it('should return empty tag set if no tags on version', async () => {
            const key = 'key';
            
            // Put object
            const putResult = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body',
            }));
            const versionId = putResult.VersionId;

            // Get tags (should be empty)
            const getResult = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
            }));

            assert.deepStrictEqual(getResult.TagSet, []);
        });

        it('should return InvalidArgument for invalid version id', async () => {
            const key = 'key';
            
            // Put object first
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body',
            }));

            // Try to get tags with invalid version id
            try {
                await s3.send(new GetObjectTaggingCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'invalid-version-id',
                }));
                throw new Error('Expected InvalidArgument error');
            } catch (err) {
                checkError(err, 'InvalidArgument', 400);
            }
        });

        it('should return NoSuchVersion for non-existent version id', async () => {
            const key = 'key';
            
            // Put object first
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body',
            }));

            // Try to get tags with non-existent but valid format version id
            const fakeVersionId = '39383736313031322039386162313433';
            
            try {
                await s3.send(new GetObjectTaggingCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: fakeVersionId,
                }));
                throw new Error('Expected NoSuchVersion error');
            } catch (err) {
                checkError(err, 'NoSuchVersion', 404);
            }
        });

        it('should return NoSuchKey when version belongs to different key', async () => {
            const key1 = 'key1';
            const key2 = 'key2';
            
            // Put object with key1
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key1,
                Body: 'body1',
            }));
            const versionId1 = putResult1.VersionId;

            // Try to get tags on key2 with version id from key1
            try {
                await s3.send(new GetObjectTaggingCommand({
                    Bucket: bucket,
                    Key: key2,
                    VersionId: versionId1,
                }));
                throw new Error('Expected NoSuchKey error');
            } catch (err) {
                checkError(err, 'NoSuchKey', 404);
            }
        });

        it('should work with versioning suspended', async () => {
            // Suspend versioning
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucket,
                VersioningConfiguration: versioningSuspended,
            }));

            const key = 'key';
            const taggingConfig = {
                TagSet: [
                    { Key: 'key1', Value: 'value1' },
                ],
            };
            
            // Put object (creates null version)
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body',
            }));

            // Tag the object
            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                Tagging: taggingConfig,
            }));

            // Get tags
            const getResult = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
            }));

            assert.deepStrictEqual(getResult.TagSet, taggingConfig.TagSet);
        });

        it('should handle multiple versions with different tags', async () => {
            const key = 'key';
            const taggingConfig1 = {
                TagSet: [
                    { Key: 'version', Value: '1' },
                    { Key: 'env', Value: 'test' },
                ],
            };
            const taggingConfig2 = {
                TagSet: [
                    { Key: 'version', Value: '2' },
                    { Key: 'env', Value: 'prod' },
                ],
            };
            
            // Put first version and tag it
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body1',
            }));
            const versionId1 = putResult1.VersionId;

            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
                Tagging: taggingConfig1,
            }));

            // Put second version and tag it
            const putResult2 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2',
            }));
            const versionId2 = putResult2.VersionId;

            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId2,
                Tagging: taggingConfig2,
            }));

            // Get tags for first version
            const getResult1 = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
            }));

            assert.deepStrictEqual(getResult1.TagSet, taggingConfig1.TagSet);

            // Get tags for second version (current)
            const getResult2 = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId2,
            }));

            assert.deepStrictEqual(getResult2.TagSet, taggingConfig2.TagSet);

            // Get tags without specifying version (should get current)
            const getCurrentResult = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
            }));

            assert.deepStrictEqual(getCurrentResult.TagSet, taggingConfig2.TagSet);
        });

        it('should preserve tags when adding new versions', async () => {
            const key = 'key';
            const taggingConfig = {
                TagSet: [
                    { Key: 'persistent', Value: 'tag' },
                ],
            };
            
            // Put first version and tag it
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body1',
            }));
            const versionId1 = putResult1.VersionId;

            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
                Tagging: taggingConfig,
            }));

            // Put second version (doesn't inherit tags)
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2',
            }));

            // First version should still have its tags
            const getResult1 = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
            }));

            assert.deepStrictEqual(getResult1.TagSet, taggingConfig.TagSet);

            // Current version should have no tags
            const getCurrentResult = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
            }));

            assert.deepStrictEqual(getCurrentResult.TagSet, []);
        });
    });
});
