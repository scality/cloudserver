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

describe('versioning on object put tagging', () => {
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

        it('should put tags on current version when no version specified', async () => {
            const key = 'key';
            const taggingConfig = {
                TagSet: [
                    { Key: 'key1', Value: 'value1' },
                    { Key: 'key2', Value: 'value2' },
                ],
            };
            
            // Put first version
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body1',
            }));

            // Put second version (becomes current)
            const putResult2 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2',
            }));
            const versionId2 = putResult2.VersionId;

            // Tag without specifying version (should tag current)
            const putTagResult = await s3.send(new PutObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                Tagging: taggingConfig,
            }));

            assert.strictEqual(putTagResult.VersionId, versionId2);

            // Verify tags were applied to current version
            const getResult = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
            }));

            assert.deepStrictEqual(getResult.TagSet, taggingConfig.TagSet);
        });

        it('should put tags on specific version', async () => {
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

            // Put second version
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2',
            }));

            // Tag specific first version
            const putTagResult = await s3.send(new PutObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
                Tagging: taggingConfig,
            }));

            assert.strictEqual(putTagResult.VersionId, versionId1);

            // Verify tags were applied to specific version
            const getResult = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
            }));

            assert.deepStrictEqual(getResult.TagSet, taggingConfig.TagSet);

            // Current version should have no tags
            const getCurrentResult = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
            }));

            assert.deepStrictEqual(getCurrentResult.TagSet, []);
        });

        it('should overwrite existing tags on version', async () => {
            const key = 'key';
            const initialTags = {
                TagSet: [
                    { Key: 'initial', Value: 'tag' },
                ],
            };
            const newTags = {
                TagSet: [
                    { Key: 'new', Value: 'tag' },
                    { Key: 'another', Value: 'tag' },
                ],
            };
            
            // Put object
            const putResult = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body',
            }));
            const versionId = putResult.VersionId;

            // Put initial tags
            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
                Tagging: initialTags,
            }));

            // Verify initial tags
            const getResult1 = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
            }));

            assert.deepStrictEqual(getResult1.TagSet, initialTags.TagSet);

            // Overwrite with new tags
            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
                Tagging: newTags,
            }));

            // Verify new tags replaced old ones
            const getResult2 = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
            }));

            assert.deepStrictEqual(getResult2.TagSet, newTags.TagSet);
        });

        it('should return InvalidArgument for invalid version id', async () => {
            const key = 'key';
            const taggingConfig = {
                TagSet: [
                    { Key: 'key1', Value: 'value1' },
                ],
            };
            
            // Put object first
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body',
            }));

            // Try to tag with invalid version id
            try {
                await s3.send(new PutObjectTaggingCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'invalid-version-id',
                    Tagging: taggingConfig,
                }));
                throw new Error('Expected InvalidArgument error');
            } catch (err) {
                checkError(err, 'InvalidArgument', 400);
            }
        });

        it('should return NoSuchVersion for non-existent version id', async () => {
            const key = 'key';
            const taggingConfig = {
                TagSet: [
                    { Key: 'key1', Value: 'value1' },
                ],
            };
            
            // Put object first
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body',
            }));

            // Try to tag with non-existent but valid format version id
            const fakeVersionId = '39383736313031322039386162313433';
            
            try {
                await s3.send(new PutObjectTaggingCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: fakeVersionId,
                    Tagging: taggingConfig,
                }));
                throw new Error('Expected NoSuchVersion error');
            } catch (err) {
                checkError(err, 'NoSuchVersion', 404);
            }
        });

        it('should return NoSuchKey when version belongs to different key', async () => {
            const key1 = 'key1';
            const key2 = 'key2';
            const taggingConfig = {
                TagSet: [
                    { Key: 'key1', Value: 'value1' },
                ],
            };
            
            // Put object with key1
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key1,
                Body: 'body1',
            }));
            const versionId1 = putResult1.VersionId;

            // Try to tag key2 with version id from key1
            try {
                await s3.send(new PutObjectTaggingCommand({
                    Bucket: bucket,
                    Key: key2,
                    VersionId: versionId1,
                    Tagging: taggingConfig,
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
            const putTagResult = await s3.send(new PutObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                Tagging: taggingConfig,
            }));

            // Version id should be undefined for suspended versioning
            assert.strictEqual(putTagResult.VersionId, undefined);

            // Verify tags were applied
            const getResult = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
            }));

            assert.deepStrictEqual(getResult.TagSet, taggingConfig.TagSet);
        });

        it('should handle multiple versions with independent tag sets', async () => {
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

            // Verify both versions have their respective tags
            const getResult1 = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
            }));

            assert.deepStrictEqual(getResult1.TagSet, taggingConfig1.TagSet);

            const getResult2 = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId2,
            }));

            assert.deepStrictEqual(getResult2.TagSet, taggingConfig2.TagSet);

            // Modify tags on first version, should not affect second version
            const modifiedTags = {
                TagSet: [
                    { Key: 'modified', Value: 'true' },
                ],
            };

            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
                Tagging: modifiedTags,
            }));

            // First version should have modified tags
            const getModifiedResult = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
            }));

            assert.deepStrictEqual(getModifiedResult.TagSet, modifiedTags.TagSet);

            // Second version should be unchanged
            const getUnchangedResult = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId2,
            }));

            assert.deepStrictEqual(getUnchangedResult.TagSet, taggingConfig2.TagSet);
        });

        it('should clear tags when putting empty tag set', async () => {
            const key = 'key';
            const initialTags = {
                TagSet: [
                    { Key: 'initial', Value: 'tag' },
                ],
            };
            const emptyTags = {
                TagSet: [],
            };
            
            // Put object and tag it
            const putResult = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body',
            }));
            const versionId = putResult.VersionId;

            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
                Tagging: initialTags,
            }));

            // Verify initial tags
            const getResult1 = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
            }));

            assert.deepStrictEqual(getResult1.TagSet, initialTags.TagSet);

            // Clear tags with empty tag set
            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
                Tagging: emptyTags,
            }));

            // Verify tags are cleared
            const getResult2 = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
            }));

            assert.deepStrictEqual(getResult2.TagSet, []);
        });
    });
});
