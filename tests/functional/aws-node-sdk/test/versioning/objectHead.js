const assert = require('assert');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    HeadObjectCommand,
    DeleteObjectCommand,
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

describe('versioning on object head', () => {
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

        it('should head the current version when no version specified', async () => {
            const key = 'key';
            
            // Put first version
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body1',
                ContentType: 'text/plain',
            }));

            // Put second version (becomes current)
            const putResult2 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2-longer',
                ContentType: 'application/json',
            }));
            const versionId2 = putResult2.VersionId;

            // Head without specifying version (should get current)
            const headResult = await s3.send(new HeadObjectCommand({
                Bucket: bucket,
                Key: key,
            }));

            assert.strictEqual(headResult.VersionId, versionId2);
            assert.strictEqual(headResult.ContentLength, 12); // 'body2-longer'
            assert.strictEqual(headResult.ContentType, 'application/json');
        });

        it('should head specific version when version id provided', async () => {
            const key = 'key';
            
            // Put first version
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body1',
                ContentType: 'text/plain',
            }));
            const versionId1 = putResult1.VersionId;

            // Put second version
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2-longer',
                ContentType: 'application/json',
            }));

            // Head specific first version
            const headResult = await s3.send(new HeadObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
            }));

            assert.strictEqual(headResult.VersionId, versionId1);
            assert.strictEqual(headResult.ContentLength, 5); // 'body1'
            assert.strictEqual(headResult.ContentType, 'text/plain');
        });

        it('should return NoSuchKey when trying to head deleted object without version', async () => {
            const key = 'key';
            
            // Put object
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body',
            }));

            // Delete object (creates delete marker)
            await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
            }));

            // Try to head object without version (should fail)
            try {
                await s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: key,
                }));
                throw new Error('Expected NoSuchKey error');
            } catch (err) {
                checkError(err, 'NoSuchKey', 404);
            }
        });

        it('should head specific version even if delete marker exists', async () => {
            const key = 'key';
            
            // Put object
            const putResult = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body',
                ContentType: 'text/plain',
            }));
            const versionId = putResult.VersionId;

            // Delete object (creates delete marker)
            await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
            }));

            // Head specific version (should work)
            const headResult = await s3.send(new HeadObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
            }));

            assert.strictEqual(headResult.VersionId, versionId);
            assert.strictEqual(headResult.ContentLength, 4); // 'body'
            assert.strictEqual(headResult.ContentType, 'text/plain');
        });

        it('should return InvalidArgument for invalid version id', async () => {
            const key = 'key';
            
            // Put object first
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body',
            }));

            // Try to head with invalid version id
            try {
                await s3.send(new HeadObjectCommand({
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

            // Try to head with non-existent but valid format version id
            const fakeVersionId = '39383736313031322039386162313433';
            
            try {
                await s3.send(new HeadObjectCommand({
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

            // Try to head key2 with version id from key1
            try {
                await s3.send(new HeadObjectCommand({
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
            
            // Put object (creates null version)
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body1',
                ContentType: 'text/plain',
            }));

            // Put again (overwrites null version)
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2',
                ContentType: 'application/json',
            }));

            // Head object (should get latest)
            const headResult = await s3.send(new HeadObjectCommand({
                Bucket: bucket,
                Key: key,
            }));

            assert.strictEqual(headResult.VersionId, 'null');
            assert.strictEqual(headResult.ContentLength, 5); // 'body2'
            assert.strictEqual(headResult.ContentType, 'application/json');
        });

        it('should preserve metadata across versions', async () => {
            const key = 'key';
            const metadata1 = { 'x-amz-meta-test': 'value1' };
            const metadata2 = { 'x-amz-meta-test': 'value2' };
            
            // Put first version with metadata
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body1',
                Metadata: metadata1,
                ContentType: 'text/plain',
            }));
            const versionId1 = putResult1.VersionId;

            // Put second version with different metadata
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2',
                Metadata: metadata2,
                ContentType: 'application/json',
            }));

            // Head first version and verify its metadata
            const headResult1 = await s3.send(new HeadObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
            }));

            assert.deepStrictEqual(headResult1.Metadata, metadata1);
            assert.strictEqual(headResult1.ContentType, 'text/plain');
            assert.strictEqual(headResult1.ContentLength, 5); // 'body1'

            // Head current version and verify its metadata
            const headResult2 = await s3.send(new HeadObjectCommand({
                Bucket: bucket,
                Key: key,
            }));

            assert.deepStrictEqual(headResult2.Metadata, metadata2);
            assert.strictEqual(headResult2.ContentType, 'application/json');
            assert.strictEqual(headResult2.ContentLength, 5); // 'body2'
        });

        it('should handle range headers on specific versions', async () => {
            const key = 'key';
            const content1 = 'Hello World Version 1';
            const content2 = 'Hello World Version 2';
            
            // Put first version
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: content1,
                ContentType: 'text/plain',
            }));
            const versionId1 = putResult1.VersionId;

            // Put second version
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: content2,
                ContentType: 'text/plain',
            }));

            // Head range from first version
            const headResult = await s3.send(new HeadObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
                Range: 'bytes=0-4', // "Hello"
            }));

            assert.strictEqual(headResult.VersionId, versionId1);
            assert.strictEqual(headResult.ContentRange, 'bytes 0-4/21');
            assert.strictEqual(headResult.ContentLength, 5); // Range length
        });

        it('should return correct ETag for different versions', async () => {
            const key = 'key';
            
            // Put first version
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body1',
            }));
            const versionId1 = putResult1.VersionId;
            const etag1 = putResult1.ETag;

            // Put second version
            const putResult2 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2',
            }));
            const versionId2 = putResult2.VersionId;
            const etag2 = putResult2.ETag;

            // Head first version
            const headResult1 = await s3.send(new HeadObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId1,
            }));

            assert.strictEqual(headResult1.VersionId, versionId1);
            assert.strictEqual(headResult1.ETag, etag1);

            // Head current version
            const headResult2 = await s3.send(new HeadObjectCommand({
                Bucket: bucket,
                Key: key,
            }));

            assert.strictEqual(headResult2.VersionId, versionId2);
            assert.strictEqual(headResult2.ETag, etag2);
        });

        it('should handle conditional headers with versions', async () => {
            const key = 'key';
            
            // Put object
            const putResult = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body',
            }));
            const versionId = putResult.VersionId;
            const etag = putResult.ETag;

            // Head with matching ETag (should succeed)
            const headResult1 = await s3.send(new HeadObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
                IfMatch: etag,
            }));

            assert.strictEqual(headResult1.VersionId, versionId);

            // Head with non-matching ETag (should fail)
            try {
                await s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: versionId,
                    IfMatch: '"non-matching-etag"',
                }));
                throw new Error('Expected PreconditionFailed error');
            } catch (err) {
                checkError(err, 'PreconditionFailed', 412);
            }
        });
    });
});
