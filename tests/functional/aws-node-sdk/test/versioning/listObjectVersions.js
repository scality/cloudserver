const assert = require('assert');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    ListObjectVersionsCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const getConfig = require('../support/config');
const {
    removeAllVersions,
    versioningEnabled,
    versioningSuspended,
} = require('../../lib/utility/versioning-util.js');

const bucket = 'versioning-test-bucket';

describe('listObjectVersions', () => {
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

        it('should list object versions', async () => {
            const key = 'key';
            
            // Put first version
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body1',
            }));
            const versionId1 = putResult1.VersionId;

            // Put second version
            const putResult2 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2',
            }));
            const versionId2 = putResult2.VersionId;

            // List versions
            const listResult = await s3.send(new ListObjectVersionsCommand({
                Bucket: bucket,
            }));

            assert.strictEqual(listResult.Versions.length, 2);
            
            // Versions should be sorted by LastModified (newest first)
            const versions = listResult.Versions.sort((a, b) => 
                new Date(b.LastModified) - new Date(a.LastModified));
            
            assert.strictEqual(versions[0].VersionId, versionId2);
            assert.strictEqual(versions[1].VersionId, versionId1);
            assert.strictEqual(versions[0].Key, key);
            assert.strictEqual(versions[1].Key, key);
        });

        it('should list both versions and delete markers', async () => {
            const key = 'key';
            
            // Put first version
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body1',
            }));

            // Delete object (creates delete marker)
            await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
            }));

            // Put second version
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2',
            }));

            // List versions
            const listResult = await s3.send(new ListObjectVersionsCommand({
                Bucket: bucket,
            }));

            assert.strictEqual(listResult.Versions.length, 2);
            assert.strictEqual(listResult.DeleteMarkers.length, 1);
            assert.strictEqual(listResult.DeleteMarkers[0].Key, key);
        });

        it('should handle pagination with MaxKeys', async () => {
            const key = 'key';
            
            // Put multiple versions
            for (let i = 0; i < 5; i++) {
                await s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: `body${i}`,
                }));
            }

            // List with MaxKeys = 2
            const listResult = await s3.send(new ListObjectVersionsCommand({
                Bucket: bucket,
                MaxKeys: 2,
            }));

            assert.strictEqual(listResult.Versions.length, 2);
            assert.strictEqual(listResult.IsTruncated, true);
            assert(listResult.NextKeyMarker);
            assert(listResult.NextVersionIdMarker);
        });

        it('should filter by prefix', async () => {
            // Put objects with different prefixes
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: 'folder1/file1',
                Body: 'body1',
            }));

            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: 'folder2/file2',
                Body: 'body2',
            }));

            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: 'folder1/file3',
                Body: 'body3',
            }));

            // List with prefix filter
            const listResult = await s3.send(new ListObjectVersionsCommand({
                Bucket: bucket,
                Prefix: 'folder1/',
            }));

            assert.strictEqual(listResult.Versions.length, 2);
            listResult.Versions.forEach(version => {
                assert(version.Key.startsWith('folder1/'));
            });
        });

        it('should handle delimiter for common prefixes', async () => {
            // Put objects in different "folders"
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: 'folder1/file1',
                Body: 'body1',
            }));

            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: 'folder2/file2',
                Body: 'body2',
            }));

            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: 'file3',
                Body: 'body3',
            }));

            // List with delimiter
            const listResult = await s3.send(new ListObjectVersionsCommand({
                Bucket: bucket,
                Delimiter: '/',
            }));

            assert.strictEqual(listResult.Versions.length, 1); // file3
            assert.strictEqual(listResult.CommonPrefixes.length, 2); // folder1/, folder2/
            assert.strictEqual(listResult.Versions[0].Key, 'file3');
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
            }));

            // Put again (overwrites null version)
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2',
            }));

            // List versions
            const listResult = await s3.send(new ListObjectVersionsCommand({
                Bucket: bucket,
            }));

            // Should only have one version (the null version)
            assert.strictEqual(listResult.Versions.length, 1);
            assert.strictEqual(listResult.Versions[0].Key, key);
            assert.strictEqual(listResult.Versions[0].VersionId, 'null');
        });
    });
});
