const assert = require('assert');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    GetObjectCommand,
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

describe('versioning on object put', () => {
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

        it('should create new version when putting object', async () => {
            const key = 'key';
            
            // Put first version
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body1',
            }));
            const versionId1 = putResult1.VersionId;

            assert(versionId1);
            assert.notEqual(versionId1, 'null');

            // Put second version
            const putResult2 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2',
            }));
            const versionId2 = putResult2.VersionId;

            assert(versionId2);
            assert.notEqual(versionId2, versionId1);
            assert.notEqual(versionId2, 'null');

            // Verify both versions exist
            const listResult = await s3.send(new ListObjectVersionsCommand({
                Bucket: bucket,
            }));

            assert.strictEqual(listResult.Versions.length, 2);
            const versionIds = listResult.Versions.map(v => v.VersionId);
            assert(versionIds.includes(versionId1));
            assert(versionIds.includes(versionId2));
        });

        it('should return version id in response', async () => {
            const key = 'key';
            
            const putResult = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body',
            }));

            assert(putResult.VersionId);
            assert.notEqual(putResult.VersionId, 'null');
            assert.strictEqual(putResult.$metadata.httpStatusCode, 200);
        });

        it('should create different version ids for same content', async () => {
            const key = 'key';
            const body = 'same content';
            
            // Put same content twice
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: body,
            }));

            const putResult2 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: body,
            }));

            // Should have different version ids even with same content
            assert.notEqual(putResult1.VersionId, putResult2.VersionId);

            // Both versions should exist
            const listResult = await s3.send(new ListObjectVersionsCommand({
                Bucket: bucket,
            }));

            assert.strictEqual(listResult.Versions.length, 2);
        });

        it('should preserve each version independently', async () => {
            const key = 'key';
            
            // Put first version
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'version1',
                ContentType: 'text/plain',
                Metadata: { version: '1' },
            }));

            // Put second version
            const putResult2 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'version2',
                ContentType: 'application/json',
                Metadata: { version: '2' },
            }));

            // Get first version
            const getResult1 = await s3.send(new GetObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: putResult1.VersionId,
            }));

            const body1 = await getResult1.Body.transformToString();
            assert.strictEqual(body1, 'version1');
            assert.strictEqual(getResult1.ContentType, 'text/plain');
            assert.strictEqual(getResult1.Metadata.version, '1');

            // Get second version
            const getResult2 = await s3.send(new GetObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: putResult2.VersionId,
            }));

            const body2 = await getResult2.Body.transformToString();
            assert.strictEqual(body2, 'version2');
            assert.strictEqual(getResult2.ContentType, 'application/json');
            assert.strictEqual(getResult2.Metadata.version, '2');
        });

        it('should make latest version current', async () => {
            const key = 'key';
            
            // Put first version
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'version1',
            }));

            // Put second version
            const putResult2 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'version2',
            }));

            // Get without version id (should get current/latest)
            const getResult = await s3.send(new GetObjectCommand({
                Bucket: bucket,
                Key: key,
            }));

            assert.strictEqual(getResult.VersionId, putResult2.VersionId);
            const body = await getResult.Body.transformToString();
            assert.strictEqual(body, 'version2');
        });

        it('should work with versioning suspended', async () => {
            // Suspend versioning
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucket,
                VersioningConfiguration: versioningSuspended,
            }));

            const key = 'key';
            
            // Put first object (creates null version)
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body1',
            }));

            assert.strictEqual(putResult1.VersionId, undefined);

            // Put second object (overwrites null version)
            const putResult2 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'body2',
            }));

            assert.strictEqual(putResult2.VersionId, undefined);

            // Should only have one version (null version)
            const listResult = await s3.send(new ListObjectVersionsCommand({
                Bucket: bucket,
            }));

            assert.strictEqual(listResult.Versions.length, 1);
            assert.strictEqual(listResult.Versions[0].VersionId, 'null');

            // Get object should return latest content
            const getResult = await s3.send(new GetObjectCommand({
                Bucket: bucket,
                Key: key,
            }));

            const body = await getResult.Body.transformToString();
            assert.strictEqual(body, 'body2');
        });

        it('should handle transition from suspended to enabled', async () => {
            const key = 'key';
            
            // Start with versioning suspended
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucket,
                VersioningConfiguration: versioningSuspended,
            }));

            // Put object (creates null version)
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'null-version',
            }));

            // Enable versioning
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucket,
                VersioningConfiguration: versioningEnabled,
            }));

            // Put new object (creates versioned version)
            const putResult = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'versioned',
            }));

            assert(putResult.VersionId);
            assert.notEqual(putResult.VersionId, 'null');

            // Should have both null version and new version
            const listResult = await s3.send(new ListObjectVersionsCommand({
                Bucket: bucket,
            }));

            assert.strictEqual(listResult.Versions.length, 2);
            const versionIds = listResult.Versions.map(v => v.VersionId);
            assert(versionIds.includes('null'));
            assert(versionIds.includes(putResult.VersionId));
        });

        it('should handle different content types and metadata', async () => {
            const key = 'key';
            
            // Put JSON version
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: JSON.stringify({ type: 'json' }),
                ContentType: 'application/json',
                Metadata: { format: 'json' },
            }));

            // Put XML version
            const putResult2 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: '<root><type>xml</type></root>',
                ContentType: 'application/xml',
                Metadata: { format: 'xml' },
            }));

            // Put text version
            const putResult3 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'plain text content',
                ContentType: 'text/plain',
                Metadata: { format: 'text' },
            }));

            // Verify all versions exist with correct metadata
            const versions = [putResult1, putResult2, putResult3];
            const expectedFormats = ['json', 'xml', 'text'];
            const expectedTypes = ['application/json', 'application/xml', 'text/plain'];

            for (let i = 0; i < versions.length; i++) {
                const getResult = await s3.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: versions[i].VersionId,
                }));

                assert.strictEqual(getResult.ContentType, expectedTypes[i]);
                assert.strictEqual(getResult.Metadata.format, expectedFormats[i]);
            }
        });

        it('should handle large objects across versions', async () => {
            const key = 'key';
            const largeBody1 = 'a'.repeat(10000);
            const largeBody2 = 'b'.repeat(15000);
            
            // Put first large version
            const putResult1 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: largeBody1,
            }));

            // Put second large version
            const putResult2 = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: largeBody2,
            }));

            // Verify both versions exist with correct sizes
            const getResult1 = await s3.send(new GetObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: putResult1.VersionId,
            }));

            assert.strictEqual(getResult1.ContentLength, 10000);

            const getResult2 = await s3.send(new GetObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: putResult2.VersionId,
            }));

            assert.strictEqual(getResult2.ContentLength, 15000);
        });

        it('should handle concurrent puts to same key', async () => {
            const key = 'key';
            
            // Put multiple versions concurrently
            const putPromises = [];
            for (let i = 0; i < 5; i++) {
                putPromises.push(s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: `version-${i}`,
                    Metadata: { index: i.toString() },
                })));
            }

            const putResults = await Promise.all(putPromises);

            // All should have unique version ids
            const versionIds = putResults.map(r => r.VersionId);
            const uniqueVersionIds = new Set(versionIds);
            assert.strictEqual(uniqueVersionIds.size, 5);

            // All versions should exist
            const listResult = await s3.send(new ListObjectVersionsCommand({
                Bucket: bucket,
            }));

            assert.strictEqual(listResult.Versions.length, 5);

            // Verify each version has correct content
            for (let i = 0; i < putResults.length; i++) {
                const getResult = await s3.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: putResults[i].VersionId,
                }));

                const body = await getResult.Body.transformToString();
                assert.strictEqual(body, `version-${i}`);
                assert.strictEqual(getResult.Metadata.index, i.toString());
            }
        });
    });
});
