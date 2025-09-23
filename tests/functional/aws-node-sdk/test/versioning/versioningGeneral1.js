const assert = require('assert');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    ListObjectsCommand,
    DeleteObjectCommand,
    ListObjectVersionsCommand,
    DeleteObjectsCommand,
} = require('@aws-sdk/client-s3');

const getConfig = require('../support/config');

const bucket = `versioning-bucket-${Date.now()}`;

function comp(v1, v2) {
    if (v1.Key > v2.Key) {
        return 1;
    }
    if (v1.Key < v2.Key) {
        return -1;
    }
    if (v1.VersionId > v2.VersionId) {
        return 1;
    }
    if (v1.VersionId < v2.VersionId) {
        return -1;
    }
    return 0;
}

const masterVersions = [];
const allVersions = [];

describe('Versioning: general', () => {
    let s3;

    before(async () => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
    });

    after(async () => {
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    });

    it('should accept valid versioning configuration', async () => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        };
        await s3.send(new PutBucketVersioningCommand(params));
    });

    it('should create a bunch of objects and their versions', async () => {
        const keycount = 20;
        const versioncount = 20;
        const value = '{"foo":"bar"}';
        
        // Process keys in batches to avoid overwhelming the server
        const batchSize = 10;
        for (let i = 0; i < keycount; i += batchSize) {
            const keyBatch = [];
            for (let j = i; j < Math.min(i + batchSize, keycount); j++) {
                const key = `foo${j}`;
                masterVersions.push(key);
                keyBatch.push(key);
            }
            
            // Create versions for each key in the batch
            await Promise.all(keyBatch.map(async key => {
                const params = { Bucket: bucket, Key: key, Body: value };
                
                // Create versions in batches to avoid overwhelming
                for (let v = 0; v < versioncount; v += batchSize) {
                    const versionPromises = [];
                    for (let k = v; k < Math.min(v + batchSize, versioncount); k++) {
                        versionPromises.push(
                            s3.send(new PutObjectCommand(params)).then(data => {
                                assert(data.VersionId, 'invalid versionId');
                                allVersions.push({ Key: key, VersionId: data.VersionId });
                                return data;
                            })
                        );
                    }
                    await Promise.all(versionPromises);
                }
            }));
        }
        
        assert.strictEqual(allVersions.length, keycount * versioncount);
    });

    it('should list all latest versions', async () => {
        const params = { Bucket: bucket, MaxKeys: 1000, Delimiter: '/' };
        const data = await s3.send(new ListObjectsCommand(params));
        const keys = data.Contents.map(entry => entry.Key);
        assert.deepStrictEqual(keys.sort(), masterVersions.sort(),
                'not same keys');
    });

    it('should return a sorted list with all versions of all objects', async () => {
        const params = { Bucket: bucket, MaxKeys: 1000 };
        const data = await s3.send(new ListObjectVersionsCommand(params));
        assert.strictEqual(data.Versions.length, allVersions.length);
        const sortedResponse = data.Versions.slice(0).sort(comp);
        const sortedVersions = allVersions.slice(0).sort(comp);
        assert.deepStrictEqual(sortedResponse, sortedVersions);
    });

    it('should be able to delete latest versions one by one', async () => {
        const keysAndVersions = allVersions.slice(0, 20);
        
        // Delete versions in batches
        const batchSize = 5;
        for (let i = 0; i < keysAndVersions.length; i += batchSize) {
            const batch = keysAndVersions.slice(i, i + batchSize);
            await Promise.all(batch.map(async keyAndVersion => {
                const params = {
                    Bucket: bucket,
                    Key: keyAndVersion.Key,
                    VersionId: keyAndVersion.VersionId,
                };
                await s3.send(new DeleteObjectCommand(params));
            }));
        }
        
        // Verify the deletions
        const params = { Bucket: bucket, MaxKeys: 1000 };
        const data = await s3.send(new ListObjectVersionsCommand(params));
        assert.strictEqual(data.Versions.length, allVersions.length - 20);
    });

    it('should be able to delete all versions for a given key name', async () => {
        const key = masterVersions[0];
        const versionsForKey = allVersions.filter(o => o.Key === key);
        
        await Promise.all(versionsForKey.map(async keyAndVersion => {
            const params = {
                Bucket: bucket,
                Key: keyAndVersion.Key,
                VersionId: keyAndVersion.VersionId,
            };
            await s3.send(new DeleteObjectCommand(params));
        }));
        
        // Verify the key no longer appears in listings
        const listParams = { Bucket: bucket, MaxKeys: 1000, Delimiter: '/' };
        const data = await s3.send(new ListObjectsCommand(listParams));
        const keys = data.Contents.map(entry => entry.Key);
        assert.strictEqual(keys.indexOf(key), -1);
    });

    it('should be able to bulk delete many versions efficiently', async () => {
        const remainingVersions = allVersions.filter(v => {
            // Filter out the 20 already deleted and the versions for the first key
            const isDeleted = allVersions.slice(0, 20).some(deleted => 
                deleted.Key === v.Key && deleted.VersionId === v.VersionId);
            const isFirstKey = v.Key === masterVersions[0];
            return !isDeleted && !isFirstKey;
        });
        
        // Bulk delete in batches of 100 (AWS limit is 1000 but we'll be conservative)
        const batchSize = 100;
        for (let i = 0; i < remainingVersions.length; i += batchSize) {
            const batch = remainingVersions.slice(i, i + batchSize);
            const deleteParams = {
                Bucket: bucket,
                Delete: {
                    Objects: batch.map(v => ({
                        Key: v.Key,
                        VersionId: v.VersionId,
                    })),
                },
            };
            await s3.send(new DeleteObjectsCommand(deleteParams));
        }
        
        // Verify all versions are deleted
        const params = { Bucket: bucket, MaxKeys: 1000 };
        const data = await s3.send(new ListObjectVersionsCommand(params));
        assert.strictEqual(data.Versions.length, 0);
    });
});
