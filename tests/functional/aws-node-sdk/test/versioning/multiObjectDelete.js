const assert = require('assert');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    DeleteObjectsCommand,
    ListObjectVersionsCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { removeAllVersions } = require('../../lib/utility/versioning-util');

const bucketName = `multi-object-delete-${Date.now()}`;
const key = 'key';
// formats differ for AWS and S3, use respective sample ids to obtain
// correct error response in tests
const nonExistingId = process.env.AWS_ON_AIR ?
    'MhhyTHhmZ4cxSi4Y9SMe5P7UJAz7HLJ9' :
    '3939393939393939393936493939393939393939756e6437';

function sortList(list) {
    return list.sort((a, b) => {
        if (a.Key > b.Key) {
            return 1;
        }
        if (a.Key < b.Key) {
            return -1;
        }
        return 0;
    });
}


describe('Multi-Object Versioning Delete Success', function success() {
    this.timeout(360000);

    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let objectsRes;

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucketName,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            }));

            const objects = [];
            for (let i = 1; i < 1001; i++) {
                objects.push(`${key}${i}`);
            }

            // Create objects in batches of 20 concurrently
            const results = [];
            for (let i = 0; i < objects.length; i += 20) {
                const batch = objects.slice(i, i + 20);
                const batchPromises = batch.map(async keyName => {
                    const res = await s3.send(new PutObjectCommand({
                        Bucket: bucketName,
                        Key: keyName,
                        Body: 'somebody',
                    }));
                    res.Key = keyName;
                    return res;
                });
                const batchResults = await Promise.all(batchPromises);
                results.push(...batchResults);
            }
            objectsRes = results;
        });


        afterEach(done => {
            removeAllVersions({ Bucket: bucketName }, err => {
                if (err) {
                    return done(err);
                }
                return s3.send(new DeleteBucketCommand({ Bucket: bucketName }))
                    .then(() => done()).catch(done);
            });
        });

        it('should batch delete 1000 objects quietly', async () => {
            const objects = objectsRes.slice(0, 1000).map(obj =>
                ({ Key: obj.Key, VersionId: obj.VersionId }));
            
            const res = await s3.send(new DeleteObjectsCommand({
                Bucket: bucketName,
                Delete: {
                    Objects: objects,
                    Quiet: true,
                },
            }));
            
            assert.strictEqual(res.Deleted, undefined);
            assert.strictEqual(res.Errors, undefined);
        });

        it('should batch delete 1000 objects', async () => {
            const objects = objectsRes.slice(0, 1000).map(obj =>
                ({ Key: obj.Key, VersionId: obj.VersionId }));
            
            const res = await s3.send(new DeleteObjectsCommand({
                Bucket: bucketName,
                Delete: {
                    Objects: objects,
                    Quiet: false,
                },
            }));
            
            assert.strictEqual(res.Deleted.length, 1000);
            // order of returned objects not sorted
            assert.deepStrictEqual(sortList(res.Deleted),
                sortList(objects));
            assert.strictEqual(res.Errors, undefined);
        });

        it('should return NoSuchVersion in errors if one versionId is ' +
        'invalid', async () => {
            const objects = objectsRes.slice(0, 1000).map(obj =>
                ({ Key: obj.Key, VersionId: obj.VersionId }));
            objects[0].VersionId = 'invalid-version-id';
            
            const res = await s3.send(new DeleteObjectsCommand({
                Bucket: bucketName,
                Delete: {
                    Objects: objects,
                },
            }));
            
            assert.strictEqual(res.Deleted.length, 999);
            assert.strictEqual(res.Errors.length, 1);
            assert.strictEqual(res.Errors[0].Code, 'NoSuchVersion');
        });

        it('should not send back any error if a versionId does not exist ' +
        'and should not create a new delete marker', async () => {
            const objects = objectsRes.slice(0, 1000).map(obj =>
                ({ Key: obj.Key, VersionId: obj.VersionId }));
            objects[0].VersionId = nonExistingId;
            
            const res = await s3.send(new DeleteObjectsCommand({
                Bucket: bucketName,
                Delete: {
                    Objects: objects,
                },
            }));
            
            assert.strictEqual(res.Deleted.length, 1000);
            assert.strictEqual(res.Errors, undefined);
            const foundVersionId = res.Deleted.find(entry =>
                entry.VersionId === nonExistingId);
            assert(foundVersionId);
            assert.strictEqual(foundVersionId.DeleteMarker, undefined);
        });

        it('should not crash when deleting a null versionId that does not exist', async () => {
            const objects = [{ Key: objectsRes[0].Key, VersionId: 'null' }];
            
            const res = await s3.send(new DeleteObjectsCommand({
                Bucket: bucketName,
                Delete: {
                    Objects: objects,
                },
            }));
            
            assert.deepStrictEqual(res.Deleted, [{ Key: objectsRes[0].Key, VersionId: 'null' }]);
            assert.strictEqual(res.Errors, undefined);
        });
    });
});

describe('Multi-Object Versioning Delete - deleting delete marker',
() => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucketName,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            }));
        });
        
         afterEach(done => {
            removeAllVersions({ Bucket: bucketName }, err => {
            if (err) {
                    return done(err);
                }
                return s3.send(new DeleteBucketCommand({ Bucket: bucketName }))
                .then(() => done()).catch(done);
            });
        });

        it('should send back VersionId and DeleteMarkerVersionId both equal ' +
        'to deleteVersionId', async () => {
            await s3.send(new PutObjectCommand({ Bucket: bucketName, Key: key }));
            
            const deleteRes = await s3.send(new DeleteObjectCommand({ 
                Bucket: bucketName,
                Key: key 
            }));
            const deleteVersionId = deleteRes.VersionId;
            
            const deleteObjectsRes = await s3.send(new DeleteObjectsCommand({ 
                Bucket: bucketName,
                Delete: {
                    Objects: [
                        {
                            Key: key,
                            VersionId: deleteVersionId,
                        },
                    ],
                } 
            }));
            
            assert.strictEqual(deleteObjectsRes.Deleted[0].DeleteMarker, true);
            assert.strictEqual(deleteObjectsRes.Deleted[0].VersionId, deleteVersionId);
            assert.strictEqual(deleteObjectsRes.Deleted[0].DeleteMarkerVersionId, deleteVersionId);
        });

        it('should send back a DeleteMarkerVersionId matching the versionId ' +
      'stored for the object if trying to delete an object that does not exist',
        async () => {
            const deleteRes = await s3.send(new DeleteObjectsCommand({ 
                Bucket: bucketName,
                Delete: {
                    Objects: [
                        {
                            Key: key,
                        },
                    ],
                } 
            }));
            
            const versionIdFromDeleteObjects = deleteRes.Deleted[0].DeleteMarkerVersionId;
            assert.strictEqual(deleteRes.Deleted[0].DeleteMarker, true);
            
            const listRes = await s3.send(new ListObjectVersionsCommand({ Bucket: bucketName }));
            const versionIdFromListObjectVersions = listRes.DeleteMarkers[0].VersionId;
            assert.strictEqual(versionIdFromDeleteObjects, versionIdFromListObjectVersions);
        });

        it('should send back a DeleteMarkerVersionId matching the versionId ' +
        'stored for the object if object exists but no version was specified',
        async () => {
            const putRes = await s3.send(new PutObjectCommand({ Bucket: bucketName, Key: key }));
            const versionId = putRes.VersionId;
            
            const deleteRes = await s3.send(new DeleteObjectsCommand({ 
                Bucket: bucketName,
                Delete: {
                    Objects: [
                        {
                            Key: key,
                        },
                    ],
                } 
            }));
            
            assert.strictEqual(deleteRes.Deleted[0].DeleteMarker, true);
            const deleteVersionId = deleteRes.Deleted[0].DeleteMarkerVersionId;
            assert.notEqual(deleteVersionId, versionId);
            
            const listRes = await s3.send(new ListObjectVersionsCommand({ Bucket: bucketName }));
            assert.strictEqual(deleteVersionId, listRes.DeleteMarkers[0].VersionId);
            assert.strictEqual(versionId, listRes.Versions[0].VersionId);
        });
    });
});
