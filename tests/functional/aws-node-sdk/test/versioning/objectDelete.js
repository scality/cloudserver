const assert = require('assert');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    DeleteObjectsCommand,
    GetObjectCommand,
    ListObjectVersionsCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const {
    versioningSuspended,
    versioningEnabled,
    removeAllVersions,
} = require('../../lib/utility/versioning-util.js');
const { promisify } = require('util');

const removeAllVersionsPromise = promisify(removeAllVersions);

const bucket = `versioning-bucket-${Date.now()}`;
const key = 'anObject';
// formats differ for AWS and S3, use respective sample ids to obtain
// correct error response in tests
const nonExistingId = process.env.AWS_ON_AIR ?
    'MhhyTHhmZ4cxSi4Y9SMe5P7UJAz7HLJ9' :
    '3939393939393939393936493939393939393939756e6437';

describe('delete marker creation in bucket with null version', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const nullVersionBody = 'nullversionbody';

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: nullVersionBody,
            }));
        });

        afterEach(async () => {
            try {
                await removeAllVersionsPromise({ Bucket: bucket });
                await bucketUtil.empty(bucket);
                await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
            } catch (err) {
                if (err.name !== 'NoSuchBucket') {
                    throw err;
                }
            }
        });

        it('should keep the null version if versioning enabled', async () => {
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucket,
                VersioningConfiguration: versioningEnabled,
            }));

            // List versions to check null version exists
            const listData = await s3.send(new ListObjectVersionsCommand({ Bucket: bucket }));
            assert.strictEqual(listData.Versions.length, 1);
            assert.strictEqual(listData.Versions[0].VersionId, 'null');

            // Delete object to create delete marker
            const deleteData = await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
            assert.strictEqual(deleteData.DeleteMarker, true);
            assert(deleteData.VersionId);

            // List versions again to verify null version still exists with delete marker
            const listData2 = await s3.send(new ListObjectVersionsCommand({ Bucket: bucket }));
            assert.strictEqual(listData2.Versions.length, 1);
            assert.strictEqual(listData2.Versions[0].VersionId, 'null');
            assert.strictEqual(listData2.DeleteMarkers[0].VersionId, deleteData.VersionId);
        });

        it('delete marker overwrites null version if versioning suspended',
        async () => {
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucket,
                VersioningConfiguration: versioningSuspended,
            }));

            // List versions to check null version exists
            const listData = await s3.send(new ListObjectVersionsCommand({ Bucket: bucket }));
            assert.strictEqual(listData.Versions.length, 1);
            assert.strictEqual(listData.Versions[0].VersionId, 'null');

            // Delete object to create delete marker
            const deleteData = await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
            assert.strictEqual(deleteData.DeleteMarker, true);
            assert.strictEqual(deleteData.VersionId, 'null');

            // List versions again to verify null version was overwritten
            const listData2 = await s3.send(new ListObjectVersionsCommand({ Bucket: bucket }));
            assert.strictEqual(listData2.Versions, undefined);
            assert.strictEqual(listData2.DeleteMarkers[0].VersionId, deleteData.VersionId);
        });
    });
});

describe('aws-node-sdk test delete object', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let versionIds;

        // setup test
        before(async () => {
            versionIds = [];
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        });

        // delete bucket after testing
        after(async () => {
            try {
                await removeAllVersionsPromise({ Bucket: bucket });
                await bucketUtil.empty(bucket);
                await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
            } catch (err) {
                if (err.name !== 'NoSuchBucket') {
                    throw err;
                }
            }
        });

        it('delete non existent object should not create a delete marker',
        async () => {
            const res = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: `${key}000`,
            }));
            assert.strictEqual(res.DeleteMarker, undefined);
            assert.strictEqual(res.VersionId, undefined);
        });

        it('creating non-versioned object', async () => {
            const res = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
            }));
            assert.equal(res.VersionId, undefined);
        });

        it('delete in non-versioned bucket should not create delete marker',
        async () => {
            const putRes = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
            }));
            assert.equal(putRes.VersionId, undefined);

            const deleteRes = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: `${key}2`,
            }));
            assert.strictEqual(deleteRes.DeleteMarker, undefined);
            assert.strictEqual(deleteRes.VersionId, undefined);
        });

        it('enable versioning', async () => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            };
            await s3.send(new PutBucketVersioningCommand(params));
        });

        it('should not send back error for non-existing key (specific version)',
            async () => {
                await s3.send(new DeleteObjectCommand({
                    Bucket: bucket,
                    Key: `${key}3`,
                    VersionId: 'null',
                }));
            });

        it('delete non existent object should create a delete marker', async () => {
            const res = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: `${key}2`,
            }));
            assert.strictEqual(res.DeleteMarker, true);
            assert.notEqual(res.VersionId, undefined);

            const res2 = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: `${key}2`,
            }));
            assert.strictEqual(res2.DeleteMarker, true);
            assert.notEqual(res2.VersionId, res.VersionId);

            await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: `${key}2`,
                VersionId: res.VersionId,
            }));

            await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: `${key}2`,
                VersionId: res2.VersionId,
            }));
        });

        it('delete non existent version should not create delete marker',
        async () => {
            const res = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: nonExistingId,
            }));
            assert.strictEqual(res.VersionId, nonExistingId);

            const listRes = await s3.send(new ListObjectVersionsCommand({ Bucket: bucket }));
            assert.strictEqual(listRes.DeleteMarkers?.length || 0, 0);
        });

        it('put a version to the object', async () => {
            const res = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'test',
            }));
            versionIds.push('null');
            versionIds.push(res.VersionId);
            assert.notEqual(res.VersionId, undefined);
        });

        it('should create a delete marker', async () => {
            const res = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
            }));
            assert.strictEqual(res.DeleteMarker, true);
            assert.strictEqual(
                versionIds.find(item => item === res.VersionId),
                undefined);
            versionIds.push(res.VersionId);
        });

        it('should return 404 with a delete marker', done => {
            s3.send(new GetObjectCommand({
                Bucket: bucket,
                Key: key,
            })).then(() => {
                done(new Error('should return 404'));
            }).catch(err => {
                assert.strictEqual(err.Code, 'NoSuchKey');
                done();
            });
        });

        it('should delete the null version', async () => {
            const version = versionIds.shift();
            const res = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: version,
            }));
            assert.strictEqual(res.VersionId, version);
            assert.equal(res.DeleteMarker, undefined);
        });

        it('should delete the versioned object', async () => {
            const version = versionIds.shift();
            const res = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: version,
            }));
            assert.strictEqual(res.VersionId, version);
            assert.equal(res.DeleteMarker, undefined);
        });

        it('should delete the delete-marker version', async () => {
            const version = versionIds.shift();
            const res = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: version,
            }));
            assert.strictEqual(res.VersionId, version);
            assert.equal(res.DeleteMarker, true);
            // In AWS SDK v3, the delete marker flag is sufficient for validation
            // The x-amz-delete-marker header is handled internally by the SDK
        });

        it('put a new version', async () => {
            const res = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'test',
            }));
            versionIds.push(res.VersionId);
            assert.notEqual(res.VersionId, undefined);
        });

        it('get the null version', async () => {
            try {
                await s3.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                }));
                throw new Error('should send back an error');
            } catch (err) {
                if (err.Code !== 'NoSuchVersion') {
                    throw err;
                }
            }
        });

        it('suspending versioning', async () => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Suspended',
                },
            };
            await s3.send(new PutBucketVersioningCommand(params));
        });

        it('delete non existent object should create a delete marker', async () => {
            const res = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: `${key}2`,
            }));
            assert.strictEqual(res.DeleteMarker, true);
            assert.notEqual(res.VersionId, undefined);

            const res2 = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: `${key}2`,
            }));
            assert.strictEqual(res2.DeleteMarker, true);
            assert.strictEqual(res2.VersionId, res.VersionId);

            await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: `${key}2`,
                VersionId: res.VersionId,
            }));
        });

        it('should put a new delete marker', async () => {
            const res = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
            }));
            assert.strictEqual(res.DeleteMarker, true);
            assert.strictEqual(res.VersionId, 'null');
        });

        it('enabling versioning', async () => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            };
            await s3.send(new PutBucketVersioningCommand(params));
        });

        it('should get the null version', done => {
            s3.send(new GetObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: 'null',
            })).then(() => {
                done('should return an error');
            }).catch(err => {
                if (err.Code !== 'MethodNotAllowed') {
                    return done(err);
                } else {
                    return done();
                }
            });
        });

        it('put a new version to store the null version', async () => {
            const res = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'test',
            }));
            versionIds.push(res.VersionId);
        });

        it('suspending versioning', async () => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Suspended',
                },
            };
            await s3.send(new PutBucketVersioningCommand(params));
        });

        it('put null version', async () => {
            const res = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'test-null-version',
            }));
            assert.strictEqual(res.VersionId, undefined);
        });

        it('enabling versioning', async () => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            };
            await s3.send(new PutBucketVersioningCommand(params));
        });

        it('should get the null version', async () => {
            const res = await s3.send(new GetObjectCommand({
                Bucket: bucket,
                Key: key,
            }));
            const body = await res.Body.transformToString();
            assert.strictEqual(body, 'test-null-version');
        });

        it('should add a delete marker', async () => {
            const res = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
            }));
            assert.strictEqual(res.DeleteMarker, true);
            versionIds.push(res.VersionId);
        });

        it('should get the null version', async () => {
            const res = await s3.send(new GetObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: 'null',
            }));
            const body = await res.Body.transformToString();
            assert.strictEqual(body, 'test-null-version');
        });

        it('should add a delete marker', async () => {
            const res = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
            }));
            assert.strictEqual(res.DeleteMarker, true);
            assert.strictEqual(
                versionIds.find(item => item === res.VersionId),
                undefined);
            versionIds.push(res.VersionId);
        });

        it('should set the null version as master', async () => {
            let version = versionIds.pop();
            const res = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: version,
            }));
            assert.strictEqual(res.VersionId, version);
            assert.strictEqual(res.DeleteMarker, true);
            
            version = versionIds.pop();
            const res2 = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: version,
            }));
            assert.strictEqual(res2.VersionId, version);
            assert.strictEqual(res2.DeleteMarker, true);
            
            const getRes = await s3.send(new GetObjectCommand({
                Bucket: bucket,
                Key: key,
            }));
            const body = await getRes.Body.transformToString();
            assert.strictEqual(body, 'test-null-version');
        });

        it('should delete null version', async () => {
            const res = await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: 'null',
            }));
            assert.strictEqual(res.VersionId, 'null');
            
            const getRes = await s3.send(new GetObjectCommand({
                Bucket: bucket,
                Key: key,
            }));
            assert.strictEqual(getRes.VersionId,
                versionIds[versionIds.length - 1]);
        });

        it('should be able to delete the bucket', async () => {
            for (const id of versionIds) {
                const res = await s3.send(new DeleteObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: id,
                }));
                assert.strictEqual(res.VersionId, id);
            }
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });
    });
});

describe('aws-node-sdk test concurrent version-specific deletes with null', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        before(() => s3.send(new CreateBucketCommand({ Bucket: bucket })));

        after(async () => {
            try {
                await removeAllVersionsPromise({ Bucket: bucket });
                await bucketUtil.empty(bucket);
                await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
            } catch (err) {
                if (err.name !== 'NoSuchBucket') {
                    throw err;
                }
            }
        });

        it('creating non-versioned object', async () => {
            const res = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: 'null-body',
            }));
            assert.equal(res.VersionId, undefined);
        });

        it('enable versioning', async () => {
            const params = {
                Bucket: bucket,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            };
            await s3.send(new PutBucketVersioningCommand(params));
        });

        it('put 5 new versions to the object', async () => {
            const promises = [];
            for (let i = 0; i < 5; i++) {
                promises.push(s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: `test-body-${i}`,
                })));
            }
            await Promise.all(promises);
        });

        it('list versions and batch-delete all except null version', async () => {
            const res = await s3.send(new ListObjectVersionsCommand({ Bucket: bucket }));
            assert.strictEqual(res.DeleteMarkers, undefined);
            assert.strictEqual(res.Versions.length, 6);
            assert.strictEqual(res.Versions[5].VersionId, 'null');
            
            await s3.send(new DeleteObjectsCommand({
                Bucket: bucket,
                Delete: {
                    Objects: res.Versions.slice(0, 5).map(item => ({
                        Key: item.Key,
                        VersionId: item.VersionId,
                    })),
                },
            }));
        });

        it('list versions should return a list with just the null version', async () => {
            const res = await s3.send(new ListObjectVersionsCommand({ Bucket: bucket }));
            assert.strictEqual(res.DeleteMarkers, undefined);
            assert.strictEqual(res.Versions.length, 1);
            assert.strictEqual(res.Versions[0].VersionId, 'null');
        });
    });
});
