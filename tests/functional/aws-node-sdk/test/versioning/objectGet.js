const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const {
    removeAllVersions,
    versioningEnabled,
    versioningSuspended,
} = require('../../lib/utility/versioning-util.js');
const { CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    GetObjectCommand,
    DeleteObjectCommand,
    PutObjectTaggingCommand,
    ListObjectVersionsCommand,
 } = require('@aws-sdk/client-s3');

const key = 'objectKey';
// formats differ for AWS and S3, use respective sample ids to obtain
// correct error response in tests
const nonExistingId = process.env.AWS_ON_AIR ?
    'MhhyTHhmZ4cxSi4Y9SMe5P7UJAz7HLJ9' :
    '3939393939393939393936493939393939393939756e6437';

function _assertError(err, statusCode, code) {
    assert.notEqual(err, null,
        'Expected failure but got success');
    assert.strictEqual(err.name, code);
    assert.strictEqual(err.$metadata.httpStatusCode, statusCode);
}


describe('get behavior on versioning-enabled bucket', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let bucket;
        let versionId;

        beforeEach(async () => {
            bucket = `versioning-bucket-${Date.now()}`;
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucket,
                VersioningConfiguration: versioningEnabled,
            }));
        });

        afterEach(async () => {
            await removeAllVersions({ Bucket: bucket });
            await bucketUtil.empty(bucket);
            try {
                await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
            } catch (err) {
                if (err.name === 'BucketNotEmpty') {
                    // eslint-disable-next-line no-console
                    console.log('[DEBUG afterEach probeF] deleteBucket BucketNotEmpty, waiting 20s and re-listing',
                        JSON.stringify({ bucket }));
                    await new Promise(resolve => setTimeout(resolve, 20000));
                    try {
                        const data = await s3.send(new ListObjectVersionsCommand({ Bucket: bucket }));
                        // eslint-disable-next-line no-console
                        console.log('[DEBUG afterEach probeF] after 20s:', JSON.stringify({
                            bucket,
                            versions: (data.Versions || []).map(v => ({
                                Key: v.Key, VersionId: v.VersionId, IsLatest: v.IsLatest,
                            })),
                            deleteMarkers: (data.DeleteMarkers || []).map(v => ({
                                Key: v.Key, VersionId: v.VersionId, IsLatest: v.IsLatest,
                            })),
                        }));
                    } catch (listErr) {
                        // eslint-disable-next-line no-console
                        console.log('[DEBUG afterEach probeF] re-list error:', listErr.name);
                    }
                }
                throw err;
            }
        });

        describe('behavior when only version put is a regular version', () => {
            beforeEach(async () => {
                const data = await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key }));
                versionId = data.VersionId;
            });

            it('should be able to get the object version', async () => {
                const data = await s3.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: versionId,
                }));
                assert.strictEqual(data.ContentLength, 0);
            });

            it('it should return NoSuchVersion if try to get a non-existing object version', async () => {
                try {
                    await s3.send(new GetObjectCommand({
                        Bucket: bucket,
                        Key: key,
                        VersionId: nonExistingId,
                    }));
                    assert.fail('Expected NoSuchVersion error but got success');
                } catch (err) {
                    _assertError(err, 404, 'NoSuchVersion');
                }
            });

            it('it should return NoSuchVersion if try to get a non-existing null version', async () => {
                try {
                    await s3.send(new GetObjectCommand({
                        Bucket: bucket,
                        Key: key,
                        VersionId: 'null',
                    }));
                    assert.fail('Expected NoSuchVersion error but got success');
                } catch (err) {
                    _assertError(err, 404, 'NoSuchVersion');
                }
            });

            it('it should return NoSuchVersion if try to get a deleted noncurrent null version', async () => {
                await s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: versioningSuspended,
                }));
                await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key }));
                await s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: versioningEnabled,
                }));
                await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key }));
                await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key, VersionId: 'null' }));
                
                try {
                    await s3.send(new GetObjectCommand({
                        Bucket: bucket,
                        Key: key,
                        VersionId: 'null',
                    }));
                    assert.fail('Expected NoSuchVersion error but got success');
                } catch (err) {
                    _assertError(err, 404, 'NoSuchVersion');
                }
            });
        });

        describe('behavior when only version put is a delete marker', () => {
            let deleteVersionId;
            
            beforeEach(async () => {
                const deleteResult = await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
                deleteVersionId = deleteResult.VersionId;
            });

            it('should not be able to get a delete marker', async () => {
                try {
                    await s3.send(new GetObjectCommand({
                        Bucket: bucket,
                        Key: key,
                        VersionId: deleteVersionId,
                    }));
                    assert.fail('Expected MethodNotAllowed error but got success');
                } catch (err) {
                    _assertError(err, 405, 'MethodNotAllowed');
                    // Note: In AWS SDK v3, response headers are accessible through err.$response
                    const headers = err.$response?.headers || {};
                    assert.strictEqual(headers['x-amz-delete-marker'], 'true');
                }
            });

            it('it should return NoSuchKey if try to get object whose ' +
            'latest version is a delete marker', async () => {
                try {
                    await s3.send(new GetObjectCommand({
                        Bucket: bucket,
                        Key: key,
                    }));
                    assert.fail('Expected NoSuchKey error but got success');
                } catch (err) {
                    _assertError(err, 404, 'NoSuchKey');
                }
            });
        });

        describe('behavior when put version with content then put delete ' +
        'marker', () => {
            let putVersionId;
            let deleteVersionId;
            
            beforeEach(async () => {
                const putResult = await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key }));
                putVersionId = putResult.VersionId;
                const deleteResult = await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
                deleteVersionId = deleteResult.VersionId;
            });

            it('should not be able to get a delete marker', async () => {
                try {
                    await s3.send(new GetObjectCommand({
                        Bucket: bucket,
                        Key: key,
                        VersionId: deleteVersionId,
                    }));
                    assert.fail('Expected MethodNotAllowed error but got success');
                } catch (err) {
                    _assertError(err, 405, 'MethodNotAllowed');
                }
            });

            it('should be able to get a version that was put prior to the ' +
            'delete marker', async () => {
                const data = await s3.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: putVersionId
                }));
                assert.strictEqual(data.VersionId, putVersionId);
            });

            it('should return NoSuchKey if get object without version and ' +
            'latest version is a delete marker',
            async () => {
                try {
                    await s3.send(new GetObjectCommand({
                        Bucket: bucket,
                        Key: key,
                    }));
                    assert.fail('Expected NoSuchKey error but got success');
                } catch (err) {
                    _assertError(err, 404, 'NoSuchKey');
                }
            });
        });

        describe('x-amz-tagging-count with versioning', () => {
            let params;
            let paramsTagging;
            let objectVersionId;
            
            beforeEach(async () => {
                params = {
                    Bucket: bucket,
                    Key: key,
                };
                paramsTagging = {
                    Bucket: bucket,
                    Key: key,
                    Tagging: {
                        TagSet: [
                            {
                                Key: 'key1',
                                Value: 'value',
                            },
                        ],
                    },
                };
                const data = await s3.send(new PutObjectCommand(params));
                objectVersionId = data.VersionId;
            });

            it('should not return "x-amz-tagging-count" if no tag ' +
            'associated with the object',
            async () => {
                params.VersionId = objectVersionId;
                const data = await s3.send(new GetObjectCommand(params));
                assert.strictEqual(data.TagCount, undefined);
            });

            describe('tag associated with the object ', () => {
                beforeEach(async () => {
                    paramsTagging.VersionId = objectVersionId;
                    await s3.send(new PutObjectTaggingCommand(paramsTagging));
                });

                it('should return "x-amz-tagging-count" header that provides ' +
                'the count of number of tags associated with the object',
                async () => {
                    params.VersionId = objectVersionId;
                    const data = await s3.send(new GetObjectCommand(params));
                    assert.equal(data.TagCount, 1);
                });
            });
        });
    });
});

