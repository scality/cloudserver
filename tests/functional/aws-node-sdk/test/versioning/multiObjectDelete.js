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
const { DeleteObjectsCommand,
    DeleteObjectCommand,
    PutObjectCommand,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand, 
    ListObjectVersionsCommand} = require('@aws-sdk/client-s3');

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

        beforeEach(done => {
            async.waterfall([
                next => s3.send(new CreateBucketCommand({ Bucket: bucketName }),
                    err => next(err)),
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    },
                })).then(res => next(null, res)).catch(err => next(err)),
                next => {
                    const objects = [];
                    for (let i = 1; i < 1001; i++) {
                        objects.push(`${key}${i}`);
                    }
                    async.mapLimit(objects, 20, (key, next) => {
                        s3.send(new PutObjectCommand({
                            Bucket: bucketName,
                            Key: key,
                            Body: 'somebody',
                        })).then(res => {
                            // eslint-disable-next-line no-param-reassign
                            res.Key = key;
                            return next(null, res);
                        }).catch(err => next(err));
                    }, (err, results) => {
                        if (err) {
                            return next(err);
                        }
                        objectsRes = results;
                        return next();
                    });
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

        afterEach(async () => {
            await removeAllVersions({ Bucket: bucketName });
            await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
        });


        it('should batch delete 1000 objects quietly', () => {
            const objects = objectsRes.slice(0, 1000).map(obj =>
                ({ Key: obj.Key, VersionId: obj.VersionId }));
            return s3.send(new DeleteObjectsCommand({
                Bucket: bucketName,
                Delete: {
                    Objects: objects,
                    Quiet: true,
                },
            })).then(res => {
                assert.strictEqual(res.Deleted, undefined);
                assert.strictEqual(res.Errors, undefined);
            }).catch(err => {
                checkNoError(err);
            });
        });

        it('should batch delete 1000 objects', async () => {
            const objects = objectsRes.slice(0, 1000).map(obj =>
                ({ Key: obj.Key, VersionId: obj.VersionId }));
            return s3.send(new DeleteObjectsCommand({
                Bucket: bucketName,
                Delete: {
                    Objects: objects,
                    Quiet: false,
                },
            })).then(res => {
                assert.strictEqual(res.Deleted.length, 1000);
                // order of returned objects not sorted
                assert.deepStrictEqual(sortList(res.Deleted),
                    sortList(objects));
                assert.strictEqual(res.Errors, undefined);
            }).catch(err => {
                checkNoError(err);
            });
        });

        it('should return NoSuchVersion in errors if one versionId is ' +
        'invalid', async () => {
            const objects = objectsRes.slice(0, 1000).map(obj =>
                ({ Key: obj.Key, VersionId: obj.VersionId }));
            objects[0].VersionId = 'invalid-version-id';
            return s3.send(new DeleteObjectsCommand({
                Bucket: bucketName,
                Delete: {
                    Objects: objects,
                },
            })).then(res => {
                assert.strictEqual(res.Deleted.length, 999);
                assert.strictEqual(res.Errors.length, 1);
                assert.strictEqual(res.Errors[0].Code, 'NoSuchVersion');
            })
            .catch(err => {
                checkNoError(err);
            });
        });

        it('should not send back any error if a versionId does not exist ' +
        'and should not create a new delete marker', async () => {
            const objects = objectsRes.slice(0, 1000).map(obj =>
                ({ Key: obj.Key, VersionId: obj.VersionId }));
            objects[0].VersionId = nonExistingId;
            return s3.send(new DeleteObjectsCommand({
                Bucket: bucketName,
                Delete: {
                    Objects: objects,
                },
            })).then(res => {
                assert.strictEqual(res.Deleted.length, 1000);
                assert.strictEqual(res.Errors, undefined);
                const foundVersionId = res.Deleted.find(entry =>
                    entry.VersionId === nonExistingId);
                assert(foundVersionId);
                assert.strictEqual(foundVersionId.DeleteMarker, undefined);
            })
            .catch(err => {
                checkNoError(err);
            });
        });

        it('should not crash when deleting a null versionId that does not exist', async () => {
            const objects = [{ Key: objectsRes[0].Key, VersionId: 'null' }];
            return s3.send(new DeleteObjectsCommand({
                Bucket: bucketName,
                Delete: {
                    Objects: objects,
                },
            })).then(res => {
                assert.deepStrictEqual(res.Deleted, [{ Key: objectsRes[0].Key, VersionId: 'null' }]);
                assert.strictEqual(res.Errors, undefined);
            }).catch(err => {
                checkNoError(err);
            });
        });
    });
});

describe('Multi-Object Versioning Delete - deleting delete marker',
() => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        beforeEach(done => {
            async.waterfall([
                next => s3.send(new CreateBucketCommand({ Bucket: bucketName })).then(() => 
                    next()).catch(err => next(err)),
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    },
                })).then(() => next()).catch(err => next(err)),
            ], done);
        });
        afterEach(async () => {
            await removeAllVersions({ Bucket: bucketName });
            await s3.deleteBucket({ Bucket: bucketName });
        });

        it('should send back VersionId and DeleteMarkerVersionId both equal ' +
        'to deleteVersionId', async () => {
            await new Promise((resolve, reject) => {
                async.waterfall([
                    next => s3.send(new PutObjectCommand({ Bucket: bucketName, Key: key })).then(() => 
                        next()).catch(err => next(err)),
                    next => s3.send(new DeleteObjectCommand({ Bucket: bucketName,
                        Key: key })).then(data => {
                        const deleteVersionId = data.VersionId;
                        next(null, deleteVersionId);
                    }).catch(err => next(err)),
                    (deleteVersionId, next) => s3.send(new DeleteObjectsCommand({ Bucket:
                      bucketName,
                        Delete: {
                            Objects: [
                                {
                                    Key: key,
                                    VersionId: deleteVersionId,
                                },
                            ],
                        } })).then(data => {
                        assert.strictEqual(data.Deleted[0].DeleteMarker, true);
                        assert.strictEqual(data.Deleted[0].VersionId,
                          deleteVersionId);
                        assert.strictEqual(data.Deleted[0].DeleteMarkerVersionId,
                          deleteVersionId);
                        next(null, data);
                    }).catch(err => next(err)),
                ], err => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            });
        });

        it('should send back a DeleteMarkerVersionId matching the versionId ' +
      'stored for the object if trying to delete an object that does not exist',
        done => {
            s3.send(new DeleteObjectsCommand({ Bucket: bucketName,
                Delete: {
                    Objects: [
                        {
                            Key: key,
                        },
                    ],
                } })).then(data => {
                const versionIdFromDeleteObjects =
                  data.Deleted[0].DeleteMarkerVersionId;
                assert.strictEqual(data.Deleted[0].DeleteMarker, true);
                return s3.send(new ListObjectVersionsCommand({ Bucket: bucketName })).then(data => {
                    const versionIdFromListObjectVersions =
                        data.DeleteMarkers[0].VersionId;
                      assert.strictEqual(versionIdFromDeleteObjects,
                        versionIdFromListObjectVersions);
                      return done();
                  }).catch(err => done(err));
            }).catch(err => done(err));
        });

        it('should send back a DeleteMarkerVersionId matching the versionId ' +
        'stored for the object if object exists but no version was specified',
        done => {
            async.waterfall([
                next => s3.putObject({ Bucket: bucketName, Key: key }).then(data => {
                    const versionId = data.VersionId;
                    next(null, versionId);
                }).catch(err => next(err)),
                (versionId, next) => s3.send(new DeleteObjectsCommand({ Bucket: bucketName,
                    Delete: {
                        Objects: [
                            {
                                Key: key,
                            },
                        ],
                    } })).then(data => {
                    assert.strictEqual(data.Deleted[0].DeleteMarker, true);
                    const deleteVersionId = data.Deleted[0].
                    DeleteMarkerVersionId;
                    assert.notEqual(deleteVersionId, versionId);
                    return next(null, deleteVersionId, versionId);
                }).catch(err => next(err)),
                (deleteVersionId, versionId, next) => s3.send(new ListObjectVersionsCommand(
                { Bucket: bucketName })).then(data => {
                    assert.strictEqual(deleteVersionId,
                      data.DeleteMarkers[0].VersionId);
                    assert.strictEqual(versionId,
                      data.Versions[0].VersionId);
                    return next();
                }).catch(err => next(err)),
            ], err => done(err));
        });
    });
});
