const assert = require('assert');
const async = require('async');

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

function checkNoError(err) {
    expect(err).toEqual(null);
}

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


describe('Multi-Object Versioning Delete Success', () => {
    this.timeout(360000);

    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let objectsRes;

        beforeEach(done => {
            async.waterfall([
                next => s3.createBucket({ Bucket: bucketName },
                    err => next(err)),
                next => s3.putBucketVersioning({
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    },
                }, err => next(err)),
                next => {
                    const objects = [];
                    for (let i = 1; i < 1001; i++) {
                        objects.push(`${key}${i}`);
                    }
                    async.mapLimit(objects, 20, (key, next) => {
                        s3.putObject({
                            Bucket: bucketName,
                            Key: key,
                            Body: 'somebody',
                        }, (err, res) => {
                            if (err) {
                                return next(err);
                            }
                            // eslint-disable-next-line no-param-reassign
                            res.Key = key;
                            return next(null, res);
                        });
                    }, (err, results) => {
                        if (err) {
                            return next(err);
                        }
                        objectsRes = results;
                        return next();
                    });
                },
            ], err => done(err));
        });

        afterEach(done => {
            removeAllVersions({ Bucket: bucketName }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucketName }, err => {
                    expect(err).toBe(null);
                    return done();
                });
            });
        });

        test('should batch delete 1000 objects quietly', () => {
            const objects = objectsRes.slice(0, 1000).map(obj =>
                ({ Key: obj.Key, VersionId: obj.VersionId }));
            return s3.deleteObjectsAsync({
                Bucket: bucketName,
                Delete: {
                    Objects: objects,
                    Quiet: true,
                },
            }).then(res => {
                expect(res.Deleted.length).toBe(0);
                expect(res.Errors.length).toBe(0);
            }).catch(err => {
                checkNoError(err);
            });
        });

        test('should batch delete 1000 objects', () => {
            const objects = objectsRes.slice(0, 1000).map(obj =>
                ({ Key: obj.Key, VersionId: obj.VersionId }));
            return s3.deleteObjectsAsync({
                Bucket: bucketName,
                Delete: {
                    Objects: objects,
                    Quiet: false,
                },
            }).then(res => {
                expect(res.Deleted.length).toBe(1000);
                // order of returned objects not sorted
                assert.deepStrictEqual(sortList(res.Deleted),
                    sortList(objects));
                expect(res.Errors.length).toBe(0);
            }).catch(err => {
                checkNoError(err);
            });
        });

        test('should return NoSuchVersion in errors if one versionId is ' +
        'invalid', () => {
            const objects = objectsRes.slice(0, 1000).map(obj =>
                ({ Key: obj.Key, VersionId: obj.VersionId }));
            objects[0].VersionId = 'invalid-version-id';
            return s3.deleteObjectsAsync({
                Bucket: bucketName,
                Delete: {
                    Objects: objects,
                },
            }).then(res => {
                expect(res.Deleted.length).toBe(999);
                expect(res.Errors.length).toBe(1);
                expect(res.Errors[0].Code).toBe('NoSuchVersion');
            })
            .catch(err => {
                checkNoError(err);
            });
        });

        test('should not send back any error if a versionId does not exist ' +
        'and should not create a new delete marker', () => {
            const objects = objectsRes.slice(0, 1000).map(obj =>
                ({ Key: obj.Key, VersionId: obj.VersionId }));
            objects[0].VersionId = nonExistingId;
            return s3.deleteObjectsAsync({
                Bucket: bucketName,
                Delete: {
                    Objects: objects,
                },
            }).then(res => {
                expect(res.Deleted.length).toBe(1000);
                expect(res.Errors.length).toBe(0);
                const foundVersionId = res.Deleted.find(entry =>
                    entry.VersionId === nonExistingId);
                expect(foundVersionId).toBeTruthy();
                expect(foundVersionId.DeleteMarker).toBe(undefined);
            })
            .catch(err => {
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
                next => s3.createBucket({ Bucket: bucketName },
                    err => next(err)),
                next => s3.putBucketVersioningAsync({
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    },
                }, err => next(err)),
            ], done);
        });
        afterEach(done => {
            removeAllVersions({ Bucket: bucketName }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucketName }, err => {
                    expect(err).toBe(null);
                    return done();
                });
            });
        });

        test('should send back VersionId and DeleteMarkerVersionId both equal ' +
        'to deleteVersionId', done => {
            async.waterfall([
                next => s3.putObject({ Bucket: bucketName, Key: key },
                  err => next(err)),
                next => s3.deleteObject({ Bucket: bucketName,
                    Key: key }, (err, data) => {
                    const deleteVersionId = data.VersionId;
                    next(err, deleteVersionId);
                }),
                (deleteVersionId, next) => s3.deleteObjects({ Bucket:
                  bucketName,
                    Delete: {
                        Objects: [
                            {
                                Key: key,
                                VersionId: deleteVersionId,
                            },
                        ],
                    } }, (err, data) => {
                    expect(data.Deleted[0].DeleteMarker).toBe(true);
                    expect(data.Deleted[0].VersionId).toBe(deleteVersionId);
                    expect(data.Deleted[0].DeleteMarkerVersionId).toBe(deleteVersionId);
                    next(err);
                }),
            ], err => done(err));
        });

        test('should send back a DeleteMarkerVersionId matching the versionId ' +
      'stored for the object if trying to delete an object that does not exist', done => {
            s3.deleteObjects({ Bucket: bucketName,
                Delete: {
                    Objects: [
                        {
                            Key: key,
                        },
                    ],
                } }, (err, data) => {
                if (err) {
                    return done(err);
                }
                const versionIdFromDeleteObjects =
                  data.Deleted[0].DeleteMarkerVersionId;
                expect(data.Deleted[0].DeleteMarker).toBe(true);
                return s3.listObjectVersions({ Bucket: bucketName },
                  (err, data) => {
                      if (err) {
                          return done(err);
                      }
                      const versionIdFromListObjectVersions =
                        data.DeleteMarkers[0].VersionId;
                      expect(versionIdFromDeleteObjects).toBe(versionIdFromListObjectVersions);
                      return done();
                  });
            });
        });

        test('should send back a DeleteMarkerVersionId matching the versionId ' +
        'stored for the object if object exists but no version was specified', done => {
            async.waterfall([
                next => s3.putObject({ Bucket: bucketName, Key: key },
                  (err, data) => {
                      const versionId = data.VersionId;
                      next(err, versionId);
                  }),
                (versionId, next) => s3.deleteObjects({ Bucket:
                  bucketName,
                    Delete: {
                        Objects: [
                            {
                                Key: key,
                            },
                        ],
                    } }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    expect(data.Deleted[0].DeleteMarker).toBe(true);
                    const deleteVersionId = data.Deleted[0].
                    DeleteMarkerVersionId;
                    expect(deleteVersionId).not.toEqual(versionId);
                    return next(err, deleteVersionId, versionId);
                }),
                (deleteVersionId, versionId, next) => s3.listObjectVersions(
                { Bucket: bucketName }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    expect(deleteVersionId).toBe(data.DeleteMarkers[0].VersionId);
                    expect(versionId).toBe(data.Versions[0].VersionId);
                    return next();
                }),
            ], err => done(err));
        });
    });
});
