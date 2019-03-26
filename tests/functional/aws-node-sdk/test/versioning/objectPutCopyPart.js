const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const {
    removeAllVersions,
    versioningEnabled,
    versioningSuspended,
} = require('../../lib/utility/versioning-util.js');

let sourceBucket;
let destBucket;
const sourceKey = 'sourceobjectkey';
const destKey = 'destobjectkey';
const invalidId = 'invalidId';

function _assertNoError(err, desc) {
    expect(err).toBe(null);
}


describe('Object Part Copy with Versioning', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let uploadId;

        beforeEach(done => {
            sourceBucket = `copypartsourcebucket-${Date.now()}`;
            destBucket = `copypartdestbucket-${Date.now()}`;
            async.forEach([sourceBucket, destBucket], (bucket, cb) => {
                s3.createBucket({ Bucket: bucket }, cb);
            }, done);
        });

        afterEach(done => {
            s3.abortMultipartUpload({
                Bucket: destBucket,
                Key: destKey,
                UploadId: uploadId,
            }, err => {
                if (err) {
                    return done(err);
                }
                return async.each([sourceBucket, destBucket], (bucket, cb) => {
                    removeAllVersions({ Bucket: bucket }, err => {
                        if (err) {
                            return cb(err);
                        }
                        return s3.deleteBucket({ Bucket: bucket }, cb);
                    });
                }, done);
            });
        });

        describe('on bucket without versioning', () => {
            const eTags = [];

            beforeEach(done => {
                async.waterfall([
                    next => s3.putObject({ Bucket: sourceBucket, Key: sourceKey,
                        Body: 'foobar' }, next),
                    (data, next) => {
                        eTags.push(data.ETag);
                        s3.createMultipartUpload({ Bucket: destBucket,
                            Key: destKey }, next);
                    },
                ], (err, data) => {
                    if (err) {
                        return done(err);
                    }
                    uploadId = data.UploadId;
                    return done();
                });
            });

            afterEach(done => {
                eTags.length = 0;
                done();
            });

            test('should not return a version id when put part by copying ' +
            'without specifying version id', done => {
                s3.uploadPartCopy({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }, (err, data) => {
                    _assertNoError(err, 'uploading part copy w/o version id');
                    expect(data.CopySourceVersionId).toBe(undefined);
                    expect(data.CopyPartResult.ETag).toBe(eTags[0]);
                    done();
                });
            });

            test('should return NoSuchKey if copy source version id is invalid ' +
            'id', done => {
                s3.uploadPartCopy({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}?` +
                    `versionId=${invalidId}`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }, err => {
                    expect(err).toBeTruthy();
                    expect(err.code).toBe('InvalidArgument');
                    expect(err.statusCode).toBe(400);
                    done();
                });
            });

            test('should allow specific version "null" for copy source ' +
            'and return version id "null" in response headers', done => {
                s3.uploadPartCopy({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}?versionId=null`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }, (err, data) => {
                    _assertNoError(err,
                        'using specific version "null" for copy source');
                    expect(data.CopySourceVersionId).toBe('null');
                    expect(data.ETag).toBe(eTags[0]);
                    done();
                });
            });
        });

        describe('on bucket with versioning', () => {
            const eTags = [];
            const versionIds = [];
            const counter = 10;

            beforeEach(done => {
                const params = { Bucket: sourceBucket, Key: sourceKey };
                async.waterfall([
                    next => s3.putObject(params, next),
                    (data, next) => {
                        eTags.push(data.ETag);
                        versionIds.push('null');
                        s3.putBucketVersioning({
                            Bucket: sourceBucket,
                            VersioningConfiguration: versioningEnabled,
                        }, err => next(err));
                    },
                    next => async.timesSeries(counter, (i, cb) =>
                        s3.putObject({ Bucket: sourceBucket, Key: sourceKey,
                            Body: `foo${i}` }, (err, data) => {
                            _assertNoError(err, `putting version #${i}`);
                            eTags.push(data.ETag);
                            versionIds.push(data.VersionId);
                            cb(err);
                        }), err => next(err)),
                    next => s3.createMultipartUpload({ Bucket: destBucket,
                        Key: destKey }, next),
                ], (err, data) => {
                    if (err) {
                        return done(err);
                    }
                    uploadId = data.UploadId;
                    return done();
                });
            });

            afterEach(done => {
                eTags.length = 0;
                versionIds.length = 0;
                done();
            });

            test('copy part without specifying version should return data and ' +
            'version id of latest version', done => {
                const lastVersion = versionIds[versionIds.length - 1];
                const lastETag = eTags[eTags.length - 1];
                s3.uploadPartCopy({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }, (err, data) => {
                    _assertNoError(err, 'uploading part copy w/o version id');
                    expect(data.CopySourceVersionId).toBe(lastVersion);
                    expect(data.CopyPartResult.ETag).toBe(lastETag);
                    done();
                });
            });

            test('copy part without specifying version should return NoSuchKey ' +
            'if latest version has a delete marker', done => {
                s3.deleteObject({ Bucket: sourceBucket, Key: sourceKey },
                    err => {
                        _assertNoError(err, 'deleting latest version');
                        s3.uploadPartCopy({
                            Bucket: destBucket,
                            CopySource: `${sourceBucket}/${sourceKey}`,
                            Key: destKey,
                            PartNumber: 1,
                            UploadId: uploadId,
                        }, err => {
                            expect(err).toBeTruthy();
                            expect(err.code).toBe('NoSuchKey');
                            expect(err.statusCode).toBe(404);
                            done();
                        });
                    });
            });

            test('copy part with specific version id should return ' +
            'InvalidRequest if that id is a delete marker', done => {
                async.waterfall([
                    next => s3.deleteObject({
                        Bucket: sourceBucket,
                        Key: sourceKey,
                    }, err => next(err)),
                    next => s3.listObjectVersions({ Bucket: sourceBucket },
                        next),
                    (data, next) => {
                        const deleteMarkerId = data.DeleteMarkers[0].VersionId;
                        return s3.uploadPartCopy({
                            Bucket: destBucket,
                            CopySource: `${sourceBucket}/${sourceKey}` +
                            `?versionId=${deleteMarkerId}`,
                            Key: destKey,
                            PartNumber: 1,
                            UploadId: uploadId,
                        }, next);
                    },
                ], err => {
                    expect(err).toBeTruthy();
                    expect(err.code).toBe('InvalidRequest');
                    expect(err.statusCode).toBe(400);
                    done();
                });
            });

            test('copy part with specific version should return NoSuchVersion ' +
            'if version does not exist', done => {
                const versionId = versionIds[1];
                s3.deleteObject({ Bucket: sourceBucket, Key: sourceKey,
                    VersionId: versionId }, (err, data) => {
                    _assertNoError(err, `deleting version ${versionId}`);
                    expect(data.VersionId).toBe(versionId);
                    s3.uploadPartCopy({
                        Bucket: destBucket,
                        CopySource: `${sourceBucket}/${sourceKey}` +
                         `?versionId=${versionId}`,
                        Key: destKey,
                        PartNumber: 1,
                        UploadId: uploadId,
                    }, err => {
                        expect(err).toBeTruthy();
                        expect(err.code).toBe('NoSuchVersion');
                        expect(err.statusCode).toBe(404);
                        done();
                    });
                });
            });

            test('copy part with specific version should return copy source ' +
            'version id if it exists', done => {
                const versionId = versionIds[1];
                s3.uploadPartCopy({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}` +
                     `?versionId=${versionId}`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }, (err, data) => {
                    _assertNoError(err, 'copy part from specific version');
                    expect(data.CopySourceVersionId).toBe(versionId);
                    expect(data.CopyPartResult.ETag).toBe(eTags[1]);
                    done();
                });
            });

            test('copy part with specific version "null" should return copy ' +
            'source version id "null" if it exists', done => {
                s3.uploadPartCopy({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}?versionId=null`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }, (err, data) => {
                    _assertNoError(err, 'copy part from specific version');
                    expect(data.CopySourceVersionId).toBe('null');
                    expect(data.CopyPartResult.ETag).toBe(eTags[0]);
                    done();
                });
            });
        });

        describe('on bucket with versioning suspended', () => {
            const eTags = []; // or eTag = ....
            const versionIds = [];
            const counter = 10;

            beforeEach(done => {
                const params = { Bucket: sourceBucket, Key: sourceKey };
                async.waterfall([
                    next => s3.putObject(params, next),
                    (data, next) => {
                        eTags.push(data.ETag);
                        versionIds.push('null');
                        s3.putBucketVersioning({
                            Bucket: sourceBucket,
                            VersioningConfiguration: versioningEnabled,
                        }, err => next(err));
                    },
                    next => async.timesSeries(counter, (i, cb) =>
                        s3.putObject({ Bucket: sourceBucket, Key: sourceKey,
                            Body: `foo${i}` }, (err, data) => {
                            _assertNoError(err, `putting version #${i}`);
                            eTags.push(data.ETag);
                            versionIds.push(data.VersionId);
                            cb(err);
                        }), err => next(err)),
                    next => {
                        s3.putBucketVersioning({
                            Bucket: sourceBucket,
                            VersioningConfiguration: versioningSuspended,
                        }, err => next(err));
                    },
                    next => s3.createMultipartUpload({ Bucket: destBucket,
                        Key: destKey }, next),
                ], (err, data) => {
                    if (err) {
                        return done(err);
                    }
                    uploadId = data.UploadId;
                    return done();
                });
            });

            afterEach(done => {
                eTags.length = 0;
                versionIds.length = 0;
                done();
            });

            test('copy part without specifying version should still return ' +
            'version id of latest version', done => {
                const lastVersion = versionIds[versionIds.length - 1];
                const lastETag = eTags[eTags.length - 1];
                s3.uploadPartCopy({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }, (err, data) => {
                    _assertNoError(err, 'uploading part copy w/o version id');
                    expect(data.CopySourceVersionId).toBe(lastVersion);
                    expect(data.CopyPartResult.ETag).toBe(lastETag);
                    done();
                });
            });

            test('copy part with specific version should still return copy ' +
            'source version id if it exists', done => {
                const versionId = versionIds[1];
                s3.uploadPartCopy({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}` +
                     `?versionId=${versionId}`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }, (err, data) => {
                    _assertNoError(err, 'copy part from specific version');
                    expect(data.CopySourceVersionId).toBe(versionId);
                    expect(data.CopyPartResult.ETag).toBe(eTags[1]);
                    done();
                });
            });

            test('copy part with specific version "null" should still return ' +
            'copy source version id "null" if it exists', done => {
                s3.uploadPartCopy({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}?versionId=null`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }, (err, data) => {
                    _assertNoError(err, 'copy part from specific version');
                    expect(data.CopySourceVersionId).toBe('null');
                    expect(data.CopyPartResult.ETag).toBe(eTags[0]);
                    done();
                });
            });
        });
    });
});
