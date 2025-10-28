const assert = require('assert');
const async = require('async');
const { promisify } = require('util');

const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    UploadPartCopyCommand,
    CreateMultipartUploadCommand,
    AbortMultipartUploadCommand,
    ListObjectVersionsCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const {
    removeAllVersions,
    versioningEnabled,
    versioningSuspended,
} = require('../../lib/utility/versioning-util.js');

const removeAllVersionsPromise = promisify(removeAllVersions);
let sourceBucket;
let destBucket;
const sourceKey = 'sourceobjectkey';
const destKey = 'destobjectkey';
const invalidId = 'invalidIdWithMoreThan40BytesAndThatIsNotLongEnoughYet';


describe('Object Part Copy with Versioning', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let uploadId;

        beforeEach(done => {
            sourceBucket = `copypartsourcebucket-${Date.now()}`;
            destBucket = `copypartdestbucket-${Date.now()}`;
            async.forEach([sourceBucket, destBucket], (bucket, cb) => {
                s3.send(new CreateBucketCommand({ Bucket: bucket }))
                    .then(() => cb())
                    .catch(cb);
            }, done);
        });

        afterEach(done => {
            s3.send(new AbortMultipartUploadCommand({
                Bucket: destBucket,
                Key: destKey,
                UploadId: uploadId,
            }))
                .then(() => {
                    async.each([sourceBucket, destBucket], (bucket, cb) => {
                        removeAllVersionsPromise({ Bucket: bucket })
                            .then(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })))
                            .then(() => cb())
                            .catch(cb);
                    }, done);
                })
                .catch(err => {
                    if (err) {
                        return done(err);
                    }
                    return done();
                });
        });

        describe('on bucket without versioning', () => {
            const eTags = [];

            beforeEach(done => {
                async.waterfall([
                    next => s3.send(new PutObjectCommand({ 
                        Bucket: sourceBucket, 
                        Key: sourceKey,
                        Body: 'foobar' 
                    }))
                        .then(data => next(null, data))
                        .catch(next),
                    (data, next) => {
                        eTags.push(data.ETag);
                        s3.send(new CreateMultipartUploadCommand({ 
                            Bucket: destBucket,
                            Key: destKey 
                        }))
                            .then(data => next(null, data))
                            .catch(next);
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

            it('should not return a version id when put part by copying ' +
            'without specifying version id', done => {
                s3.send(new UploadPartCopyCommand({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }))
                    .then(data => {
                        assert.strictEqual(data.CopySourceVersionId, undefined);
                        assert.strictEqual(data.CopyPartResult.ETag, eTags[0]);
                        done();
                    })
                    .catch(done);
            });

            it('should return NoSuchKey if copy source version id is invalid ' +
            'id', done => {
                s3.send(new UploadPartCopyCommand({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}?` +
                    `versionId=${invalidId}`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }))
                    .then(() => {
                        done(new Error('Expected error but got success'));
                    })
                    .catch(err => {
                        assert(err, `Expected err but got ${err}`);
                        assert.strictEqual(err.name, 'InvalidArgument');
                        assert.strictEqual(err.$metadata?.httpStatusCode, 400);
                        done();
                    });
            });

            it('should allow specific version "null" for copy source ' +
            'and return version id "null" in response headers', done => {
                s3.send(new UploadPartCopyCommand({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}?versionId=null`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }))
                    .then(data => {
                        assert.strictEqual(data.CopySourceVersionId, 'null');
                        assert.strictEqual(data.CopyPartResult.ETag, eTags[0]);
                        done();
                    })
                    .catch(done);
            });
        });

        describe('on bucket with versioning', () => {
            const eTags = [];
            const versionIds = [];
            const counter = 10;

            beforeEach(done => {
                const params = { Bucket: sourceBucket, Key: sourceKey };
                async.waterfall([
                    next => s3.send(new PutObjectCommand(params))
                        .then(data => next(null, data))
                        .catch(next),
                    (data, next) => {
                        eTags.push(data.ETag);
                        versionIds.push('null');
                        s3.send(new PutBucketVersioningCommand({
                            Bucket: sourceBucket,
                            VersioningConfiguration: versioningEnabled,
                        }))
                            .then(() => next())
                            .catch(next);
                    },
                    next => async.timesSeries(counter, (i, cb) =>
                        s3.send(new PutObjectCommand({ 
                            Bucket: sourceBucket, 
                            Key: sourceKey,
                            Body: `foo${i}` 
                        }))
                            .then(data => {
                                eTags.push(data.ETag);
                                versionIds.push(data.VersionId);
                                cb();
                            })
                            .catch(cb),
                        err => next(err)),
                    next => s3.send(new CreateMultipartUploadCommand({ 
                        Bucket: destBucket,
                        Key: destKey 
                    }))
                        .then(data => next(null, data))
                        .catch(next),
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

            it('copy part without specifying version should return data and ' +
            'version id of latest version', done => {
                const lastVersion = versionIds[versionIds.length - 1];
                const lastETag = eTags[eTags.length - 1];
                s3.send(new UploadPartCopyCommand({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }))
                    .then(data => {
                        assert.strictEqual(data.CopySourceVersionId, lastVersion);
                        assert.strictEqual(data.CopyPartResult.ETag, lastETag);
                        done();
                    })
                    .catch(done);
            });

            it('copy part without specifying version should return NoSuchKey ' +
            'if latest version has a delete marker', done => {
                s3.send(new DeleteObjectCommand({ 
                    Bucket: sourceBucket, 
                    Key: sourceKey 
                }))
                    .then(() => s3.send(new UploadPartCopyCommand({
                            Bucket: destBucket,
                            CopySource: `${sourceBucket}/${sourceKey}`,
                            Key: destKey,
                            PartNumber: 1,
                            UploadId: uploadId,
                        })))
                    .then(() => {
                        done(new Error('Expected err but did not find one'));
                    })
                    .catch(err => {
                        assert(err, 'Expected err but did not find one');
                        assert.strictEqual(err.name, 'NoSuchKey');
                        assert.strictEqual(err.$metadata?.httpStatusCode, 404);
                        done();
                    });
            });

            it('copy part with specific version id should return ' +
            'InvalidRequest if that id is a delete marker', done => {
                async.waterfall([
                    next => s3.send(new DeleteObjectCommand({
                        Bucket: sourceBucket,
                        Key: sourceKey,
                    }))
                        .then(() => next())
                        .catch(next),
                    next => s3.send(new ListObjectVersionsCommand({ 
                        Bucket: sourceBucket 
                    }))
                        .then(data => next(null, data))
                        .catch(next),
                    (data, next) => {
                        const deleteMarkerId = data.DeleteMarkers[0].VersionId;
                        return s3.send(new UploadPartCopyCommand({
                            Bucket: destBucket,
                            CopySource: `${sourceBucket}/${sourceKey}` +
                            `?versionId=${deleteMarkerId}`,
                            Key: destKey,
                            PartNumber: 1,
                            UploadId: uploadId,
                        }))
                            .then(data => next(null, data))
                            .catch(next);
                    },
                ], err => {
                    assert(err, 'Expected err but did not find one');
                    assert.strictEqual(err.name, 'InvalidRequest');
                    assert.strictEqual(err.$metadata?.httpStatusCode, 400);
                    done();
                });
            });

            it('copy part with specific version should return NoSuchVersion ' +
            'if version does not exist', done => {
                const versionId = versionIds[1];
                s3.send(new DeleteObjectCommand({ 
                    Bucket: sourceBucket, 
                    Key: sourceKey,
                    VersionId: versionId 
                }))
                    .then(data => {
                        assert.strictEqual(data.VersionId, versionId);
                        return s3.send(new UploadPartCopyCommand({
                            Bucket: destBucket,
                            CopySource: `${sourceBucket}/${sourceKey}` +
                             `?versionId=${versionId}`,
                            Key: destKey,
                            PartNumber: 1,
                            UploadId: uploadId,
                        }));
                    })
                    .then(() => {
                        done(new Error('Expected err but did not find one'));
                    })
                    .catch(err => {
                        assert(err, 'Expected err but did not find one');
                        assert.strictEqual(err.name, 'NoSuchVersion');
                        assert.strictEqual(err.$metadata?.httpStatusCode, 404);
                        done();
                    });
            });

            it('copy part with specific version should return copy source ' +
            'version id if it exists', done => {
                const versionId = versionIds[1];
                s3.send(new UploadPartCopyCommand({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}` +
                     `?versionId=${versionId}`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }))
                    .then(data => {
                        assert.strictEqual(data.CopySourceVersionId, versionId);
                        assert.strictEqual(data.CopyPartResult.ETag, eTags[1]);
                        done();
                    })
                    .catch(done);
            });

            it('copy part with specific version "null" should return copy ' +
            'source version id "null" if it exists', done => {
                s3.send(new UploadPartCopyCommand({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}?versionId=null`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }))
                    .then(data => {
                        assert.strictEqual(data.CopySourceVersionId, 'null');
                        assert.strictEqual(data.CopyPartResult.ETag, eTags[0]);
                        done();
                    })
                    .catch(done);
            });
        });

        describe('on bucket with versioning suspended', () => {
            const eTags = []; // or eTag = ....
            const versionIds = [];
            const counter = 10;

            beforeEach(done => {
                const params = { Bucket: sourceBucket, Key: sourceKey };
                async.waterfall([
                    next => s3.send(new PutObjectCommand(params))
                        .then(data => next(null, data))
                        .catch(next),
                    (data, next) => {
                        eTags.push(data.ETag);
                        versionIds.push('null');
                        s3.send(new PutBucketVersioningCommand({
                            Bucket: sourceBucket,
                            VersioningConfiguration: versioningEnabled,
                        }))
                            .then(() => next())
                            .catch(next);
                    },
                    next => async.timesSeries(counter, (i, cb) =>
                        s3.send(new PutObjectCommand({ 
                            Bucket: sourceBucket, 
                            Key: sourceKey,
                            Body: `foo${i}` 
                        }))
                            .then(data => {
                                eTags.push(data.ETag);
                                versionIds.push(data.VersionId);
                                cb();
                            })
                            .catch(cb),
                        err => next(err)),
                    next => {
                        s3.send(new PutBucketVersioningCommand({
                            Bucket: sourceBucket,
                            VersioningConfiguration: versioningSuspended,
                        }))
                            .then(() => next())
                            .catch(next);
                    },
                    next => s3.send(new CreateMultipartUploadCommand({ 
                        Bucket: destBucket,
                        Key: destKey 
                    }))
                        .then(data => next(null, data))
                        .catch(next),
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

            it('copy part without specifying version should still return ' +
            'version id of latest version', done => {
                const lastVersion = versionIds[versionIds.length - 1];
                const lastETag = eTags[eTags.length - 1];
                s3.send(new UploadPartCopyCommand({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }))
                    .then(data => {
                        assert.strictEqual(data.CopySourceVersionId, lastVersion);
                        assert.strictEqual(data.CopyPartResult.ETag, lastETag);
                        done();
                    })
                    .catch(done);
            });

            it('copy part with specific version should still return copy ' +
            'source version id if it exists', done => {
                const versionId = versionIds[1];
                s3.send(new UploadPartCopyCommand({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}` +
                     `?versionId=${versionId}`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }))
                    .then(data => {
                        assert.strictEqual(data.CopySourceVersionId, versionId);
                        assert.strictEqual(data.CopyPartResult.ETag, eTags[1]);
                        done();
                    })
                    .catch(done);
            });

            it('copy part with specific version "null" should still return ' +
            'copy source version id "null" if it exists', done => {
                s3.send(new UploadPartCopyCommand({
                    Bucket: destBucket,
                    CopySource: `${sourceBucket}/${sourceKey}?versionId=null`,
                    Key: destKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }))
                    .then(data => {
                        assert.strictEqual(data.CopySourceVersionId, 'null');
                        assert.strictEqual(data.CopyPartResult.ETag, eTags[0]);
                        done();
                    })
                    .catch(done);
            });
        });
    });
});
