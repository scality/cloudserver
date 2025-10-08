const assert = require('assert');
const async = require('async');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const {
    removeAllVersions,
    versioningEnabled,
    versioningSuspended,
} = require('../../lib/utility/versioning-util.js');
const { taggingTests } = require('../../lib/utility/tagging');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
    GetObjectCommand,
    PutBucketVersioningCommand,
    GetObjectTaggingCommand,
    NoSuchKey
} = require('@aws-sdk/client-s3');

const date = Date.now();
const bucket = `completempu${date}`;
const key = 'key';


describe('Complete MPU', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        function _completeMpuAndCheckVid(uploadId, eTag, expectedVid, cb) {
            let versionId;
            s3.send(new CompleteMultipartUploadCommand({
                Bucket: bucket,
                Key: key,
                MultipartUpload: {
                    Parts: [{ ETag: eTag, PartNumber: 1 }],
                },
                UploadId: uploadId 
            }))
            .then(data => {
                versionId = data.VersionId; // Assign to the outer scope variable
                if (expectedVid) {
                    assert.notEqual(versionId, undefined);
                } else {
                    assert.strictEqual(versionId, expectedVid);
                }
                return s3.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: key,
                }));
            })
            .then(data => {
                if (versionId) {
                    // Check version ID when we expect one
                    assert.strictEqual(data.VersionId, versionId);
                }
                cb();
            })
            .catch(err => cb(err));
        }

        function _initiateMpuAndPutOnePart() {
            const result = {};
            return s3.send(new CreateMultipartUploadCommand({
                Bucket: bucket, 
                Key: key 
            }))
            .then(data => {
                result.uploadId = data.UploadId;
                return s3.send(new UploadPartCommand({
                    Bucket: bucket,
                    Key: key,
                    PartNumber: 1,
                    UploadId: data.UploadId,
                    Body: 'foo',
                }));
            })
            .then(data => {
                result.eTag = data.ETag;
                return result;
            })
            .catch(err => {
                process.stdout.write(`Error in beforeEach: ${err}\n`);
                throw err;
            });
        }

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        });

        afterEach(async () => {
            await removeAllVersions({ Bucket: bucket });
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        describe('on bucket without versioning configuration', () => {
            let uploadId;
            let eTag;

            beforeEach(() => _initiateMpuAndPutOnePart()
                .then(result => {
                    uploadId = result.uploadId;
                    eTag = result.eTag;
                })
            );

            it('should complete an MPU with fewer parts than were ' +
                'originally put without returning a version id', done => {
                _completeMpuAndCheckVid(uploadId, eTag, undefined, done);
            });
        });

        describe('on bucket with enabled versioning', () => {
            let uploadId;
            let eTag;

            beforeEach(() => s3.send(new PutBucketVersioningCommand({ 
                Bucket: bucket,
                VersioningConfiguration: versioningEnabled 
            }))
                .then(() => _initiateMpuAndPutOnePart())
                .then(result => {
                    uploadId = result.uploadId;
                    eTag = result.eTag;
                })
            );

            it('should complete an MPU with fewer parts than were ' +
                'originally put and return a version id', done => {
                _completeMpuAndCheckVid(uploadId, eTag, true, done);
            });
        });

        describe('on bucket with suspended versioning', () => {
            let uploadId;
            let eTag;

            beforeEach(() => s3.send(new PutBucketVersioningCommand({ 
                Bucket: bucket,
                VersioningConfiguration: versioningSuspended 
            }))
                .then(() => _initiateMpuAndPutOnePart())
                .then(result => {
                    uploadId = result.uploadId;
                    eTag = result.eTag;
                    return result;
                })
            );

            it('should complete an MPU with fewer parts than were ' +
                'originally put and should not return a version id', done => {
                _completeMpuAndCheckVid(uploadId, eTag, undefined, done);
            });
        });

        describe('with tags set on initiation', () => {
            const tagKey = 'keywithtags';

            taggingTests.forEach(test => {
                it(test.it, done => {
                    const [key, value] =
                        [test.tag.key, test.tag.value].map(encodeURIComponent);
                    const tagging = `${key}=${value}`;

                    async.waterfall([
                        next => {
                            s3.send(new CreateMultipartUploadCommand({
                                Bucket: bucket,
                                Key: tagKey,
                                Tagging: tagging,
                            }))
                            .then(data => {
                                if (test.error) {
                                    return next(new Error('Expected error but got success'));
                                }
                                return next(null, data.UploadId);
                            })
                            .catch(err => {
                                if (test.error) {
                                    assert.strictEqual(err.name, test.error);
                                    assert.strictEqual(err.$metadata.httpStatusCode, 400);
                                    return next('expected');
                                }
                                return next(err);
                            });
                        },
                        (uploadId, next) => {
                            s3.send(new UploadPartCommand({
                                Bucket: bucket,
                                Key: tagKey,
                                PartNumber: 1,
                                UploadId: uploadId,
                                Body: 'foo',
                            }))
                            .then(data => next(null, data.ETag, uploadId))
                            .catch(err => next(err));
                        },
                        (eTag, uploadId, next) => {
                            s3.send(new CompleteMultipartUploadCommand({
                                Bucket: bucket,
                                Key: tagKey,
                                UploadId: uploadId,
                                MultipartUpload: {
                                    Parts: [{
                                        ETag: eTag,
                                        PartNumber: 1,
                                    }],
                                },
                            }))
                            .then(() => next())
                            .catch(err => next(err));
                        },
                    ], err => {
                        if (err === 'expected') {
                            done();
                        } else {
                            assert.ifError(err);
                            s3.send(new GetObjectTaggingCommand({
                                Bucket: bucket,
                                Key: tagKey,
                            }))
                            .then(tagData => {
                                assert.deepStrictEqual(tagData.TagSet,
                                    [{
                                        Key: test.tag.key,
                                        Value: test.tag.value,
                                    }]);
                                done();
                            })
                            .catch(err => done(err));
                        }
                    });
                });
            });
        });
         describe('with re-upload of part during CompleteMPU execution', () => {
            let uploadId;
            let eTag;

            beforeEach(() => _initiateMpuAndPutOnePart()
                .then(result => {
                    uploadId = result.uploadId;
                    eTag = result.eTag;
                })
            );

            it('should complete the MPU successfully and leave a readable object', done => {
                async.parallel([
                    doneReUpload => {
                        s3.send(new UploadPartCommand({
                            Bucket: bucket,
                            Key: key,
                            PartNumber: 1,
                            UploadId: uploadId,
                            Body: 'foo',
                        }))
                        .then(() => doneReUpload())
                        .catch(err => {
                            // in case the CompleteMPU finished earlier,
                            // we may get a NoSuchKey error, so just
                            // ignore it
                            if (err instanceof NoSuchKey) {
                                return doneReUpload();
                            }
                            return doneReUpload(err);
                        });
                    },
                    doneComplete => _completeMpuAndCheckVid(
                        uploadId, eTag, undefined, doneComplete),
                ], done);
            });
        });
    });
});
