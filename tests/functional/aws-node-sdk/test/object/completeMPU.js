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

const date = Date.now();
const bucket = `completempu${date}`;
const key = 'key';

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}


describe('Complete MPU', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        function _completeMpuAndCheckVid(uploadId, eTag, expectedVid, cb) {
            s3.completeMultipartUpload({
                Bucket: bucket,
                Key: key,
                MultipartUpload: {
                    Parts: [{ ETag: eTag, PartNumber: 1 }],
                },
                UploadId: uploadId },
            (err, data) => {
                checkNoError(err);
                const versionId = data.VersionId;
                if (expectedVid) {
                    assert.notEqual(versionId, undefined);
                } else {
                    assert.strictEqual(versionId, expectedVid);
                }
                return s3.getObject({
                    Bucket: bucket,
                    Key: key,
                },
                (err, data) => {
                    checkNoError(err);
                    if (versionId) {
                        assert.strictEqual(data.VersionId, versionId);
                    }
                    cb();
                });
            });
        }

        function _initiateMpuAndPutOnePart() {
            const result = {};
            return s3.createMultipartUploadPromise({
                Bucket: bucket, Key: key })
            .then(data => {
                result.uploadId = data.UploadId;
                return s3.uploadPartPromise({ Bucket: bucket, Key: key,
                    PartNumber: 1, UploadId: data.UploadId, Body: 'foo' });
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

        beforeEach(done => {
            s3.createBucket({ Bucket: bucket }, done);
        });

        afterEach(done => {
            removeAllVersions({ Bucket: bucket }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucket }, done);
            });
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

            beforeEach(() => s3.putBucketVersioningPromise({ Bucket: bucket,
                VersioningConfiguration: versioningEnabled })
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

            beforeEach(() => s3.putBucketVersioningPromise({ Bucket: bucket,
                VersioningConfiguration: versioningSuspended })
                .then(() => _initiateMpuAndPutOnePart())
                .then(result => {
                    uploadId = result.uploadId;
                    eTag = result.eTag;
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
                    const key = encodeURIComponent(test.tag.key);
                    const value = encodeURIComponent(test.tag.value);
                    const tagging = `${key}=${value}`;

                    async.waterfall([
                        next => s3.createMultipartUpload({
                            Bucket: bucket,
                            Key: tagKey,
                            Tagging: tagging,
                        }, (err, data) => {
                            if (test.error) {
                                assert(err, 'Expected err but found none');
                                assert.strictEqual(err.code, test.error);
                                assert.strictEqual(err.statusCode, 400);
                                next('expected');
                            }
                            next(null, data.UploadId);
                        }),
                        (uploadId, next) => s3.uploadPart({
                            Bucket: bucket,
                            Key: tagKey,
                            PartNumber: 1,
                            UploadId: uploadId,
                            Body: 'foo',
                        }, (err, data) => {
                            next(err, data.ETag, uploadId);
                        }),
                        (eTag, uploadId, next) => s3.completeMultipartUpload({
                            Bucket: bucket,
                            Key: tagKey,
                            UploadId: uploadId,
                            MultipartUpload: {
                                Parts: [{
                                    ETag: eTag,
                                    PartNumber: 1,
                                }],
                            },
                        }, next),
                    ], err => {
                        if (err === 'expected') {
                            done();
                        } else {
                            assert.ifError(err);
                            s3.getObjectTagging({
                                Bucket: bucket,
                                Key: tagKey,
                            }, (err, tagData) => {
                                assert.ifError(err);
                                assert.deepStrictEqual(tagData.TagSet,
                                    [{
                                        Key: test.tag.key,
                                        Value: test.tag.value,
                                    }]);
                                done();
                            });
                        }
                    });
                });
            });
        });
    });
});
