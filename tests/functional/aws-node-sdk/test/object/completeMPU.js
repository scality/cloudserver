import assert from 'assert';
import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';
import {
    constants,
    removeAllVersions,
} from '../../lib/utility/versioning-util.js';

const date = Date.now();
const bucket = `completempu${date}`;
const key = 'key';

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

const testing = process.env.VERSIONING === 'no' ? describe.skip : describe;

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
            return s3.createMultipartUploadAsync({
                Bucket: bucket, Key: key })
            .then(data => {
                result.uploadId = data.UploadId;
                return s3.uploadPartAsync({ Bucket: bucket, Key: key,
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

        testing('on bucket with enabled versioning', () => {
            let uploadId;
            let eTag;

            beforeEach(() => s3.putBucketVersioningAsync({ Bucket: bucket,
                    VersioningConfiguration: constants.versioningEnabled })
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

        testing('on bucket with suspended versioning', () => {
            let uploadId;
            let eTag;

            beforeEach(() => s3.putBucketVersioningAsync({ Bucket: bucket,
                    VersioningConfiguration: constants.versioningSuspended })
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
    });
});
