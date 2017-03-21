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
const bodyFirstPart = Buffer.allocUnsafe(10).fill(0);
const bodySecondPart = Buffer.allocUnsafe(5).fill(0);

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

const testing = process.env.VERSIONING === 'no' ? describe.skip : describe;

describe('Complete MPU', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        function _completeMpuAndCheckVid(uploadId, firstEtag, expectedVid, cb) {
            s3.completeMultipartUpload({
                Bucket: bucket,
                Key: key,
                MultipartUpload: {
                    Parts: [{ ETag: firstEtag, PartNumber: 1 }],
                },
                UploadId: uploadId },
            (err, data) => {
                checkNoError(err);
                // to show that the mpu completed with just 1 part
                assert.strictEqual(data.ETag.slice(-3), '-1"');
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
                    // to show that data in completed key is just first part
                    assert.strictEqual(data.ContentLength, '10');
                    if (versionId) {
                        assert.strictEqual(data.VersionId, versionId);
                    }
                    cb();
                });
            });
        }

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
            let firstEtag;

            beforeEach(() => s3.createBucketAsync({ Bucket: bucket })
                .then(() => s3.createMultipartUploadAsync({
                    Bucket: bucket, Key: key }))
                .then(res => {
                    uploadId = res.UploadId;
                    return s3.uploadPartAsync({ Bucket: bucket, Key: key,
                      PartNumber: 1, UploadId: uploadId, Body: bodyFirstPart });
                })
                .then(res => {
                    firstEtag = res.ETag;
                    return firstEtag;
                })
                .then(() => s3.uploadPartAsync({ Bucket: bucket, Key: key,
                    PartNumber: 2, UploadId: uploadId, Body: bodySecondPart }))
                .catch(err => {
                    process.stdout.write(`Error in beforeEach: ${err}\n`);
                    throw err;
                })
            );

            it('should complete an MPU with fewer parts than were ' +
                'originally put without returning a version id', done => {
                _completeMpuAndCheckVid(uploadId, firstEtag, undefined, done);
            });
        });

        testing('on bucket with enabled versioning', () => {
            let uploadId;
            let firstEtag;

            beforeEach(() => s3.createBucketAsync({ Bucket: bucket })
                .then(() => s3.putBucketVersioningAsync({ Bucket: bucket,
                    VersioningConfiguration: constants.versioningEnabled }))
                .then(() => s3.createMultipartUploadAsync({
                    Bucket: bucket, Key: key }))
                .then(res => {
                    uploadId = res.UploadId;
                    return s3.uploadPartAsync({ Bucket: bucket, Key: key,
                      PartNumber: 1, UploadId: uploadId, Body: bodyFirstPart });
                })
                .then(res => {
                    firstEtag = res.ETag;
                    return firstEtag;
                })
                .then(() => s3.uploadPartAsync({ Bucket: bucket, Key: key,
                    PartNumber: 2, UploadId: uploadId, Body: bodySecondPart }))
                .catch(err => {
                    process.stdout.write(`Error in beforeEach: ${err}\n`);
                    throw err;
                })
            );

            it('should complete an MPU with fewer parts than were ' +
                'originally put and return a version id', done => {
                _completeMpuAndCheckVid(uploadId, firstEtag, true, done);
            });
        });

        testing('on bucket with suspended versioning', () => {
            let uploadId;
            let firstEtag;

            beforeEach(() => s3.createBucketAsync({ Bucket: bucket })
                .then(() => s3.putBucketVersioningAsync({ Bucket: bucket,
                    VersioningConfiguration: constants.versioningSuspended }))
                .then(() => s3.createMultipartUploadAsync({
                    Bucket: bucket, Key: key }))
                .then(res => {
                    uploadId = res.UploadId;
                    return s3.uploadPartAsync({ Bucket: bucket, Key: key,
                      PartNumber: 1, UploadId: uploadId, Body: bodyFirstPart });
                })
                .then(res => {
                    firstEtag = res.ETag;
                    return firstEtag;
                })
                .then(() => s3.uploadPartAsync({ Bucket: bucket, Key: key,
                    PartNumber: 2, UploadId: uploadId, Body: bodySecondPart }))
                .catch(err => {
                    process.stdout.write(`Error in beforeEach: ${err}\n`);
                    throw err;
                })
            );

            it('should complete an MPU with fewer parts than were ' +
                'originally put and should not return a version id', done => {
                _completeMpuAndCheckVid(uploadId, firstEtag, undefined, done);
            });
        });
    });
});
