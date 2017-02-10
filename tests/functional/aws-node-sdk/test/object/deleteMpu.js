import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucket = 'functestabortmultipart';
const key = 'key';

const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

describe('DELETE multipart', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        function _assert204StatusCode(uploadId, callback) {
            const request =
            s3.abortMultipartUpload({ Bucket: bucket, Key: key,
                UploadId: uploadId }, err => {
                const statusCode =
                request.response.httpResponse.statusCode;
                assert.strictEqual(statusCode, 204,
                    `Found unexpected statusCode ${statusCode}`);
                assert.strictEqual(err, null,
                    `Expected no err but found ${err}`);
                callback(err);
            });
        }

        it('on bucket that does not exist: should return NoSuchBucket',
        done => {
            const uploadId = 'nonexistinguploadid';
            s3.abortMultipartUpload({ Bucket: bucket, Key: key,
                UploadId: uploadId }, err => {
                assert.notEqual(err, null,
                    'Expected NoSuchBucket but found no err');
                assert.strictEqual(err.code, 'NoSuchBucket');
                done();
            });
        });

        describe('on existing bucket', () => {
            beforeEach(() =>
                s3.createBucketAsync({ Bucket: bucket })
                .catch(err => {
                    process.stdout.write(`Error in beforeEach: ${err}\n`);
                    throw err;
                })
            );

            afterEach(() => {
                process.stdout.write('Emptying bucket\n');
                return bucketUtil.empty(bucket)
                .then(() => {
                    process.stdout.write('Deleting bucket\n');
                    return bucketUtil.deleteOne(bucket);
                })
                .catch(err => {
                    process.stdout.write('Error in afterEach');
                    throw err;
                });
            });

            // AWS returns 404 - NoSuchUpload in us-east-1. This behavior can
            // be toggled to be compatible with AWS by enabling usEastBehavior
            // in the config.
            itSkipIfAWS('should return 204 if mpu does not exist with uploadId',
            done => {
                const uploadId = 'nonexistinguploadid';
                _assert204StatusCode(uploadId, done);
            });

            describe('if mpu exists with uploadId + at least one part', () => {
                let uploadId;

                beforeEach(() =>
                    s3.createMultipartUploadAsync({
                        Bucket: bucket,
                        Key: key,
                    })
                    .then(res => {
                        uploadId = res.UploadId;
                        return s3.uploadPart({
                            Bucket: bucket,
                            Key: key,
                            PartNumber: 1,
                            UploadId: uploadId,
                        });
                    })
                );

                it('should return 204 for abortMultipartUpload', done => {
                    _assert204StatusCode(uploadId, done);
                });
            });
        });
    });
});
