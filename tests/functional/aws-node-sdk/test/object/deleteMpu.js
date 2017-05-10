const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'functestabortmultipart';
const key = 'key';

const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

const westLocation = 'scality-us-west-1';
const eastLocation = 'us-east-1';

const confLocations = [
  { name: 'us-west-1', statusCode: 204, location: westLocation, describe },
  { name: 'us-east-1', statusCode: 404, location: eastLocation, describe },
];

describe('DELETE multipart', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        function _assertStatusCode(uploadId, statusCodeExpected, callback) {
            const request =
            s3.abortMultipartUpload({ Bucket: bucket, Key: key,
                UploadId: uploadId }, err => {
                const statusCode =
                request.response.httpResponse.statusCode;
                assert.strictEqual(statusCode, statusCodeExpected,
                    `Found unexpected statusCode ${statusCode}`);
                if (statusCode === 204) {
                    assert.strictEqual(err, null,
                        `Expected no err but found ${err}`);
                    return callback(err);
                }
                return callback();
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
        confLocations.forEach(confLocation => {
            confLocation.describe('on existing bucket with ' +
            `${confLocation.name}`,
            () => {
                beforeEach(() =>
                    s3.createBucketAsync({ Bucket: bucket,
                      CreateBucketConfiguration: {
                          LocationConstraint: confLocation.location,
                      } })
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

                itSkipIfAWS(`should return ${confLocation.statusCode} if ` +
                'mpu does not exist with uploadId',
                done => {
                    const uploadId = 'nonexistinguploadid';
                    _assertStatusCode(uploadId, confLocation.statusCode, done);
                });

                describe('if mpu exists with uploadId + at least one part',
                () => {
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
                        _assertStatusCode(uploadId, 204,
                          done);
                    });
                });
            });
        });
    });
});
