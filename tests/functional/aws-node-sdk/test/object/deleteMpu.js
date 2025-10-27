const assert = require('assert');
const {
    CreateBucketCommand,
    AbortMultipartUploadCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
} = require('@aws-sdk/client-s3');

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
        const s3Client = bucketUtil.s3;

        function _assertStatusCode(uploadId, statusCodeExpected, callback) {
            const command = new AbortMultipartUploadCommand({
                Bucket: bucket,
                Key: key,
                UploadId: uploadId,
            });

            s3Client.send(command)
                .then(response => {
                    const statusCode = 
                    response?.$metadata?.httpStatusCode;
                    assert.strictEqual(statusCode, statusCodeExpected,
                        `Found unexpected statusCode ${statusCode}`);
                    return callback();
                })
                .catch(err => {
                    const statusCode = err?.$metadata?.httpStatusCode;
                    if (statusCode) {
                        assert.strictEqual(statusCode, statusCodeExpected,
                            `Found unexpected statusCode ${statusCode}`);
                    }
                    if (statusCodeExpected === 204) {
                        return callback(err);
                    }
                    return callback();
                });
        }

        it('on bucket that does not exist: should return NoSuchBucket',
        done => {
            const uploadId = 'nonexistinguploadid';
            const command = new AbortMultipartUploadCommand({
                Bucket: bucket,
                Key: key,
                UploadId: uploadId,
            });

            s3Client.send(command)
                .then(() => {
                    done(new Error('Expected NoSuchBucket but request succeeded'));
                })
                .catch(err => {
                    assert.notEqual(err, null,
                        'Expected NoSuchBucket but found no err');
                    assert.strictEqual(err.name, 'NoSuchBucket');
                    done();
                });
        });

        confLocations.forEach(confLocation => {
            confLocation.describe('on existing bucket with ' +
            `${confLocation.name}`,
            () => {
                beforeEach(async () => {
                    const command = new CreateBucketCommand({
                        Bucket: bucket,
                        CreateBucketConfiguration: {
                            LocationConstraint: confLocation.location,
                        },
                    });
                    await s3Client.send(command);
                });

                afterEach(async () => {
                        process.stdout.write('Emptying bucket\n');
                        await bucketUtil.empty(bucket);
                        process.stdout.write('Deleting bucket\n');
                        await bucketUtil.deleteOne(bucket);
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

                    beforeEach(async () => {
                            const createCommand = new CreateMultipartUploadCommand({
                                Bucket: bucket,
                                Key: key,
                            });
                            const createResponse = await s3Client.send(createCommand);
                            uploadId = createResponse.UploadId;
                            const uploadCommand = new UploadPartCommand({
                                Bucket: bucket,
                                Key: key,
                                PartNumber: 1,
                                UploadId: uploadId,
                                Body: Buffer.from('test data'),
                            });
                            await s3Client.send(uploadCommand);
                    });

                    it('should return 204 for abortMultipartUpload', done => {
                        _assertStatusCode(uploadId, 204, done);
                    });
                });
            });
        });
    });
});
