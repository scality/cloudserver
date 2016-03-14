import assert from 'assert';
import crypto from 'crypto';
import { S3 } from 'aws-sdk';

import getConfig from '../support/config';

const random = Math.round(Math.random() * 100).toString();
const bucket = `ftest-mybucket-${random}`;

// Create a buffer to put as a multipart upload part
// and get its ETag
const md5Hash = crypto.createHash('md5');
const bufferBody =
    new Buffer(5242880).fill(0);
md5Hash.update(bufferBody);
const calculatedHash = md5Hash.digest('hex');

// Store uploadId's in memory so can do multiple tests with
// same uploadId
const multipartUploadData = {};

describe('aws-node-sdk test suite as registered user', function testSuite() {
    this.timeout(60000);
    let s3;

    before(function setup() {
        const config = getConfig('default', { signatureVersion: 'v4' });

        s3 = new S3(config);
    });

    it('should do bucket listing', function bucketListing(done) {
        s3.listBuckets((err, data) => {
            if (err) {
                return done(new Error(`error listing buckets: ${err}`));
            }

            assert(data.Buckets, 'No buckets Info sent back');
            assert(data.Owner, 'No owner Info sent back');
            assert(data.Owner.ID, 'Owner ID not sent back');
            assert(data.Owner.DisplayName, 'DisplayName not sent back');
            const owner = Object.keys(data.Owner);
            assert.strictEqual(owner.length, 2, 'Too much fields in owner');
            done();
        });
    });

    it('should create a bucket', function createbucket(done) {
        s3.createBucket({ Bucket: bucket }, (err) => {
            if (err) {
                return done(new Error(`error creating bucket: ${err}`));
            }
            done();
        });
    });

    it('should create a multipart upload', function createMPU(done) {
        s3.createMultipartUpload({ Bucket: bucket, Key: 'toAbort' },
            (err, data) => {
                if (err) {
                    return done(new Error(
                        `error initiating multipart upload: ${err}`));
                }
                assert.strictEqual(data.Bucket, bucket);
                assert.strictEqual(data.Key, 'toAbort');
                assert.ok(data.UploadId);
                multipartUploadData.firstUploadId = data.UploadId;
                done();
            });
    });

    it('should upload a part of a multipart upload to be aborted',
        function uploadpart(done) {
            const params = {
                Bucket: bucket,
                Key: 'toAbort',
                PartNumber: 1,
                UploadId: multipartUploadData.firstUploadId,
                Body: bufferBody,
            };
            s3.uploadPart(params, (err, data) => {
                if (err) {
                    return done(new Error(`error uploading a part: ${err}`));
                }
                assert.strictEqual(data.ETag, `"${calculatedHash}"`);
                done();
            });
        });

    it('should abort a multipart upload', function abortMPU(done) {
        const params = {
            Bucket: bucket,
            Key: 'toAbort',
            UploadId: multipartUploadData.firstUploadId,
        };
        s3.abortMultipartUpload(params, (err, data) => {
            if (err) {
                return done(new Error(
                    `error aborting multipart upload: ${err}`));
            }
            assert.ok(data);
            done();
        });
    });

    it('should upload a part of a multipart upload', function createMPU(done) {
        s3.createMultipartUpload({ Bucket: bucket, Key: 'toComplete' },
            (err, data) => {
                if (err) {
                    return done(new Error(
                        `error initiating multipart upload: ${err}`));
                }
                const uploadId = data.UploadId;
                multipartUploadData.secondUploadId = data.UploadId;
                const params = {
                    Bucket: bucket,
                    Key: 'toComplete',
                    PartNumber: 1,
                    UploadId: uploadId,
                    Body: bufferBody,
                };
                s3.uploadPart(params, (err, data) => {
                    if (err) {
                        return done(
                            new Error('error uploading a part: ${err}'));
                    }
                    assert.strictEqual(data.ETag, `"${calculatedHash}"`);
                    done();
                });
            });
    });

    it('should upload a second part of a multipart upload',
        function createMPU(done) {
            const params = {
                Bucket: bucket,
                Key: 'toComplete',
                PartNumber: 2,
                UploadId: multipartUploadData.secondUploadId,
                Body: bufferBody,
            };
            s3.uploadPart(params, (err, data) => {
                if (err) {
                    return done(new Error(`error uploading a part: ${err}`));
                }
                assert.strictEqual(data.ETag, `"${calculatedHash}"`);
                done();
            });
        });

    it('should list the parts of a multipart upload', function listparts(done) {
        const params = {
            Bucket: bucket,
            Key: 'toComplete',
            UploadId: multipartUploadData.secondUploadId,
        };
        s3.listParts(params, (err, data) => {
            if (err) {
                return done(new Error(`error listing parts: ${err}`));
            }
            assert.strictEqual(data.Bucket, bucket);
            assert.strictEqual(data.Key, 'toComplete');
            assert.strictEqual(data.UploadId, multipartUploadData
                .secondUploadId);
            assert.strictEqual(data.IsTruncated, false);
            assert.strictEqual(data.Parts[0].PartNumber, 1);
            assert.strictEqual(data.Parts[0].ETag, calculatedHash);
            assert.strictEqual(data.Parts[0].Size, 5242880);
            assert.strictEqual(data.Parts[1].PartNumber, 2);
            assert.strictEqual(data.Parts[1].ETag, calculatedHash);
            assert.strictEqual(data.Parts[1].Size, 5242880);
            // Must disable for now when running with Vault
            // since will need to pull actual ARN and canonicalId
            // assert.strictEqual(data.Initiator.ID, accessKey1ARN);
            // Note that for in memory implementation, "accessKey1"
            // is both the access key and the canonicalId so this
            // call works.  For real implementation with vault,
            // will need the canonicalId.
            // assert.strictEqual(data.Owner.ID, config.accessKeyId);
            assert.strictEqual(data.StorageClass, 'STANDARD');
        });
        done();
    });

    it('should list ongoing multipart uploads', (done) => {
        const params = {
            Bucket: bucket,
        };
        s3.listMultipartUploads(params, (err, data) => {
            if (err) {
                return done(new Error(`error in listMultipartUploads: ${err}`));
            }
            assert.strictEqual(data.Uploads.length, 1);
            assert.strictEqual(data.Uploads[0].UploadId,
                multipartUploadData.secondUploadId);
            done();
        });
    });

    it('should list ongoing multipart uploads with params', (done) => {
        const params = {
            Bucket: bucket,
            Prefix: 'to',
            MaxUploads: 2,
        };
        s3.listMultipartUploads(params, (err, data) => {
            if (err) {
                return done(new Error(`error in listMultipartUploads: ${err}`));
            }
            assert.strictEqual(data.Uploads.length, 1);
            assert.strictEqual(data.Uploads[0].UploadId,
                multipartUploadData.secondUploadId);
            done();
        });
    });

    it('should return an error if do not provide correct ' +
        'xml when completing a multipart upload', function completempu(done) {
        const params = {
            Bucket: bucket,
            Key: 'toComplete',
            UploadId: multipartUploadData.secondUploadId,
        };
        s3.completeMultipartUpload(params, (err) => {
            assert.strictEqual(err.code, 'MalformedXML');
            done();
        });
    });

    it('should complete a multipart upload', function completempu(done) {
        const params = {
            Bucket: bucket,
            Key: 'toComplete',
            UploadId: multipartUploadData.secondUploadId,
            MultipartUpload: {
                Parts: [
                    {
                        ETag: `"${calculatedHash}"`,
                        PartNumber: 1,
                    },
                    {
                        ETag: `"${calculatedHash}"`,
                        PartNumber: 2,
                    },
                ],
            },
        };
        s3.completeMultipartUpload(params, (err, data) => {
            if (err) {
                return done(new Error(`error completing mpu: ${err}`));
            }
            assert.strictEqual(data.Bucket, bucket);
            assert.strictEqual(data.Key, 'toComplete');
            assert.strictEqual(data.ETag, 'a7d414b9133d6483d9a1c4e04e856e3b-2');
            done();
        });
    });

    it('should delete object created by multipart upload',
        function deleteObject(done) {
            const params = {
                Bucket: bucket,
                Key: 'toComplete',
            };
            s3.deleteObject(params, (err, data) => {
                if (err) {
                    return done(new Error(`error deleting object: ${err}`));
                }
                assert.ok(data);
                done();
            });
        });

    it('should delete a bucket', function deletebucket(done) {
        s3.deleteBucket({ Bucket: bucket }, (err) => {
            if (err) {
                return done(new Error(`error deleting bucket: ${err}`));
            }
            done();
        });
    });
});
