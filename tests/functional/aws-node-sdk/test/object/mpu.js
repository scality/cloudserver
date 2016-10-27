import assert from 'assert';
import crypto from 'crypto';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

// Create a buffer to put as a multipart upload part and get its ETag
const bucket = 'bucket-test-mpu';
const objectKey = 'toAbort&<>"\'';
const md5HashFirstPart = crypto.createHash('md5');
const firstBufferBody = Buffer.allocUnsafe(5242880).fill(0);
const md5HashSecondPart = crypto.createHash('md5');
const secondBufferBody = Buffer.allocUnsafe(5242880).fill(1);
md5HashFirstPart.update(firstBufferBody);
md5HashSecondPart.update(secondBufferBody);
const firstPartHash = md5HashFirstPart.digest('hex');
const secondPartHash = md5HashSecondPart.digest('hex');
const combinedETag = '"0ea4f0f688a0be07ae1d92eb298d5218-2"';

const throwErr = (str, err) => {
    process.stdout.write(`${str}: ${err}\n`);
    throw err;
};

const objCmp = (resObj, expectedObj, cb) => {
    assert.deepStrictEqual(resObj, expectedObj);
    cb();
};

function listObj(uploadId, userId, displayName) {
    return {
        Bucket: bucket,
        KeyMarker: '',
        UploadIdMarker: '',
        NextKeyMarker: objectKey,
        Prefix: '',
        Delimiter: '',
        NextUploadIdMarker: uploadId,
        MaxUploads: 1,
        IsTruncated: false,
        Uploads: [{
            UploadId: uploadId,
            Key: objectKey,
            StorageClass: 'STANDARD',
            Owner:
            {
                DisplayName: displayName,
                ID: userId,
            },
            Initiator:
            {
                DisplayName: displayName,
                ID: userId,
            },
        }],
        CommonPrefixes: [],
    };
}

describe('MPU Upload Parts', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let uploadId;

        before(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            bucketUtil.createOne(bucket)
            .catch(err => throwErr('Error in before', err));
        });

        after(() => {
            bucketUtil.empty(bucket)
            .catch(err => throwErr('Error in after', err));
            bucketUtil.deleteOne(bucket)
            .catch(err => throwErr('Error in after', err));
        });

        it('should create a multipart upload', done => {
            s3.createMultipartUploadAsync({ Bucket: bucket, Key: objectKey })
            .catch(err => throwErr('Error initiating multipart upload', err))
            .then(res => {
                assert.deepStrictEqual(res, {
                    Bucket: bucket, Key: objectKey, UploadId: res.UploadId,
                });
                uploadId = res.UploadId;
                done();
            })
            .catch(done);
        });

        it('should upload a part of a multipart upload to be aborted',
            done => {
                s3.uploadPartAsync({
                    Bucket: bucket, Key: objectKey, PartNumber: 1,
                    UploadId: uploadId, Body: firstBufferBody,
                })
                .catch(err => throwErr('Error uploading a part', err))
                .then(res => objCmp(res, { ETag: `"${firstPartHash}"` }, done))
                .catch(done);
            });

        it('should abort a multipart upload', done => {
            s3.abortMultipartUploadAsync({
                Bucket: bucket, Key: objectKey, UploadId: uploadId,
            })
            .catch(err => throwErr('Error aborting multipart upload', err))
            .then(res => objCmp(res, {}, done))
            .catch(done);
        });
    });
});

describe('Complete MPU', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let uploadId;
        let displayName;
        let userId;

        before(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;

            return s3.createBucketAsync({ Bucket: bucket })
            .then(() => bucketUtil.getOwner())
            .then(res => { displayName = res.DisplayName; userId = res.ID; })
            .then(() => s3.createMultipartUploadAsync({
                Bucket: bucket, Key: objectKey,
            }))
            .then(res => { uploadId = res.UploadId; })
            .then(() => {
                process.stdout.write('Uploading a part');
                return s3.uploadPartAsync({
                    Bucket: bucket, Key: objectKey, PartNumber: 1,
                    UploadId: uploadId, Body: firstBufferBody,
                });
            })
            .catch(err => throwErr('Error in before', err));
        });

        after(() => {
            process.stdout.write('Emptying bucket');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => throwErr('Error in after', err));
        });

        it('should upload the second part of a multipart upload', done => {
            s3.uploadPartAsync({
                Bucket: bucket, Key: objectKey, PartNumber: 2,
                UploadId: uploadId, Body: secondBufferBody,
            })
            .catch(err => throwErr('Error uploading a part', err))
            .then(res => objCmp(res, { ETag: `"${secondPartHash}"` }, done))
            .catch(done);
        });

        it('should list the parts of a multipart upload', done => {
            s3.listPartsAsync({
                Bucket: bucket, Key: objectKey, UploadId: uploadId,
            })
            .catch(err => throwErr('Error listing parts', err))
            .then(res => {
                // The date object cannot be tested for because there will be a
                // difference between the time of upload creation and assertion
                // testing. Remove it so that we can still compare the object.
                const obj = res;
                assert(obj.Parts[0].LastModified instanceof Date);
                assert(obj.Parts[1].LastModified instanceof Date);
                delete obj.Parts[0].LastModified;
                delete obj.Parts[1].LastModified;

                objCmp(obj, {
                    Bucket: bucket,
                    Key: objectKey,
                    UploadId: uploadId,
                    MaxParts: 1000,
                    IsTruncated: false,
                    Parts: [
                        {
                            PartNumber: 1,
                            ETag: firstPartHash,
                            Size: 5242880,
                        },
                        {
                            PartNumber: 2,
                            ETag: secondPartHash,
                            Size: 5242880,
                        },
                    ],
                    Initiator: {
                        ID: userId,
                        DisplayName: displayName,
                    },
                    Owner: {
                        ID: userId,
                        DisplayName: displayName,
                    },
                    StorageClass: 'STANDARD',
                }, done);
            })
            .catch(done);
        });

        it('should list ongoing multipart uploads', done => {
            s3.listMultipartUploadsAsync({ Bucket: bucket })
            .catch(err => throwErr('Error in listMultipartUploads', err))
            .then(res => {
                assert(res.Uploads[0].Initiated instanceof Date);
                const obj = res;
                delete obj.Uploads[0].Initiated;

                objCmp(obj, listObj(uploadId, userId, displayName), done);
            })
            .catch(done);
        });

        it('should list ongoing multipart uploads with params', done => {
            s3.listMultipartUploadsAsync({
                Bucket: bucket, Prefix: 'to', MaxUploads: 2,
            })
            .catch(err => throwErr('Error in listMultipartUploads', err))
            .then(res => {
                assert(res.Uploads[0].Initiated instanceof Date);
                const obj = res;
                delete obj.Uploads[0].Initiated;

                objCmp(obj, listObj(uploadId, userId, displayName), done);
            })
            .catch(done);
        });

        it('should return an error if do not provide correct xml when ' +
            'completing a multipart upload', done => {
            s3.completeMultipartUploadAsync({
                Bucket: bucket, Key: objectKey, UploadId: uploadId,
            })
            .catch(err => objCmp(err.code, 'MalformedXML', done))
            .catch(done);
        });

        it('should complete a multipart upload', done => {
            s3.completeMultipartUploadAsync({
                Bucket: bucket, Key: objectKey, UploadId: uploadId,
                MultipartUpload: {
                    Parts: [
                        {
                            ETag: firstPartHash,
                            PartNumber: 1,
                        },
                        {
                            ETag: secondPartHash,
                            PartNumber: 2,
                        },
                    ],
                },
            })
            .catch(err => throwErr('Error copleting MPU', err))
            .then(res => objCmp(res, {
                Location: `http://${bucket}.localhost/${objectKey}`,
                Bucket: bucket, Key: objectKey, ETag: combinedETag,
            }, done))
            .catch(done);
        });
    });
});
