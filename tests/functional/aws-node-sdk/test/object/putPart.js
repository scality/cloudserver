const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'bucket2putstuffin4324242';
const key = 'key';

describe('PUT object', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let uploadId;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucket({ Bucket: bucket }).promise()
            .then(() => s3.createMultipartUpload({
                Bucket: bucket, Key: key }).promise())
            .then(res => {
                uploadId = res.UploadId;
                return uploadId;
            })
            .catch(err => {
                process.stdout.write(`Error in beforeEach: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket');
            return s3.abortMultipartUpload({
                Bucket: bucket, Key: key, UploadId: uploadId,
            }).promise()
            .then(() => bucketUtil.empty(bucket))
            .then(() => {
                process.stdout.write('Deleting bucket');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            });
        });

        it('should return Not Implemented error for obj. encryption using ' +
            'customer-provided encryption keys', done => {
            const params = { Bucket: bucket, Key: 'key', PartNumber: 0,
                UploadId: uploadId, SSECustomerAlgorithm: 'AES256' };
            s3.uploadPart(params, err => {
                assert.strictEqual(err.code, 'NotImplemented');
                done();
            });
        });

        it('should return InvalidArgument if negative PartNumber', done => {
            const params = { Bucket: bucket, Key: 'key', PartNumber: -1,
                UploadId: uploadId };
            s3.uploadPart(params, err => {
                assert.strictEqual(err.code, 'InvalidArgument');
                done();
            });
        });
    });
});
