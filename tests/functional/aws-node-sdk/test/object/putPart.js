const assert = require('assert');
const {
    CreateBucketCommand,
    CreateMultipartUploadCommand,
    AbortMultipartUploadCommand,
    UploadPartCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'bucket2putstuffin4324242';
const key = 'key';

describe('PUT object', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let uploadId;

        beforeEach(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            const res = await s3.send(new CreateMultipartUploadCommand({
                Bucket: bucket, 
                Key: key 
            }));
            uploadId = res.UploadId;
        });

        afterEach(async () => {
            await s3.send(new AbortMultipartUploadCommand({
                Bucket: bucket, 
                Key: key, 
                UploadId: uploadId,
            }));
            await bucketUtil.empty(bucket);
            await bucketUtil.deleteOne(bucket);
        });

        it('should return Not Implemented error for obj. encryption using ' +
            'customer-provided encryption keys', async () => {
            const params = { 
                Bucket: bucket, 
                Key: 'key', 
                PartNumber: 0,
                UploadId: uploadId, 
                SSECustomerAlgorithm: 'AES256' 
            };
            try {
                await s3.send(new UploadPartCommand(params));
                throw new Error('Expected NotImplemented error');
            } catch (err) {
                assert.strictEqual(err.name, 'NotImplemented');
            }
        });

        it('should return InvalidArgument if negative PartNumber', async () => {
            const params = {
                Bucket: bucket,
                Key: 'key',
                PartNumber: -1,
                UploadId: uploadId
            };
            
            try {
                await s3.send(new UploadPartCommand(params));
                assert.fail('Expected InvalidArgument error');
            } catch (err) {
                assert.strictEqual(err.name, 'InvalidArgument');
            }
        });
    });
});
