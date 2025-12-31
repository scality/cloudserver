
const assert = require('assert');
const {
    CreateBucketCommand,
    CreateMultipartUploadCommand,
    AbortMultipartUploadCommand,
    ListMultipartUploadsCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = `object-test-mpu-${Date.now()}`;
const objectKey = 'toAbort&<>"\'';

// Get the expected object of listMPU API.
function getExpectedObj(res, data) {
    // If `maxUploads` is not given as a parameter, it should default to 1000.
    const maxUploads = data.maxUploads === undefined ? 1000 : data.maxUploads;

    // If `MaxUploads` is defined as 0, `IsTruncated` is set to `false` despite
    // the fact that there may be multipart uploads in the bucket.
    if (maxUploads === 0) {
        return {
            Bucket: bucket,
            KeyMarker: '',
            UploadIdMarker: '',
            MaxUploads: 0,
            IsTruncated: false,
        };
    }

    const { prefixVal, delimiter, uploadId, displayName, userId } = data;
    const initiated = new Date(res.Uploads[0].Initiated.toISOString());
    const expectedObj = {
        Bucket: bucket,
        KeyMarker: '',
        UploadIdMarker: '',
        NextKeyMarker: objectKey,
        Prefix: prefixVal,
        Delimiter: delimiter,
        NextUploadIdMarker: uploadId,
        MaxUploads: maxUploads,
        IsTruncated: false,
        Uploads: [{
            UploadId: uploadId,
            Key: objectKey,
            Initiated: initiated,
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
    };

    // If no `prefixVal` is given, it should not be included in the response.
    if (!prefixVal) {
        delete expectedObj.Prefix;
    }

    // If no `delimiter` is given, it should not be included in the response.
    if (!delimiter) {
        delete expectedObj.Delimiter;
    }

    return expectedObj;
}

// Compare the response object with the expected object.
function checkValues(res, data) {
    const expectedObj = getExpectedObj(res, data);
    assert.deepStrictEqual(res, expectedObj);
}

describe('aws-node-sdk test suite of listMultipartUploads', () =>
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        const data = {};

        beforeEach(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            const ownerRes = await bucketUtil.getOwner();
            // The owner of the bucket will also be the MPU upload owner.
            data.displayName = ownerRes.DisplayName;
            data.userId = ownerRes.ID;
            
            const mpuRes = await s3.send(new CreateMultipartUploadCommand({
                Bucket: bucket,
                Key: objectKey,
            }));
            data.uploadId = mpuRes.UploadId; 
        });

        afterEach(async () => {
            await s3.send(new AbortMultipartUploadCommand({
                Bucket: bucket,
                Key: objectKey,
                UploadId: data.uploadId,
            }));
            await bucketUtil.empty(bucket);
            await bucketUtil.deleteOne(bucket);
        });

        it('should list ongoing multipart uploads', async () => {
            // eslint-disable-next-line no-unused-vars
            const { $metadata, ...res } = await s3.send(new ListMultipartUploadsCommand({ Bucket: bucket }));
            checkValues(res, data);
        });

        it('should list ongoing multipart uploads with params', async () => {
            data.prefixVal = 'to';
            data.delimiter = 'test-delimiter';
            data.maxUploads = 1;
            // eslint-disable-next-line no-unused-vars
            const {$metadata, ...res } = await s3.send(new ListMultipartUploadsCommand({
                Bucket: bucket,
                Prefix: 'to',
                Delimiter: 'test-delimiter',
                MaxUploads: 1,
            }));
            checkValues(res, data);
        });

        it('should list 0 multipart uploads when MaxUploads is 0', async () => {
            data.maxUploads = 0;
            // eslint-disable-next-line no-unused-vars
            const { $metadata , ...res } = await s3.send(new ListMultipartUploadsCommand({
                Bucket: bucket,
                MaxUploads: 0,
            }));
            checkValues(res, data);
        });
    })
);
