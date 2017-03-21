import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

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
            Uploads: [],
            CommonPrefixes: [],
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
        CommonPrefixes: [],
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

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;

            return s3.createBucketAsync({ Bucket: bucket })
            .then(() => bucketUtil.getOwner())
            .then(res => {
                // The owner of the bucket will also be the MPU upload owner.
                data.displayName = res.DisplayName;
                data.userId = res.ID;
            })
            .then(() => s3.createMultipartUploadAsync({
                Bucket: bucket,
                Key: objectKey,
            }))
            .then(res => {
                data.uploadId = res.UploadId;
            });
        });

        afterEach(() =>
            s3.abortMultipartUploadAsync({
                Bucket: bucket,
                Key: objectKey,
                UploadId: data.uploadId,
            })
            .then(() => bucketUtil.empty(bucket))
            .then(() => bucketUtil.deleteOne(bucket))
        );

        it('should list ongoing multipart uploads', () =>
            s3.listMultipartUploadsAsync({ Bucket: bucket })
            .then(res => checkValues(res, data))
        );

        it('should list ongoing multipart uploads with params', () => {
            data.prefixVal = 'to';
            data.delimiter = 'test-delimiter';
            data.maxUploads = 1;

            return s3.listMultipartUploadsAsync({
                Bucket: bucket,
                Prefix: 'to',
                Delimiter: 'test-delimiter',
                MaxUploads: 1,
            })
            .then(res => checkValues(res, data));
        });

        it('should list 0 multipart uploads when MaxUploads is 0', () => {
            data.maxUploads = 0;

            return s3.listMultipartUploadsAsync({
                Bucket: bucket,
                MaxUploads: 0,
            })
            .then(res => checkValues(res, data));
        });
    })
);
