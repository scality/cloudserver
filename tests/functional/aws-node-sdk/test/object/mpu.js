import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucket = 'object-test-mpu';
const objectKey = 'toAbort&<>"\'';

const throwErr = (str, err) => {
    process.stdout.write(`${str}: ${err}\n`);
    throw err;
};

const checkValues = (res, uploadId, displayName, cb) => {
    const prefix = res.Prefix ? res.Prefix : undefined;
    const delimeter = res.Delimiter ? res.Delimiter : undefined;
    assert.deepStrictEqual(res.KeyMarker, '');
    assert.deepStrictEqual(res.UploadIdMarker, '');
    assert.deepStrictEqual(res.Prefix, prefix);
    assert.deepStrictEqual(res.Delimiter, delimeter);
    assert.deepStrictEqual(res.Uploads.length, 1);
    assert.deepStrictEqual(res.Uploads[0].UploadId, uploadId);
    assert.deepStrictEqual(res.Uploads[0].StorageClass, 'STANDARD');
    assert.deepStrictEqual(res.Uploads[0].Owner.DisplayName, displayName);
    cb();
};

describe('aws-node-sdk test suite of listMultipartUploads', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let uploadId;
        let displayName;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;

            return s3.createBucketAsync({ Bucket: bucket })
            .then(() => bucketUtil.getOwner())
            .then(res => {
                // In this case, the owner of the bucket will also be the MPU
                // upload owner. We need this value for testing comparison.
                displayName = res.DisplayName;
            })
            .then(() => s3.createMultipartUploadAsync({
                Bucket: bucket,
                Key: objectKey,
            }))
            .then(res => {
                uploadId = res.UploadId;
            })
            .catch(err => throwErr('Error in beforeEach', err));
        });

        afterEach(() =>
            s3.abortMultipartUploadAsync({
                Bucket: bucket,
                Key: objectKey,
                UploadId: uploadId,
            })
            .then(() => bucketUtil.empty(bucket))
            .then(() => bucketUtil.deleteOne(bucket))
            .catch(err => throwErr('Error in afterEach', err))
        );

        it('should list ongoing multipart uploads', done => {
            s3.listMultipartUploadsAsync({ Bucket: bucket })
            .then(res => checkValues(res, uploadId, displayName, done))
            .catch(done);
        });

        it('should list ongoing multipart uploads with params', done => {
            s3.listMultipartUploadsAsync({
                Bucket: bucket,
                Prefix: 'to',
                MaxUploads: 1,
            })
            .then(res => checkValues(res, uploadId, displayName, done))
            .catch(done);
        });
    });
});
