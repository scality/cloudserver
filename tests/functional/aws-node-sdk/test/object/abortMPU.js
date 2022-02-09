const assert = require('assert');
const { v4: uuidv4 } = require('uuid');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const date = Date.now();
const bucket = `abortmpu${date}`;
const key = 'key';
const bodyFirstPart = Buffer.allocUnsafe(10).fill(0);

function checkError(err, code, message) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.code, code);
    assert.strictEqual(err.message, message);
}

// TODO: CLDSRV-124, test fails because of arsenal changes for metadata search
describe.skip('Abort MPU', () => {
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
                return s3.uploadPart({ Bucket: bucket, Key: key,
                    PartNumber: 1, UploadId: uploadId, Body: bodyFirstPart,
                }).promise();
            })
            .catch(err => {
                process.stdout.write(`Error in beforeEach: ${err}\n`);
                throw err;
            });
        });

        afterEach(() =>
            s3.abortMultipartUpload({
                Bucket: bucket,
                Key: key,
                UploadId: uploadId,
            }).promise()
            .then(() => bucketUtil.empty(bucket))
            .then(() => bucketUtil.deleteOne(bucket))
        );

        // aws-sdk now (v2.363.0) returns 'UriParameterError' error
        // this test was not replaced in any other suite
        it.skip('should return InvalidRequest error if aborting without key',
        done => {
            s3.abortMultipartUpload({
                Bucket: bucket,
                Key: '',
                UploadId: uploadId },
            err => {
                checkError(err, 'InvalidRequest', 'A key must be specified');
                done();
            });
        });
    });
});

// TODO: CLDSRV-124, test fails because of arsenal changes for metadata search
describe.skip('Abort MPU - No Such Upload', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucket({ Bucket: bucket }).promise();
        });

        afterEach(() => bucketUtil.deleteOne(bucket));

        it('should return NoSuchUpload error when aborting non-existent mpu',
        done => {
            s3.abortMultipartUpload({
                Bucket: bucket,
                Key: key,
                UploadId: uuidv4().replace(/-/g, '') },
            err => {
                assert.notEqual(err, null, 'Expected failure but got success');
                assert.strictEqual(err.code, 'NoSuchUpload');
                done();
            });
        });
    });
});
