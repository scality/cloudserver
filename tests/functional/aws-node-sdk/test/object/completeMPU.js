import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const date = Date.now();
const bucket = `completempu${date}`;
const key = 'key';
const bodyFirstPart = Buffer.allocUnsafe(10).fill(0);
const bodySecondPart = Buffer.allocUnsafe(5).fill(0);

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

describe('Complete MPU', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let uploadId;
        let firstEtag;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .then(() => s3.createMultipartUploadAsync({
                Bucket: bucket, Key: key }))
            .then(res => {
                uploadId = res.UploadId;
                return s3.uploadPartAsync({ Bucket: bucket, Key: key,
                  PartNumber: 1, UploadId: uploadId, Body: bodyFirstPart });
            })
            .then(res => {
                firstEtag = res.ETag;
                return firstEtag;
            })
            .then(() => s3.uploadPartAsync({ Bucket: bucket, Key: key,
                PartNumber: 2, UploadId: uploadId, Body: bodySecondPart }))
            .catch(err => {
                process.stdout.write(`Error in beforeEach: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            });
        });

        it('should complete an MPU with fewer parts than were ' +
            'originally put', done => {
            s3.completeMultipartUpload({
                Bucket: bucket,
                Key: key,
                MultipartUpload: {
                    Parts: [{ ETag: firstEtag, PartNumber: 1 }],
                },
                UploadId: uploadId },
            (err, data) => {
                checkNoError(err);
                // to show that the mpu completed with just 1 part
                assert.strictEqual(data.ETag.slice(-3), '-1"');
                return s3.getObject({
                    Bucket: bucket,
                    Key: key,
                },
                (err, data) => {
                    checkNoError(err);
                    // to show that data in completed key is just first part
                    assert.strictEqual(data.ContentLength, '10');
                    done();
                });
            });
        });
    });
});
