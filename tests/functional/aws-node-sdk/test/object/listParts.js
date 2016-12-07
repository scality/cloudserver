import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucket = 'bucketlistparts';
const key = 'key';
const bodyFirstPart = Buffer.allocUnsafe(10);
const bodySecondPart = Buffer.allocUnsafe(20);

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

describe('List parts', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let uploadId;
        let secondEtag;

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
            }).then(() => s3.uploadPartAsync({ Bucket: bucket, Key: key,
                PartNumber: 2, UploadId: uploadId, Body: bodySecondPart })
            ).then(res => {
                secondEtag = res.ETag;
                return secondEtag;
            })
            .catch(err => {
                process.stdout.write(`Error in beforeEach: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket');
            return s3.abortMultipartUploadAsync({
                Bucket: bucket, Key: key, UploadId: uploadId,
            })
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

        // remove the quote when forward porting to master
        it('should only list the second part', done => {
            s3.listParts({
                Bucket: bucket,
                Key: key,
                PartNumberMarker: 1,
                UploadId: uploadId },
            (err, data) => {
                checkNoError(err);
                assert.strictEqual(data.Parts[0].PartNumber, 2);
                assert.strictEqual(data.Parts[0].Size, 20);
                assert.strictEqual(`"${data.Parts[0].ETag}"`, secondEtag);
                done();
            });
        });
    });
});
