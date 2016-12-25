import assert from 'assert';
import Promise from 'bluebird';
import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucketName = 'testdeletempu';
const objectName = 'key';

describe('DELETE object', () => {
    withV4(sigCfg => {
        let uploadId;
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const testfile = new Buffer(1024 * 1024 * 54);

        before(() => {
            process.stdout.write('creating bucket\n');
            return s3.createBucketAsync({ Bucket: bucketName })
            .then(() => {
                process.stdout.write('initiating multipart upload\n');
                return s3.createMultipartUploadAsync({ Bucket: bucketName,
                    Key: objectName });
            })
            .then(res => {
                process.stdout.write('uploading parts\n');
                uploadId = res.UploadId;
                const uploads = [];
                for (let i = 1; i <= 3; i++) {
                    uploads.push(
                        s3.uploadPartAsync({ Bucket: bucketName,
                            Key: objectName, PartNumber: i, Body: testfile,
                            UploadId: uploadId })
                    );
                }
                return Promise.all(uploads);
            })
            .catch(err => {
                process.stdout.write(`Error with uploadPart ${err}\n`);
                throw err;
            })
            .then(res => {
                process.stdout.write('about to complete multipart upload\n');
                return s3.completeMultipartUploadAsync({
                    Bucket: bucketName,
                    Key: objectName,
                    UploadId: uploadId,
                    MultipartUpload: {
                        Parts: [
                            { ETag: res[0].ETag, PartNumber: 1 },
                            { ETag: res[1].ETag, PartNumber: 2 },
                            { ETag: res[2].ETag, PartNumber: 3 },
                        ],
                    },
                });
            })
            .catch(err => {
                process.stdout.write(`completeMultipartUpload error: ${err}\n`);
                throw err;
            });
        });

        after(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucketName)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucketName);
            })
            .catch(err => {
                process.stdout.write('Error in after\n');
                throw err;
            });
        });

        it('should delete a object uploaded in parts successfully', done => {
            s3.deleteObject({ Bucket: bucketName, Key: objectName }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                done();
            });
        });
    });
});
