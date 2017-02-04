import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucket = 'functestabortmultipart';
const key = 'key';

describe('DELETE multipart', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error in beforeEach: ${err}\n`);
                throw err;
            });
        });

        after(() => {
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

        it('should return 204 if uploadId does not exist on' +
        'multipart abort call', done => {
            const uploadId = 'nonexistinguploadid';
            const request =
            s3.abortMultipartUpload({ Bucket: bucket, Key: key,
                UploadId: uploadId }, err => {
                const statusCode =
                request.response.httpResponse.statusCode;
                assert.strictEqual(statusCode, 204,
                    `Found unexpected statusCode ${statusCode}`);
                assert.strictEqual(err, null,
                    `Expected no err but found ${err}`);
                done(err);
            });
        });
    });
});
