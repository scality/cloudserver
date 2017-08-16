const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const genMaxSizeMetaHeaders
    = require('../../lib/utility/genMaxSizeMetaHeaders');

const bucket = `initiatempubucket${Date.now()}`;
const key = 'key';

describe('Initiate MPU', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => bucketUtil.deleteOne(bucket));

        it('should return InvalidRedirectLocation if initiate MPU ' +
        'with x-amz-website-redirect-location header that does not start ' +
        'with \'http://\', \'https://\' or \'/\'', done => {
            const params = { Bucket: bucket, Key: key,
                WebsiteRedirectLocation: 'google.com' };
            s3.createMultipartUpload(params, err => {
                assert.strictEqual(err.code, 'InvalidRedirectLocation');
                assert.strictEqual(err.statusCode, 400);
                done();
            });
        });

        it('should return error if initiating MPU w/ > 2KB user-defined md',
        done => {
            const metadata = genMaxSizeMetaHeaders();
            const params = { Bucket: bucket, Key: key, Metadata: metadata };
            async.waterfall([
                next => s3.createMultipartUpload(params, (err, data) => {
                    assert.strictEqual(err, null, `Unexpected err: ${err}`);
                    next(null, data.UploadId);
                }),
                (uploadId, next) => s3.abortMultipartUpload({
                    Bucket: bucket,
                    Key: key,
                    UploadId: uploadId,
                }, err => {
                    assert.strictEqual(err, null, `Unexpected err: ${err}`);
                    // add one more byte to push over limit for next call
                    metadata.header0 = `${metadata.header0}${'0'}`;
                    next();
                }),
                next => s3.createMultipartUpload(params, next),
            ], err => {
                assert(err, 'Expected err but did not find one');
                assert.strictEqual(err.code, 'MetadataTooLarge');
                assert.strictEqual(err.statusCode, 400);
                done();
            });
        });
    });
});
