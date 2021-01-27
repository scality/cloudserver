const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const genMaxSizeMetaHeaders
    = require('../../lib/utility/genMaxSizeMetaHeaders');
const { generateMultipleTagQuery } = require('../../lib/utility/tagging');

const bucket = `initiatempubucket${Date.now()}`;
const key = 'key';

describe('Initiate MPU', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucket({ Bucket: bucket }).promise()
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

        describe('with tag set', () => {
            it('should be able to put object with 10 tags',
            done => {
                const taggingConfig = generateMultipleTagQuery(10);
                s3.createMultipartUpload({
                    Bucket: bucket,
                    Key: key,
                    Tagging: taggingConfig,
                }, err => {
                    assert.ifError(err);
                    done();
                });
            });

            it('should allow putting 50 tags', done => {
                const taggingConfig = generateMultipleTagQuery(50);
                s3.createMultipartUpload({
                    Bucket: bucket,
                    Key: key,
                    Tagging: taggingConfig,
                }, err => {
                    assert.ifError(err);
                    done();
                });
            });

            it('should return BadRequest if putting more that 50 tags',
            done => {
                const taggingConfig = generateMultipleTagQuery(51);
                s3.createMultipartUpload({
                    Bucket: bucket,
                    Key: key,
                    Tagging: taggingConfig,
                }, err => {
                    assert(err, 'Expected err but did not find one');
                    assert.strictEqual(err.code, 'BadRequest');
                    assert.strictEqual(err.statusCode, 400);
                    done();
                });
            });

            it('should return InvalidArgument creating mpu tag with ' +
            'invalid characters: %', done => {
                const value = 'value1%';
                s3.createMultipartUpload({
                    Bucket: bucket,
                    Key: key,
                    Tagging: `key1=${value}`,
                }, err => {
                    assert(err, 'Expected err but did not find one');
                    assert.strictEqual(err.code, 'InvalidArgument');
                    assert.strictEqual(err.statusCode, 400);
                    done();
                });
            });

            it('should return InvalidArgument creating mpu with ' +
            'bad encoded tags', done => {
                s3.createMultipartUpload({
                    Bucket: bucket,
                    Key: key,
                    Tagging: 'key1==value1',
                }, err => {
                    assert(err, 'Expected err but did not find one');
                    assert.strictEqual(err.code, 'InvalidArgument');
                    assert.strictEqual(err.statusCode, 400);
                    done();
                });
            });

            it('should return InvalidArgument if tag with no key', done => {
                s3.createMultipartUpload({
                    Bucket: bucket,
                    Key: key,
                    Tagging: '=value1',
                }, err => {
                    assert(err, 'Expected err but did not find one');
                    assert.strictEqual(err.code, 'InvalidArgument');
                    assert.strictEqual(err.statusCode, 400);
                    done();
                });
            });

            it('should return InvalidArgument if using the same key twice',
            done => {
                s3.createMultipartUpload({
                    Bucket: bucket,
                    Key: key,
                    Tagging: 'key1=value1&key1=value2',
                }, err => {
                    assert(err, 'Expected err but did not find one');
                    assert.strictEqual(err.code, 'InvalidArgument');
                    assert.strictEqual(err.statusCode, 400);
                    done();
                });
            });

            it('should return InvalidArgument if using the same key twice ' +
            'and empty tags', done => {
                s3.putObject({
                    Bucket: bucket,
                    Key: key,
                    Tagging: '&&&&&&&&&&&&&&&&&key1=value1&key1=value2',
                },
                err => {
                    assert(err, 'Expected err but did not find one');
                    assert.strictEqual(err.code, 'InvalidArgument');
                    assert.strictEqual(err.statusCode, 400);
                    done();
                });
            });
        });
    });
});
