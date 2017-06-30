const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'bucketlistparts';
const object = 'toto';

function checkError(err, statusCode, code) {
    assert.strictEqual(err.statusCode, statusCode);
    assert.strictEqual(err.code, code);
}

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

const body = Buffer.alloc(1024 * 1024 * 5, 'a');

const testsOrder = [
  { values: [3, 8, 1000], err: false },
  { values: [8, 3, 1000], err: true },
  { values: [8, 1000, 3], err: true },
  { values: [1000, 3, 8], err: true },
  { values: [3, 1000, 8], err: true },
  { values: [1000, 8, 3], err: true },
  { values: [3, 3, 1000], err: true },
];

describe('More MPU tests', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(function beforeEachF(done) {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            async.waterfall([
                next => s3.createBucket({ Bucket: bucket }, err => next(err)),
                next => s3.createMultipartUpload({ Bucket: bucket,
                Key: object }, (err, data) => {
                    checkNoError(err);
                    this.currentTest.UploadId = data.UploadId;
                    return next();
                }),
                next => s3.uploadPart({
                    Bucket: bucket,
                    Key: object,
                    PartNumber: 1000,
                    Body: body,
                    UploadId: this.currentTest.UploadId }, (err, data) => {
                    checkNoError(err);
                    this.currentTest.Etag = data.ETag;
                    return next();
                }),
                next => s3.uploadPart({
                    Bucket: bucket,
                    Key: object,
                    PartNumber: 3,
                    Body: body,
                    UploadId: this.currentTest.UploadId }, err => next(err)),
                next => s3.uploadPart({
                    Bucket: bucket,
                    Key: object,
                    PartNumber: 8,
                    Body: body,
                    UploadId: this.currentTest.UploadId }, err => next(err)),
            ], done);
        });

        afterEach(done => {
            async.waterfall([
                next => s3.deleteObject({ Bucket: bucket, Key: object },
                  err => next(err)),
                next => s3.deleteBucket({ Bucket: bucket }, err => next(err)),
            ], done);
        });
        testsOrder.forEach(testOrder => {
            it('should complete MPU by concatenating the parts in ' +
            `the following order: ${testOrder.values}`, function itF(done) {
                async.waterfall([
                    next => s3.completeMultipartUpload({
                        Bucket: bucket,
                        Key: object,
                        MultipartUpload: {
                            Parts: [
                                {
                                    ETag: this.test.Etag,
                                    PartNumber: testOrder.values[0],
                                },
                                {
                                    ETag: this.test.Etag,
                                    PartNumber: testOrder.values[1],
                                },
                                {
                                    ETag: this.test.Etag,
                                    PartNumber: testOrder.values[2],
                                },
                            ],
                        },
                        UploadId: this.test.UploadId }, next),
                ], err => {
                    if (testOrder.err) {
                        checkError(err, 400, 'InvalidPartOrder');
                        return s3.abortMultipartUpload({
                            Bucket: bucket,
                            Key: object,
                            UploadId: this.test.UploadId,
                        }, done);
                    }
                    checkNoError(err);
                    return done();
                });
            });
        });
    });
});
