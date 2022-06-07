const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { maximumAllowedPartCount } = require('../../../../../constants');

const bucket = 'mpu-test-bucket';
const object = 'mpu-test-object';

const bodySize = 1024 * 1024 * 5;
const bodyContent = 'a';
const howManyParts = 3;
const partNumbers = Array.from(Array(howManyParts).keys());
const invalidPartNumbers = [-1, 0, maximumAllowedPartCount + 1];

let ETags = [];

function checkError(err, statusCode, code) {
    assert.strictEqual(err.statusCode, statusCode);
    assert.strictEqual(err.code, code);
}

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

function generateContent(partNumber) {
    return Buffer.alloc(bodySize + partNumber, bodyContent);
}

describe('Part size tests with object head', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        function headObject(fields, cb) {
            s3.headObject(Object.assign({
                Bucket: bucket,
                Key: object,
            }, fields), cb);
        }

        beforeEach(function beforeF(done) {
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
                next => async.mapSeries(partNumbers, (partNumber, callback) => {
                    const uploadPartParams = {
                        Bucket: bucket,
                        Key: object,
                        PartNumber: partNumber + 1,
                        UploadId: this.currentTest.UploadId,
                        Body: generateContent(partNumber + 1),
                    };

                    return s3.uploadPart(uploadPartParams,
                        (err, data) => {
                            if (err) {
                                return callback(err);
                            }
                            return callback(null, data.ETag);
                        });
                }, (err, results) => {
                    checkNoError(err);
                    ETags = results;
                    return next();
                }),
                next => {
                    const params = {
                        Bucket: bucket,
                        Key: object,
                        MultipartUpload: {
                            Parts: partNumbers.map(partNumber => ({
                                ETag: ETags[partNumber],
                                PartNumber: partNumber + 1,
                            })),
                        },
                        UploadId: this.currentTest.UploadId,
                    };
                    return s3.completeMultipartUpload(params, next);
                },
            ], err => {
                checkNoError(err);
                done();
            });
        });

        afterEach(done => {
            async.waterfall([
                next => s3.deleteObject({ Bucket: bucket, Key: object },
                  err => next(err)),
                next => s3.deleteBucket({ Bucket: bucket }, err => next(err)),
            ], done);
        });

        it('should return the total size of the object ' +
            'when --part-number is not used', done => {
            const totalSize = partNumbers.reduce((total, current) =>
                total + (bodySize + current + 1), 0);
            headObject({}, (err, data) => {
                checkNoError(err);
                assert.equal(totalSize, data.ContentLength);
                done();
            });
        });

        partNumbers.forEach(part => {
            it(`should return the size of part ${part + 1} ` +
                `when --part-number is set to ${part + 1}`, done => {
                const partNumber = Number.parseInt(part, 0) + 1;
                const partSize = bodySize + partNumber;
                headObject({ PartNumber: partNumber }, (err, data) => {
                    checkNoError(err);
                    assert.equal(partSize, data.ContentLength);
                    done();
                });
            });
        });

        invalidPartNumbers.forEach(part => {
            it(`should return an error when --part-number is set to ${part}`,
            done => {
                headObject({ PartNumber: part }, (err, data) => {
                    checkError(err, 400, 'BadRequest');
                    assert.strictEqual(data, null);
                    done();
                });
            });
        });

        it('should return an error when incorrect --part-number is used',
            done => {
                headObject({ PartNumber: partNumbers.length + 1 },
                (err, data) => {
                    // the error response does not contain the actual
                    // statusCode instead it has '416'
                    checkError(err, 416, 416);
                    assert.strictEqual(data, null);
                    done();
                });
            });
    });
});
