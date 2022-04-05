const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const objectConfigs = require('../support/objectConfigs');

function checkError(err, statusCode, code) {
    assert.strictEqual(err.statusCode, statusCode);
    assert.strictEqual(err.code, code);
}

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

function generateContent(size, bodyContent) {
    return Buffer.alloc(size, bodyContent);
}

describe.only('Part size tests with object head', () => {
    objectConfigs.forEach(config => {
        describe(config.signature, () => {
            let ETags = [];

            const {
                bucket,
                object,
                bodySize,
                bodyContent,
                partNumbers,
                invalidPartNumbers,
            } = config;

            withV4(sigCfg => { //eslint-disable-line
                let bucketUtil;
                let s3;

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
                            let allocAmount = bodySize + partNumber + 1;
                            if (config.signature === 'for empty object') {
                                allocAmount = 0;
                            }
                            const uploadPartParams = {
                                Bucket: bucket,
                                Key: object,
                                PartNumber: partNumber + 1,
                                UploadId: this.currentTest.UploadId,
                                Body: generateContent(allocAmount, bodyContent),
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
                    const totalSize = config.meta.computeTotalSize(partNumbers, bodySize);

                    s3.headObject({ Bucket: bucket, Key: object }, (err, data) => {
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

                        s3.headObject({ Bucket: bucket, Key: object, PartNumber: partNumber }, (err, data) => {
                            checkNoError(err);
                            if (data.ContentLength === 0) {
                                done();
                            }
                            assert.equal(partSize, data.ContentLength);
                            done();
                        });
                    });
                });

                invalidPartNumbers.forEach(part => {
                    it(`should return an error when --part-number is set to ${part}`,
                    done => {
                        s3.headObject({ Bucket: bucket, Key: object, PartNumber: part }, (err, data) => {
                            checkError(err, 400, 'BadRequest');
                            assert.strictEqual(data, null);
                            done();
                        });
                    });
                });

                it('when incorrect --part-number is used', done => {
                    bucketUtil = new BucketUtility('default', sigCfg);
                    s3 = bucketUtil.s3;
                    s3.headObject({ Bucket: bucket, Key: object, PartNumber: partNumbers.length + 1 },
                    (err, data) => {
                        if (config.meta.objectIsEmpty) {
                            // returns metadata for the only empty part
                            checkNoError(err);
                            assert.strictEqual(data.ContentLength, 0);
                            done();
                        } else {
                            // returns a 416 error
                            // the error response does not contain the actual
                            // statusCode instead it has '416'
                            checkError(err, 416, 416);
                            assert.strictEqual(data, null);
                            done();
                        }
                    });
                });
            });
        });
    });
});
