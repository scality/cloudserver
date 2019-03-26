const assert = require('assert');
const async = require('async');
const crypto = require('crypto');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucketName = 'buckettestgetobject';
const objectName = 'someObject';
const copyPartKey = `${objectName}-copypart`;
// Specify sample headers to check for in GET response
const cacheControl = 'max-age=86400';
const contentDisposition = 'attachment; filename="fname.ext";';
const contentEncoding = 'aws-chunked,gzip';
const contentLanguage = 'en-US';
const contentType = 'xml';
// AWS Node SDK requires Date object, ISO-8601 string, or
// a UNIX timestamp for Expires header
const expires = new Date().toISOString();
const etagTrim = 'd41d8cd98f00b204e9800998ecf8427e';
const etag = `"${etagTrim}"`;
const partSize = 1024 * 1024 * 5; // 5MB minumum required part size.

function checkNoError(err) {
    expect(err).toEqual(null);
}

function checkError(err, code) {
    expect(err).not.toEqual(null);
    expect(err.code).toBe(code);
}

function checkContentLength(contentLengthHeader, expectedSize) {
    expect(Number.parseInt(contentLengthHeader, 10)).toBe(expectedSize);
}

function dateFromNow(diff) {
    const d = new Date();
    d.setHours(d.getHours() + diff);
    return d.toISOString();
}

function dateConvert(d) {
    return (new Date(d)).toISOString();
}


describe('GET object', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        function requestGet(fields, cb) {
            s3.getObject(Object.assign({
                Bucket: bucketName,
                Key: objectName,
            }, fields), cb);
        }

        function checkGetObjectPart(key, partNumber, len, body, cb) {
            s3.getObject({
                Bucket: bucketName,
                Key: key,
                PartNumber: partNumber,
            }, (err, data) => {
                checkNoError(err);
                checkContentLength(data.ContentLength, len);
                const md5Hash = crypto.createHash('md5');
                const md5HashExpected = crypto.createHash('md5');
                expect(md5Hash.update(data.Body).digest('hex')).toBe(md5HashExpected.update(body).digest('hex'));
                return cb();
            });
        }

        // Upload parts with the given partNumbers array and complete MPU.
        function completeMPU(partNumbers, cb) {
            let ETags = [];

            return async.waterfall([
                next => {
                    const createMpuParams = {
                        Bucket: bucketName,
                        Key: objectName,
                    };

                    s3.createMultipartUpload(createMpuParams, (err, data) => {
                        checkNoError(err);
                        return next(null, data.UploadId);
                    });
                },
                (uploadId, next) =>
                    async.eachSeries(partNumbers, (partNumber, callback) => {
                        const uploadPartParams = {
                            Bucket: bucketName,
                            Key: objectName,
                            PartNumber: partNumber,
                            UploadId: uploadId,
                            Body: Buffer.alloc(partSize).fill(partNumber),
                        };

                        return s3.uploadPart(uploadPartParams, (err, data) => {
                            checkNoError(err);
                            ETags = ETags.concat(data.ETag);
                            return callback();
                        });
                    }, err => next(err, uploadId)),
                (uploadId, next) => {
                    const parts = Array.from(Array(partNumbers.length).keys());
                    const params = {
                        Bucket: bucketName,
                        Key: objectName,
                        MultipartUpload: {
                            Parts: parts.map(n => ({
                                ETag: ETags[n],
                                PartNumber: partNumbers[n],
                            })),
                        },
                        UploadId: uploadId,
                    };
                    return s3.completeMultipartUpload(params, err => {
                        checkNoError(err);
                        return next(null, uploadId);
                    });
                },
            ], (err, uploadId) => {
                if (err) {
                    return s3.abortMultipartUpload({
                        Bucket: bucketName,
                        Key: objectName,
                        UploadId: uploadId,
                    }, cb);
                }
                return cb();
            });
        }

        function createMPUAndPutTwoParts(partTwoBody, cb) {
            let uploadId;
            const ETags = [];
            return async.waterfall([
                next => s3.createMultipartUpload({
                    Bucket: bucketName,
                    Key: copyPartKey,
                }, (err, data) => {
                    checkNoError(err);
                    uploadId = data.UploadId;
                    return next();
                }),
                // Copy an object with three parts.
                next => s3.uploadPartCopy({
                    Bucket: bucketName,
                    CopySource: `/${bucketName}/${objectName}`,
                    Key: copyPartKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                }, (err, data) => {
                    checkNoError(err);
                    ETags[0] = data.ETag;
                    return next();
                }),
                // Put an object with one part.
                next => s3.uploadPart({
                    Bucket: bucketName,
                    Key: copyPartKey,
                    PartNumber: 2,
                    UploadId: uploadId,
                    Body: partTwoBody,
                }, (err, data) => {
                    checkNoError(err);
                    ETags[1] = data.ETag;
                    return next();
                }),
            ], err => {
                if (err) {
                    return s3.abortMultipartUpload({
                        Bucket: bucketName,
                        Key: copyPartKey,
                        UploadId: uploadId,
                    }, cb);
                }
                return cb(null, uploadId, ETags);
            });
        }

        beforeAll(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            // Create a bucket to put object to get later
            s3.createBucket({ Bucket: bucketName }, done);
        });

        afterAll(done => {
            s3.deleteObject({ Bucket: bucketName, Key: objectName }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucketName }, done);
            });
        });


        test(
            'should return an error to get request without a valid bucket name',
            done => {
                s3.getObject({ Bucket: '', Key: 'somekey' }, err => {
                    expect(err).not.toEqual(null);
                    expect(err.code).toBe('MethodNotAllowed');
                    return done();
                });
            }
        );

        test('should return NoSuchKey error when no such object', done => {
            s3.getObject({ Bucket: bucketName, Key: 'nope' }, err => {
                expect(err).not.toEqual(null);
                expect(err.code).toBe('NoSuchKey');
                return done();
            });
        });

        describe('Additional headers: [Cache-Control, Content-Disposition, ' +
            'Content-Encoding, Expires, Accept-Ranges]', () => {
            describe('if specified in put object request', () => {
                beforeAll(done => {
                    const params = {
                        Bucket: bucketName,
                        Key: objectName,
                        CacheControl: cacheControl,
                        ContentDisposition: contentDisposition,
                        ContentEncoding: contentEncoding,
                        ContentType: contentType,
                        Expires: expires,
                    };
                    s3.putObject(params, err => done(err));
                });
                test('should return additional headers', done => {
                    s3.getObject({ Bucket: bucketName, Key: objectName },
                      (err, res) => {
                          if (err) {
                              return done(err);
                          }
                          expect(res.CacheControl).toBe(cacheControl);
                          expect(res.ContentDisposition).toBe(contentDisposition);
                          // Should remove V4 streaming value 'aws-chunked'
                          // to be compatible with AWS behavior
                          expect(res.ContentEncoding).toBe('gzip');
                          expect(res.ContentType).toBe(contentType);
                          expect(res.Expires).toBe(new Date(expires).toGMTString());
                          expect(res.AcceptRanges).toBe('bytes');
                          return done();
                      });
                });
            });

            describe('if response content headers are set in query', () => {
                beforeAll(done => {
                    s3.putObject({ Bucket: bucketName, Key: objectName },
                        err => done(err));
                });

                test('should return additional headers even if not set in ' +
                'put object request', done => {
                    const params = {
                        Bucket: bucketName,
                        Key: objectName,
                        ResponseCacheControl: cacheControl,
                        ResponseContentDisposition: contentDisposition,
                        ResponseContentEncoding: contentEncoding,
                        ResponseContentLanguage: contentLanguage,
                        ResponseContentType: contentType,
                        ResponseExpires: expires,
                    };
                    s3.getObject(params, (err, res) => {
                        if (err) {
                            return done(err);
                        }
                        expect(res.CacheControl).toBe(cacheControl);
                        expect(res.ContentDisposition).toBe(contentDisposition);
                        expect(res.ContentEncoding).toBe(contentEncoding);
                        expect(res.ContentLanguage).toBe(contentLanguage);
                        expect(res.ContentType).toBe(contentType);
                        expect(res.Expires).toBe(expires);
                        return done();
                    });
                });
            });
        });

        describe('x-amz-website-redirect-location header', () => {
            beforeAll(done => {
                const params = {
                    Bucket: bucketName,
                    Key: objectName,
                    WebsiteRedirectLocation: '/',
                };
                s3.putObject(params, err => done(err));
            });
            test('should return website redirect header if specified in ' +
                'objectPUT request', done => {
                s3.getObject({ Bucket: bucketName, Key: objectName },
                  (err, res) => {
                      if (err) {
                          return done(err);
                      }
                      expect(res.WebsiteRedirectLocation).toBe('/');
                      return done();
                  });
            });
        });

        describe('x-amz-tagging-count', () => {
            const params = {
                Bucket: bucketName,
                Key: objectName,
            };
            const paramsTagging = {
                Bucket: bucketName,
                Key: objectName,
                Tagging: {
                    TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value',
                        },
                    ],
                },
            };
            beforeEach(done => {
                s3.putObject(params, done);
            });

            test('should not return "x-amz-tagging-count" if no tag ' +
            'associated with the object', done => {
                s3.getObject(params, (err, data) => {
                    if (err) {
                        return done(err);
                    }
                    expect(data.TagCount).toBe(undefined);
                    return done();
                });
            });

            describe('tag associated with the object', () => {
                beforeEach(done => {
                    s3.putObjectTagging(paramsTagging, done);
                });
                test('should return "x-amz-tagging-count" header that provides ' +
                'the count of number of tags associated with the object', done => {
                    s3.getObject(params, (err, data) => {
                        if (err) {
                            return done(err);
                        }
                        expect(data.TagCount).toEqual(1);
                        return done();
                    });
                });
            });
        });

        describe('conditional headers', () => {
            const params = { Bucket: bucketName, Key: objectName };
            beforeEach(done => {
                s3.putObject(params, done);
            });
            test('If-Match: returns no error when ETag match, with double ' +
                'quotes around ETag', done => {
                requestGet({ IfMatch: etag }, err => {
                    checkNoError(err);
                    done();
                });
            });

            test('If-Match: returns no error when one of ETags match, with ' +
                'double quotes around ETag', done => {
                requestGet({ IfMatch:
                    `non-matching,${etag}` }, err => {
                    checkNoError(err);
                    done();
                });
            });

            test('If-Match: returns no error when ETag match, without double ' +
                'quotes around ETag', done => {
                requestGet({ IfMatch: etagTrim }, err => {
                    checkNoError(err);
                    done();
                });
            });

            test('If-Match: returns no error when one of ETags match, without ' +
                'double quotes around ETag', done => {
                requestGet({ IfMatch:
                    `non-matching,${etagTrim}` }, err => {
                    checkNoError(err);
                    done();
                });
            });

            test('If-Match: returns no error when ETag match with *', done => {
                requestGet({ IfMatch: '*' }, err => {
                    checkNoError(err);
                    done();
                });
            });

            test('If-Match: returns PreconditionFailed when ETag does not match', done => {
                requestGet({
                    IfMatch: 'non-matching ETag',
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

            test('If-None-Match: returns no error when ETag does not match', done => {
                requestGet({ IfNoneMatch: 'non-matching' }, err => {
                    checkNoError(err);
                    done();
                });
            });

            test('If-None-Match: returns no error when all ETags do not match', done => {
                requestGet({
                    IfNoneMatch: 'non-matching,' +
                    'non-matching-either',
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

            test('If-None-Match: returns NotModified when ETag match, with ' +
                'double quotes around ETag', done => {
                requestGet({ IfNoneMatch: etag }, err => {
                    checkError(err, 'NotModified');
                    done();
                });
            });

            test('If-None-Match: returns NotModified when one of ETags match, ' +
                'with double quotes around ETag', done => {
                requestGet({
                    IfNoneMatch: `non-matching,${etag}`,
                }, err => {
                    checkError(err, 'NotModified');
                    done();
                });
            });

            test('If-None-Match: returns NotModified when value is "*"', done => {
                requestGet({
                    IfNoneMatch: '*',
                }, err => {
                    checkError(err, 'NotModified');
                    done();
                });
            });

            test('If-None-Match: returns NotModified when ETag match, without ' +
                'double quotes around ETag', done => {
                requestGet({ IfNoneMatch: etagTrim }, err => {
                    checkError(err, 'NotModified');
                    done();
                });
            });

            test('If-None-Match: returns NotModified when one of ETags match, ' +
                'without double quotes around ETag', done => {
                requestGet({
                    IfNoneMatch: `non-matching,${etagTrim}`,
                }, err => {
                    checkError(err, 'NotModified');
                    done();
                });
            });

            test('If-Modified-Since: returns no error if Last modified date is ' +
                'greater', done => {
                requestGet({ IfModifiedSince: dateFromNow(-1) },
                    err => {
                        checkNoError(err);
                        done();
                    });
            });

            // Skipping this test, because real AWS does not provide error as
            // expected
            test.skip('If-Modified-Since: returns NotModified if Last modified ' +
                'date is lesser', done => {
                requestGet({ IfModifiedSince: dateFromNow(1) },
                    err => {
                        checkError(err, 'NotModified');
                        done();
                    });
            });

            test('If-Modified-Since: returns NotModified if Last modified ' +
                'date is equal', done => {
                s3.headObject({ Bucket: bucketName, Key: objectName },
                (err, data) => {
                    checkNoError(err);
                    const lastModified = dateConvert(data.LastModified);
                    requestGet({ IfModifiedSince: lastModified }, err => {
                        checkError(err, 'NotModified');
                        done();
                    });
                });
            });

            test('If-Unmodified-Since: returns no error when lastModified date ' +
                'is greater', done => {
                requestGet({ IfUnmodifiedSince: dateFromNow(1) },
                err => {
                    checkNoError(err);
                    done();
                });
            });

            test('If-Unmodified-Since: returns no error when lastModified ' +
                'date is equal', done => {
                s3.headObject({ Bucket: bucketName, Key: objectName },
                    (err, data) => {
                        checkNoError(err);
                        const lastModified = dateConvert(data.LastModified);
                        requestGet({ IfUnmodifiedSince: lastModified },
                            err => {
                                checkNoError(err);
                                done();
                            });
                    });
            });

            test('If-Unmodified-Since: returns PreconditionFailed when ' +
                'lastModified date is lesser', done => {
                requestGet({ IfUnmodifiedSince: dateFromNow(-1) },
                err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

            test('If-Match & If-Unmodified-Since: returns no error when match ' +
                'Etag and lastModified is greater', done => {
                requestGet({
                    IfMatch: etagTrim,
                    IfUnmodifiedSince: dateFromNow(-1),
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

            test('If-Match match & If-Unmodified-Since match', done => {
                requestGet({
                    IfMatch: etagTrim,
                    IfUnmodifiedSince: dateFromNow(1),
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

            test('If-Match not match & If-Unmodified-Since not match', done => {
                requestGet({
                    IfMatch: 'non-matching',
                    IfUnmodifiedSince: dateFromNow(-1),
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

            test('If-Match not match & If-Unmodified-Since match', done => {
                requestGet({
                    IfMatch: 'non-matching',
                    IfUnmodifiedSince: dateFromNow(1),
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

            // Skipping this test, because real AWS does not provide error as
            // expected
            test.skip('If-Match match & If-Modified-Since not match', done => {
                requestGet({
                    IfMatch: etagTrim,
                    IfModifiedSince: dateFromNow(1),
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

            test('If-Match match & If-Modified-Since match', done => {
                requestGet({
                    IfMatch: etagTrim,
                    IfModifiedSince: dateFromNow(-1),
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

            test('If-Match not match & If-Modified-Since not match', done => {
                requestGet({
                    IfMatch: 'non-matching',
                    IfModifiedSince: dateFromNow(1),
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

            test('If-Match not match & If-Modified-Since match', done => {
                requestGet({
                    IfMatch: 'non-matching',
                    IfModifiedSince: dateFromNow(-1),
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

            test('If-None-Match & If-Modified-Since: returns NotModified when ' +
                'Etag does not match and lastModified is greater', done => {
                requestGet({
                    IfNoneMatch: etagTrim,
                    IfModifiedSince: dateFromNow(-1),
                }, err => {
                    checkError(err, 'NotModified');
                    done();
                });
            });

            test('If-None-Match not match & If-Modified-Since not match', done => {
                requestGet({
                    IfNoneMatch: etagTrim,
                    IfModifiedSince: dateFromNow(1),
                }, err => {
                    checkError(err, 'NotModified');
                    done();
                });
            });

            test('If-None-Match match & If-Modified-Since match', done => {
                requestGet({
                    IfNoneMatch: 'non-matching',
                    IfModifiedSince: dateFromNow(-1),
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

            // Skipping this test, because real AWS does not provide error as
            // expected
            test.skip('If-None-Match match & If-Modified-Since not match', done => {
                requestGet({
                    IfNoneMatch: 'non-matching',
                    IfModifiedSince: dateFromNow(1),
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

            test('If-None-Match match & If-Unmodified-Since match', done => {
                requestGet({
                    IfNoneMatch: 'non-matching',
                    IfUnmodifiedSince: dateFromNow(1),
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

            test('If-None-Match match & If-Unmodified-Since not match', done => {
                requestGet({
                    IfNoneMatch: 'non-matching',
                    IfUnmodifiedSince: dateFromNow(-1),
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

            test('If-None-Match not match & If-Unmodified-Since match', done => {
                requestGet({
                    IfNoneMatch: etagTrim,
                    IfUnmodifiedSince: dateFromNow(1),
                }, err => {
                    checkError(err, 'NotModified');
                    done();
                });
            });

            test('If-None-Match not match & If-Unmodified-Since not match', done => {
                requestGet({
                    IfNoneMatch: etagTrim,
                    IfUnmodifiedSince: dateFromNow(-1),
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });
        });

        describe('With PartNumber field', () => {
            const orderedPartNumbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            const unOrderedPartNumbers = [3, 5, 9, 10, 12, 13, 20, 21, 22, 30];
            const invalidPartNumbers = [-1, 0, 10001];

            orderedPartNumbers.forEach(num =>
                test(
                    `should get the body of part ${num} when ordered MPU`,
                    done => completeMPU(orderedPartNumbers, err => {
                        checkNoError(err);
                        return requestGet({ PartNumber: num }, (err, data) => {
                            checkNoError(err);
                            checkContentLength(data.ContentLength, partSize);
                            const md5Hash = crypto.createHash('md5');
                            const md5HashExpected = crypto.createHash('md5');
                            const expected = Buffer.alloc(partSize).fill(num);
                            expect(md5Hash.update(data.Body).digest('hex')).toBe(md5HashExpected.update(expected).digest('hex'));
                            return done();
                        });
                    })
                ));

            // Use the orderedPartNumbers to retrieve parts with GetObject.
            orderedPartNumbers.forEach(num =>
                test(
                    `should get the body of part ${num} when unordered MPU`,
                    done => completeMPU(unOrderedPartNumbers, err => {
                        checkNoError(err);
                        return requestGet({ PartNumber: num }, (err, data) => {
                            checkNoError(err);
                            checkContentLength(data.ContentLength, partSize);
                            const md5Hash = crypto.createHash('md5');
                            const md5HashExpected = crypto.createHash('md5');
                            const expected = Buffer.alloc(partSize)
                                .fill(unOrderedPartNumbers[num - 1]);
                            expect(md5Hash.update(data.Body).digest('hex')).toBe(md5HashExpected.update(expected).digest('hex'));
                            return done();
                        });
                    })
                ));

            invalidPartNumbers.forEach(num =>
                test(
                    `should not accept a partNumber that is not 1-10000: ${num}`,
                    done => completeMPU(orderedPartNumbers, err => {
                        checkNoError(err);
                        return requestGet({ PartNumber: num }, err => {
                            checkError(err, 'InvalidArgument');
                            done();
                        });
                    })
                ));

            test('should not accept a part number greater than the total parts ' +
            'uploaded for an MPU', done =>
                completeMPU(orderedPartNumbers, err => {
                    checkNoError(err);
                    return requestGet({ PartNumber: 11 }, err => {
                        checkError(err, 'InvalidPartNumber');
                        done();
                    });
                }));

            test(
                'should accept a part number of 1 for regular put object',
                done => s3.putObject({
                    Bucket: bucketName,
                    Key: objectName,
                    Body: Buffer.alloc(10),
                }, err => {
                    checkNoError(err);
                    return requestGet({ PartNumber: 1 }, (err, data) => {
                        const md5Hash = crypto.createHash('md5');
                        const md5HashExpected = crypto.createHash('md5');
                        const expected = Buffer.alloc(10);
                        expect(md5Hash.update(data.Body).digest('hex')).toBe(md5HashExpected.update(expected).digest('hex'));
                        done();
                    });
                })
            );

            test('should accept a part number that is a string', done =>
                s3.putObject({
                    Bucket: bucketName,
                    Key: objectName,
                    Body: Buffer.alloc(10),
                }, err => {
                    checkNoError(err);
                    return requestGet({ PartNumber: '1' }, (err, data) => {
                        checkContentLength(data.ContentLength, 10);
                        const md5Hash = crypto.createHash('md5');
                        const md5HashExpected = crypto.createHash('md5');
                        const expected = Buffer.alloc(10);
                        expect(md5Hash.update(data.Body).digest('hex')).toBe(md5HashExpected.update(expected).digest('hex'));
                        done();
                    });
                }));

            test('should not accept a part number greater than 1 for regular ' +
            'put object', done =>
                s3.putObject({
                    Bucket: bucketName,
                    Key: objectName,
                    Body: Buffer.alloc(10),
                }, err => {
                    checkNoError(err);
                    return requestGet({ PartNumber: 2 }, err => {
                        checkError(err, 'InvalidPartNumber');
                        done();
                    });
                }));

            test('should not accept both PartNumber and Range as params', done =>
                completeMPU(orderedPartNumbers, err => {
                    checkNoError(err);
                    return requestGet({
                        PartNumber: 1,
                        Range: 'bytes=0-10',
                    }, err => {
                        checkError(err, 'InvalidRequest');
                        done();
                    });
                }));

            describe('uploadPartCopy', () => {
                // The original object was composed of three parts
                const partOneSize = partSize * 10;
                const bufs = orderedPartNumbers.map(n =>
                    Buffer.alloc(partSize, n));
                const partOneBody = Buffer.concat(bufs, partOneSize);
                const partTwoBody = Buffer.alloc(partSize, 4);

                beforeEach(done => async.waterfall([
                    next => completeMPU(orderedPartNumbers, next),
                    next => createMPUAndPutTwoParts(partTwoBody, next),
                    (uploadId, ETags, next) =>
                        s3.completeMultipartUpload({
                            Bucket: bucketName,
                            Key: copyPartKey,
                            MultipartUpload: {
                                Parts: [
                                    {
                                        ETag: ETags[0],
                                        PartNumber: 1,
                                    },
                                    {
                                        ETag: ETags[1],
                                        PartNumber: 2,
                                    },
                                ],
                            },
                            UploadId: uploadId,
                        }, next),
                ], done));

                afterEach(done => s3.deleteObject({
                    Bucket: bucketName,
                    Key: copyPartKey,
                }, done));

                test('should retrieve a part copied from an MPU', done =>
                    checkGetObjectPart(copyPartKey, 1, partOneSize, partOneBody,
                        done));

                test(
                    'should retrieve a part put after part copied from MPU',
                    done => checkGetObjectPart(copyPartKey, 2, partSize,
                        partTwoBody, done)
                );
            });

            describe('uploadPartCopy overwrite', () => {
                const partOneBody = Buffer.alloc(partSize, 1);
                // The original object was composed of three parts
                const partTwoSize = partSize * 10;
                const bufs = orderedPartNumbers.map(n =>
                    Buffer.alloc(partSize, n));
                const partTwoBody = Buffer.concat(bufs, partTwoSize);

                beforeEach(done => async.waterfall([
                    next => completeMPU(orderedPartNumbers, next),
                    next => createMPUAndPutTwoParts(partTwoBody, next),
                    /* eslint-disable no-param-reassign */
                    // Overwrite part one.
                    (uploadId, ETags, next) =>
                        s3.uploadPart({
                            Bucket: bucketName,
                            Key: copyPartKey,
                            PartNumber: 1,
                            UploadId: uploadId,
                            Body: partOneBody,
                        }, (err, data) => {
                            checkNoError(err);
                            ETags[0] = data.ETag;
                            return next(null, uploadId, ETags);
                        }),
                    // Overwrite part one with an three-part object.
                    (uploadId, ETags, next) =>
                        s3.uploadPartCopy({
                            Bucket: bucketName,
                            CopySource: `/${bucketName}/${objectName}`,
                            Key: copyPartKey,
                            PartNumber: 2,
                            UploadId: uploadId,
                        }, (err, data) => {
                            checkNoError(err);
                            ETags[1] = data.ETag;
                            return next(null, uploadId, ETags);
                        }),
                    /* eslint-enable no-param-reassign */
                    (uploadId, ETags, next) =>
                        s3.completeMultipartUpload({
                            Bucket: bucketName,
                            Key: copyPartKey,
                            MultipartUpload: {
                                Parts: [
                                    {
                                        ETag: ETags[0],
                                        PartNumber: 1,
                                    },
                                    {
                                        ETag: ETags[1],
                                        PartNumber: 2,
                                    },
                                ],
                            },
                            UploadId: uploadId,
                        }, next),
                ], done));

                afterEach(done => s3.deleteObject({
                    Bucket: bucketName,
                    Key: copyPartKey,
                }, done));

                test('should retrieve a part that overwrote another part ' +
                'originally copied from an MPU', done =>
                    checkGetObjectPart(copyPartKey, 1, partSize, partOneBody,
                        done));

                test('should retrieve a part copied from an MPU after the ' +
                'original part was overwritten', done => checkGetObjectPart(copyPartKey, 2, partTwoSize,
                    partTwoBody, done));
            });
        });

        describe('absent x-amz-website-redirect-location header', () => {
            beforeAll(done => {
                const params = {
                    Bucket: bucketName,
                    Key: objectName,
                };
                s3.putObject(params, err => done(err));
            });
            test('should return website redirect header if specified in ' +
                'objectPUT request', done => {
                s3.getObject({ Bucket: bucketName, Key: objectName },
                  (err, res) => {
                      if (err) {
                          return done(err);
                      }
                      expect(res.WebsiteRedirectLocation).toBe(undefined);
                      return done();
                  });
            });
        });
    });
});
