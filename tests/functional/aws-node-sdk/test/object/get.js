const { promisify } = require('util');
const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const moment = require('moment');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    DeleteObjectCommand,
    GetObjectCommand,
    HeadObjectCommand,
    PutObjectCommand,
    PutObjectTaggingCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    UploadPartCopyCommand,
    CompleteMultipartUploadCommand,
    AbortMultipartUploadCommand,
    ListObjectVersionsCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const changeObjectLock = require('../../../../utilities/objectLock-util');
const { algorithms } = require('../../../../../lib/api/apiUtils/integrity/validateChecksums');

const { crc64NvmeCrtContainer } = require('@aws-sdk/middleware-flexible-checksums');
if (crc64NvmeCrtContainer) {
    const { CrtCrc64Nvme } = require('@aws-sdk/crc64-nvme-crt');
    crc64NvmeCrtContainer.CrtCrc64Nvme = CrtCrc64Nvme;
}

const changeLockPromise = promisify(changeObjectLock);

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
const expires = new Date();
const etagTrim = 'd41d8cd98f00b204e9800998ecf8427e';
const etag = `"${etagTrim}"`;
const partSize = 1024 * 1024 * 5; // 5MB minumum required part size.

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

function checkError(err, code) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.name, code);
}

function checkIntegerHeader(integerHeader, expectedSize) {
    assert.strictEqual(Number.parseInt(integerHeader, 10), expectedSize);
}

function dateFromNow(diff) {
    const d = new Date();
    d.setHours(d.getHours() + diff);
    return d;
}

function dateConvert(d) {
    return new Date(d);
}

describe('GET object', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        function requestGet(fields, cb) {
            s3.send(new GetObjectCommand(Object.assign({
                Bucket: bucketName,
                Key: objectName,
            }, fields))).then(data => cb(null, data)).catch(err => {
                if (err.$metadata.httpStatusCode === 304) {
                    const notModifiedError = new Error('NotModified');
                    notModifiedError.name = 'NotModified';
                    notModifiedError.$metadata = err.$metadata;
                    return cb(notModifiedError);
                }
                return cb(err);
            });
        }

        const requestGetPromise = promisify(requestGet);

        function checkGetObjectPart(key, partNumber, len, body, cb) {
            s3.send(new GetObjectCommand({
                Bucket: bucketName,
                Key: key,
                PartNumber: partNumber,
            })).then(async data => {
                checkIntegerHeader(data.ContentLength, len);
                const md5Hash = crypto.createHash('md5');
                const md5HashExpected = crypto.createHash('md5');
                const bodyText = await data.Body.transformToString();
                assert.strictEqual(
                    md5Hash.update(bodyText).digest('hex'),
                    md5HashExpected.update(body).digest('hex')
                );
                return cb();
            }).catch(cb);
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

                    s3.send(new CreateMultipartUploadCommand(createMpuParams)).then(data => 
                        next(null, data.UploadId)).catch(next);
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
                        return s3.send(new UploadPartCommand(uploadPartParams)).then(data => {
                            ETags = ETags.concat(data.ETag);
                            return callback();
                        }).catch(callback);
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
                    return s3.send(new CompleteMultipartUploadCommand(params)).then(() => 
                        next(null, uploadId)).catch(next);
                },
            ], (err, uploadId) => {
                if (err) {
                    if (uploadId) {
                        return s3.send(new AbortMultipartUploadCommand({
                            Bucket: bucketName,
                            Key: objectName,
                            UploadId: uploadId,
                        })).then(() => cb(err)).catch(() => cb(err));
                    }
                    return cb(err);
                }
                return cb();
            });
        }

        function createMPUAndPutTwoParts(partTwoBody, cb) {
            let uploadId;
            const ETags = [];
            return async.waterfall([
                next => s3.send(new CreateMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: copyPartKey,
                })).then(data => {
                    uploadId = data.UploadId;
                    return next();
                }).catch(next),
                // Copy an object with three parts.
                next => s3.send(new UploadPartCopyCommand({
                    Bucket: bucketName,
                    CopySource: `/${bucketName}/${objectName}`,
                    Key: copyPartKey,
                    PartNumber: 1,
                    UploadId: uploadId,
                })).then(data => {
                    ETags[0] = data.CopyPartResult.ETag;
                    return next();
                }).catch(next),
                // Put an object with one part.
                next => s3.send(new UploadPartCommand({
                    Bucket: bucketName,
                    Key: copyPartKey,
                    PartNumber: 2,
                    UploadId: uploadId,
                    Body: partTwoBody,
                })).then(data => {
                    ETags[1] = data.ETag;
                    return next();
                }).catch(next),
            ], err => {
                if (err) {
                    if (uploadId) {
                        return s3.send(new AbortMultipartUploadCommand({
                            Bucket: bucketName,
                            Key: copyPartKey,
                            UploadId: uploadId,
                        })).then(() => cb(err)).catch(() => cb(err));
                    }
                    return cb(err);
                }
                return cb(null, uploadId, ETags);
            });
        }

        before(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            // Create a bucket to put object to get later
            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
        });

        after(async () => {
                await s3.send(new DeleteObjectCommand({ Bucket: bucketName, Key: objectName }));
                await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
        });

        it('should return NoSuchKey error when no such object',
            done => {
                s3.send(new GetObjectCommand({ Bucket: bucketName, Key: 'nope' })).then(() => {
                    assert.fail('Expected failure but got success');
                }).catch(err => {
                    assert.strictEqual(err.name, 'NoSuchKey');
                    return done();
                });
            });

        it('should return NoSuchKey error when no such object even with key longer than 915 bytes',
            done => {
                s3.send(new GetObjectCommand({ Bucket: bucketName, Key: 'a'.repeat(2000) })).then(() => {
                    assert.fail('Expected failure but got success');
                }).catch(err => {
                    assert.strictEqual(err.name, 'NoSuchKey');
                    return done();
                });
            });

        describe('Additional headers: [Cache-Control, Content-Disposition, ' +
            'Content-Encoding, Expires, Accept-Ranges]', () => {
            describe('if specified in put object request', () => {
                before(async () => {
                    const params = {
                        Bucket: bucketName,
                        Key: objectName,
                        CacheControl: cacheControl,
                        ContentDisposition: contentDisposition,
                        ContentEncoding: contentEncoding,
                        ContentType: contentType,
                        Expires: expires,
                    };
                    await s3.send(new PutObjectCommand(params));
                });
                it('should return additional headers', done => {
                    s3.send(new GetObjectCommand({ Bucket: bucketName, Key: objectName })).then(res => {
                        assert.strictEqual(res.CacheControl,
                          cacheControl);
                        assert.strictEqual(res.ContentDisposition,
                          contentDisposition);
                        // Should remove V4 streaming value 'aws-chunked'
                        // to be compatible with AWS behavior
                        assert.strictEqual(res.ContentEncoding,
                          'gzip');
                        assert.strictEqual(res.ContentType, contentType);
                        assert.strictEqual(res.Expires.toGMTString(),
                          new Date(expires).toGMTString());
                        assert.strictEqual(res.AcceptRanges, 'bytes');
                        return done();
                    }).catch(done);
                });
            });

            describe('if response content headers are set in query', () => {
                before(async () => {
                    await s3.send(new PutObjectCommand({ Bucket: bucketName, Key: objectName }));
                });

                it('should return additional headers even if not set in ' +
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
                    s3.send(new GetObjectCommand(params)).then(res => {
                        assert.strictEqual(res.CacheControl,
                          cacheControl);
                        assert.strictEqual(res.ContentDisposition,
                          contentDisposition);
                        assert.strictEqual(res.ContentEncoding,
                          contentEncoding);
                        assert.strictEqual(res.ContentLanguage,
                            contentLanguage);
                        assert.strictEqual(res.ContentType, contentType);
                        assert.strictEqual(res.Expires.toGMTString(),
                            new Date(expires).toGMTString());
                        return done();
                    }).catch(done);
                });
            });
        });

        describe('x-amz-website-redirect-location header', () => {
            before(async () => {
                const params = {
                    Bucket: bucketName,
                    Key: objectName,
                    WebsiteRedirectLocation: '/',
                };
                await s3.send(new PutObjectCommand(params));
            });
            it('should return website redirect header if specified in ' +
                'objectPUT request', done => {
                s3.send(new GetObjectCommand({ Bucket: bucketName, Key: objectName })).then(res => {
                    assert.strictEqual(res.WebsiteRedirectLocation, '/');
                    return done();
                }).catch(done);
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
            beforeEach(async () => {
                await s3.send(new PutObjectCommand(params));
            });

            it('should not return "x-amz-tagging-count" if no tag ' +
            'associated with the object',
            done => {
                s3.send(new GetObjectCommand(params)).then(data => {
                    assert.strictEqual(data.TagCount, undefined);
                    return done();
                }).catch(done);
            });

            describe('tag associated with the object', () => {
                beforeEach(async () => {
                    await s3.send(new PutObjectTaggingCommand(paramsTagging));
                });
                it('should return "x-amz-tagging-count" header that provides ' +
                'the count of number of tags associated with the object',
                done => {
                    s3.send(new GetObjectCommand(params)).then(data => {
                        assert.equal(data.TagCount, 1);
                        return done();
                    }).catch(done);
                });
            });
        });

        describe('conditional headers', () => {
            const params = { Bucket: bucketName, Key: objectName };
            beforeEach(async () => {
                await s3.send(new PutObjectCommand(params));
            });
            it('If-Match: returns no error when ETag match, with double ' +
                'quotes around ETag',
                done => {
                    requestGet({ IfMatch: etag }, err => {
                        checkNoError(err);
                        done();
                    });
                });

            it('If-Match: returns no error when one of ETags match, with ' +
                'double quotes around ETag',
                done => {
                    requestGet({ IfMatch:
                        `non-matching,${etag}` }, err => {
                        checkNoError(err);
                        done();
                    });
                });

            it('If-Match: returns no error when ETag match, without double ' +
                'quotes around ETag',
                done => {
                    requestGet({ IfMatch: etagTrim }, err => {
                        checkNoError(err);
                        done();
                    });
                });

            it('If-Match: returns no error when one of ETags match, without ' +
                'double quotes around ETag',
                done => {
                    requestGet({ IfMatch:
                        `non-matching,${etagTrim}` }, err => {
                        checkNoError(err);
                        done();
                    });
                });

            it('If-Match: returns no error when ETag match with *', done => {
                requestGet({ IfMatch: '*' }, err => {
                    checkNoError(err);
                    done();
                });
            });

            it('If-Match: returns PreconditionFailed when ETag does not match',
                done => {
                    requestGet({
                        IfMatch: 'non-matching ETag',
                    }, err => {
                        checkError(err, 'PreconditionFailed');
                        done();
                    });
                });

            it('If-None-Match: returns no error when ETag does not match',
                done => {
                    requestGet({ IfNoneMatch: 'non-matching' }, err => {
                        checkNoError(err);
                        done();
                    });
                });

            it('If-None-Match: returns no error when all ETags do not match',
                done => {
                    requestGet({
                        IfNoneMatch: 'non-matching,' +
                        'non-matching-either',
                    }, err => {
                        checkNoError(err);
                        done();
                    });
                });

            it('If-None-Match: returns NotModified when ETag match, with ' +
                'double quotes around ETag',
                done => {
                    requestGet({ IfNoneMatch: etag }, err => {
                        checkError(err, 'NotModified');
                        done();
                    });
                });

            it('If-None-Match: returns NotModified when one of ETags match, ' +
                'with double quotes around ETag',
                done => {
                    requestGet({
                        IfNoneMatch: `non-matching,${etag}`,
                    }, err => {
                        checkError(err, 'NotModified');
                        done();
                    });
                });

            it('If-None-Match: returns NotModified when value is "*"',
                done => {
                    requestGet({
                        IfNoneMatch: '*',
                    }, err => {
                        checkError(err, 'NotModified');
                        done();
                    });
                });

            it('If-None-Match: returns NotModified when ETag match, without ' +
                'double quotes around ETag',
                done => {
                    requestGet({ IfNoneMatch: etagTrim }, err => {
                        checkError(err, 'NotModified');
                        done();
                    });
                });

            it('If-None-Match: returns NotModified when one of ETags match, ' +
                'without double quotes around ETag',
                done => {
                    requestGet({
                        IfNoneMatch: `non-matching,${etagTrim}`,
                    }, err => {
                        checkError(err, 'NotModified');
                        done();
                    });
                });

            it('If-Modified-Since: returns no error if Last modified date is ' +
                'greater',
                done => {
                    requestGet({ IfModifiedSince: dateFromNow(-1) },
                        err => {
                            checkNoError(err);
                            done();
                        });
                });

            // Skipping this test, because real AWS does not provide error as
            // expected
            it.skip('If-Modified-Since: returns NotModified if Last modified ' +
                'date is lesser',
                done => {
                    requestGet({ IfModifiedSince: dateFromNow(1) },
                        err => {
                            checkError(err, 'NotModified');
                            done();
                        });
                });

            it('If-Modified-Since: returns NotModified if Last modified ' +
                'date is equal',
                done => {
                    s3.send(new HeadObjectCommand({ Bucket: bucketName, Key: objectName })).then(data => {
                        const lastModified = dateConvert(data.LastModified);
                        requestGet({ IfModifiedSince: lastModified }, err => {
                            checkError(err, 'NotModified');
                            done();
                        });
                    }).catch(done);
                });

            it('If-Unmodified-Since: returns no error when lastModified date ' +
                'is greater',
                done => {
                    requestGet({ IfUnmodifiedSince: dateFromNow(1) },
                    err => {
                        checkNoError(err);
                        done();
                    });
                });

            it('If-Unmodified-Since: returns no error when lastModified ' +
                'date is equal', done => {
                s3.send(new HeadObjectCommand({ Bucket: bucketName, Key: objectName })).then(data => {
                    const lastModified = dateConvert(data.LastModified);
                    requestGet({ IfUnmodifiedSince: lastModified },
                        err => {
                            checkNoError(err);
                            done();
                        });
                }).catch(done);
            });

            it('If-Unmodified-Since: returns PreconditionFailed when ' +
                'lastModified date is lesser',
                done => {
                    requestGet({ IfUnmodifiedSince: dateFromNow(-1) },
                    err => {
                        checkError(err, 'PreconditionFailed');
                        done();
                    });
                });

            it('If-Match & If-Unmodified-Since: returns no error when match ' +
                'Etag and lastModified is greater',
                done => {
                    requestGet({
                        IfMatch: etagTrim,
                        IfUnmodifiedSince: dateFromNow(-1),
                    }, err => {
                        checkNoError(err);
                        done();
                    });
                });

            it('If-Match match & If-Unmodified-Since match', done => {
                requestGet({
                    IfMatch: etagTrim,
                    IfUnmodifiedSince: dateFromNow(1),
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

            it('If-Match not match & If-Unmodified-Since not match', done => {
                requestGet({
                    IfMatch: 'non-matching',
                    IfUnmodifiedSince: dateFromNow(-1),
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

            it('If-Match not match & If-Unmodified-Since match', done => {
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
            it.skip('If-Match match & If-Modified-Since not match', done => {
                requestGet({
                    IfMatch: etagTrim,
                    IfModifiedSince: dateFromNow(1),
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

            it('If-Match match & If-Modified-Since match', done => {
                requestGet({
                    IfMatch: etagTrim,
                    IfModifiedSince: dateFromNow(-1),
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

            it('If-Match not match & If-Modified-Since not match', done => {
                requestGet({
                    IfMatch: 'non-matching',
                    IfModifiedSince: dateFromNow(1),
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

            it('If-Match not match & If-Modified-Since match', done => {
                requestGet({
                    IfMatch: 'non-matching',
                    IfModifiedSince: dateFromNow(-1),
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

            it('If-None-Match & If-Modified-Since: returns NotModified when ' +
                'Etag does not match and lastModified is greater',
                done => {
                    requestGet({
                        IfNoneMatch: etagTrim,
                        IfModifiedSince: dateFromNow(1),
                    }, err => {
                        checkError(err, 'NotModified');
                        done();
                    });
                });

            it('If-None-Match not match & If-Modified-Since not match',
            done => {
                requestGet({
                    IfNoneMatch: etagTrim,
                    IfModifiedSince: dateFromNow(1),
                }, err => {
                    checkError(err, 'NotModified');
                    done();
                });
            });

            it('If-None-Match match & If-Modified-Since match', done => {
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
            it.skip('If-None-Match match & If-Modified-Since not match',
            done => {
                requestGet({
                    IfNoneMatch: 'non-matching',
                    IfModifiedSince: dateFromNow(1),
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

            it('If-None-Match match & If-Unmodified-Since match', done => {
                requestGet({
                    IfNoneMatch: 'non-matching',
                    IfUnmodifiedSince: dateFromNow(1),
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

            it('If-None-Match match & If-Unmodified-Since not match', done => {
                requestGet({
                    IfNoneMatch: 'non-matching',
                    IfUnmodifiedSince: dateFromNow(-1),
                }, err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
            });

            it('If-None-Match not match & If-Unmodified-Since match', done => {
                requestGet({
                    IfNoneMatch: etagTrim,
                    IfUnmodifiedSince: dateFromNow(1),
                }, err => {
                    checkError(err, 'NotModified');
                    done();
                });
            });

            it('If-None-Match not match & If-Unmodified-Since not match',
            done => {
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
                it(`should get the body of part ${num} when ordered MPU`,
                    done => completeMPU(orderedPartNumbers, err => {
                        checkNoError(err);
                        return requestGet({ PartNumber: num }, async (err, data) => {
                            checkNoError(err);
                            checkIntegerHeader(data.ContentLength, partSize);
                            const md5Hash = crypto.createHash('md5');
                            const md5HashExpected = crypto.createHash('md5');
                            const expected = Buffer.alloc(partSize).fill(num);
                            const bodyText = await data.Body.transformToString();
                            assert.strictEqual(
                                md5Hash.update(bodyText).digest('hex'),
                                md5HashExpected.update(expected).digest('hex')
                            );
                            return done();
                        });
                    })));

            // Use the orderedPartNumbers to retrieve parts with GetObject.
            orderedPartNumbers.forEach(num =>
                it(`should get the body of part ${num} when unordered MPU`,
                    done => completeMPU(unOrderedPartNumbers, err => {
                        checkNoError(err);
                        return requestGet({ PartNumber: num }, async (err, data) => {
                            checkNoError(err);
                            checkIntegerHeader(data.ContentLength, partSize);
                            const md5Hash = crypto.createHash('md5');
                            const md5HashExpected = crypto.createHash('md5');
                            const expected = Buffer.alloc(partSize)
                                .fill(unOrderedPartNumbers[num - 1]);
                            const bodyText = await data.Body.transformToString();
                            assert.strictEqual(
                                md5Hash.update(bodyText).digest('hex'),
                                md5HashExpected.update(expected).digest('hex')
                            );
                            return done();
                        });
                    })));

            invalidPartNumbers.forEach(num =>
                it(`should not accept a partNumber that is not 1-10000: ${num}`,
                done => completeMPU(orderedPartNumbers, err => {
                    checkNoError(err);
                    return requestGet({ PartNumber: num }, err => {
                        checkError(err, 'InvalidArgument');
                        done();
                    });
                })));

            it('should not accept a part number greater than the total parts ' +
            'uploaded for an MPU', done =>
                completeMPU(orderedPartNumbers, err => {
                    checkNoError(err);
                    return requestGet({ PartNumber: 11 }, err => {
                        checkError(err, 'InvalidPartNumber');
                        done();
                    });
                }));

            it('should accept a part number of 1 for regular put object',
                async () => {
                    await s3.send(new PutObjectCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        Body: Buffer.alloc(10),
                    }));
                    
                    const data = await requestGetPromise({ PartNumber: 1 });
                    const md5Hash = crypto.createHash('md5');
                    const md5HashExpected = crypto.createHash('md5');
                    const expected = Buffer.alloc(10).fill(0);
                    const bodyText = await data.Body.transformToString();
                    assert.strictEqual(
                        md5Hash.update(bodyText).digest('hex'),
                        md5HashExpected.update(expected).digest('hex')
                    );
                });

            it('should accept a part number that is a string', async () => {
                await s3.send(new PutObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Body: Buffer.alloc(10),
                }));
                
                const data = await requestGetPromise({ PartNumber: '1' });
                checkIntegerHeader(data.ContentLength, 10); 
                const md5Hash = crypto.createHash('md5');
                const md5HashExpected = crypto.createHash('md5');
                const expected = Buffer.alloc(10).fill(0);
                const bodyText = await data.Body.transformToString();
                assert.strictEqual(
                    md5Hash.update(bodyText).digest('hex'),
                    md5HashExpected.update(expected).digest('hex')
                );
            });

            it('should not accept a part number greater than 1 for regular ' +
            'put object', async () => {
                await s3.send(new PutObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Body: Buffer.alloc(10),
                }));
                
                await assert.rejects(
        () => requestGetPromise({ PartNumber: 2 }),
        err => {
            checkError(err, 'InvalidPartNumber');
            return true;
        }
    );
            });

            it('should not accept both PartNumber and Range as params', done =>
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

            it('should not include PartsCount response header for regular ' +
            'put object', async () => {
                await s3.send(new PutObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Body: Buffer.alloc(10),
                }));
                
                const data = await requestGetPromise({ PartNumber: 1 });
                assert.strictEqual('PartsCount' in data, false,
                    'PartsCount header is present.');
            });

            it('should include PartsCount response header for mpu object',
            done => {
                completeMPU(orderedPartNumbers, err => {
                    assert.ifError(err);
                    return requestGet({ PartNumber: 1 }, (err, data) => {
                        assert.ifError(err);
                        checkIntegerHeader(data.PartsCount, 10);
                        done();
                    });
                });
            });

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
                        s3.send(new CompleteMultipartUploadCommand({
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
                        })).then(() => next()).catch(next),
                ], done));

                afterEach(async () => {
                    await s3.send(new DeleteObjectCommand({
                        Bucket: bucketName,
                        Key: copyPartKey,
                    }));
                });

                it('should retrieve a part copied from an MPU', done =>
                    checkGetObjectPart(copyPartKey, 1, partOneSize, partOneBody,
                        done));

                it('should retrieve a part put after part copied from MPU',
                    done => checkGetObjectPart(copyPartKey, 2, partSize,
                        partTwoBody, done));
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
                        s3.send(new UploadPartCommand({
                            Bucket: bucketName,
                            Key: copyPartKey,
                            PartNumber: 1,
                            UploadId: uploadId,
                            Body: partOneBody,
                        })).then(data => {
                            ETags[0] = data.ETag;
                            return next(null, uploadId, ETags);
                        }).catch(next),
                    // Overwrite part one with an three-part object.
                    (uploadId, ETags, next) =>
                        s3.send(new UploadPartCopyCommand({
                            Bucket: bucketName,
                            CopySource: `/${bucketName}/${objectName}`,
                            Key: copyPartKey,
                            PartNumber: 2,
                            UploadId: uploadId,
                        })).then(data => {
                            ETags[1] = data.CopyPartResult.ETag;
                            return next(null, uploadId, ETags);
                        }).catch(next),
                    /* eslint-enable no-param-reassign */
                    (uploadId, ETags, next) =>
                        s3.send(new CompleteMultipartUploadCommand({
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
                        })).then(() => next()).catch(next),
                ], done));

                afterEach(async () => {
                    await s3.send(new DeleteObjectCommand({
                        Bucket: bucketName,
                        Key: copyPartKey,
                    }));
                });

                it('should retrieve a part that overwrote another part ' +
                'originally copied from an MPU', done =>
                    checkGetObjectPart(copyPartKey, 1, partSize, partOneBody,
                        done));

                it('should retrieve a part copied from an MPU after the ' +
                'original part was overwritten',
                    done => checkGetObjectPart(copyPartKey, 2, partTwoSize,
                        partTwoBody, done));
            });
        });

        describe('absent x-amz-website-redirect-location header', () => {
            before(async () => {
                const params = {
                    Bucket: bucketName,
                    Key: objectName,
                };
                await s3.send(new PutObjectCommand(params));
            });
            it('should return website redirect header if specified in ' +
                'objectPUT request', done => {
                s3.send(new GetObjectCommand({ Bucket: bucketName, Key: objectName })).then(res => {
                    assert.strictEqual(res.WebsiteRedirectLocation,
                        undefined);
                    return done();
                }).catch(done);
            });
        });
    });
});


describe('GET object with object lock', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const bucket = 'bucket-with-lock';
        const key = 'object-with-lock';
        const formatDate = date => date.toString().slice(0, 20);
        const mockDate = moment().add(1, 'days');
        const mockMode = 'GOVERNANCE';
        let versionId;

        beforeEach(() => {
            const params = {
                Bucket: bucket,
                Key: key,
                ObjectLockRetainUntilDate: mockDate.toDate(),
                ObjectLockMode: mockMode,
                ObjectLockLegalHoldStatus: 'ON',
            };
            return s3.send(new CreateBucketCommand({
                Bucket: bucket,
                ObjectLockEnabledForBucket: true,
            }))
            .then(() => s3.send(new PutObjectCommand(params)))
            .then(() => s3.send(new GetObjectCommand({ Bucket: bucket, Key: key })))
            /* eslint-disable no-return-assign */
            .then(res => versionId = res.VersionId)
            .catch(err => {
                process.stdout.write('Error in before\n');
                throw err;
            });
        });

        afterEach(() => changeLockPromise([{ bucket, key, versionId }], '')
            .then(() => s3.send(new ListObjectVersionsCommand({ Bucket: bucket })))
            .then(res => res.Versions?.forEach(object => {
                const params = [
                    {
                        bucket,
                        key: object.Key,
                        versionId: object.VersionId,
                    },
                ];
                changeLockPromise(params, '');
            }))
            .then(() => {
                process.stdout.write('Emptying and deleting buckets\n');
                return bucketUtil.empty(bucket);
            })
            .then(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })))
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            }));

        it('should return object lock headers if set on the object', done => {
            s3.send(new GetObjectCommand({ Bucket: bucket, Key: key })).then(res => {
                assert.strictEqual(res.ObjectLockMode, mockMode);
                const responseDate
                    = formatDate(res.ObjectLockRetainUntilDate);
                const expectedDate = formatDate(mockDate);
                assert.strictEqual(responseDate, expectedDate);
                assert.strictEqual(res.ObjectLockLegalHoldStatus, 'ON');
                const objectWithLock = [
                    {
                        bucket,
                        key,
                        versionId: res.VersionId,
                    },
                ];
                changeObjectLock(objectWithLock, '', done);
            }).catch(done);
        });
    });
});

describe('GET object checksum mode', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        const checksumBucket = 'checksum-getobject-test';
        const checksumKey = 'checksum-test-object';
        const body = Buffer.from('checksum test body');

        const expectedDigests = {};

        const checksumAlgorithms = [
            { algorithm: 'SHA256',    responseField: 'ChecksumSHA256',    internalName: 'sha256'    },
            { algorithm: 'SHA1',      responseField: 'ChecksumSHA1',      internalName: 'sha1'      },
            { algorithm: 'CRC32',     responseField: 'ChecksumCRC32',     internalName: 'crc32'     },
            { algorithm: 'CRC32C',    responseField: 'ChecksumCRC32C',    internalName: 'crc32c'    },
            { algorithm: 'CRC64NVME', responseField: 'ChecksumCRC64NVME', internalName: 'crc64nvme' },
        ];

        before(async () => {
            // Disable automatic response checksum validation so the SDK does
            // not silently add x-amz-checksum-mode: ENABLED to every GetObject
            // request, which would interfere with the "mode not set" test.
            bucketUtil = new BucketUtility('default',
                { ...sigCfg, responseChecksumValidation: 'WHEN_REQUIRED' });
            s3 = bucketUtil.s3;
            await s3.send(new CreateBucketCommand({ Bucket: checksumBucket }));

            for (const { internalName } of checksumAlgorithms) {
                expectedDigests[internalName] =
                    await algorithms[internalName].digest(body);
            }
        });

        after(async () => {
            await bucketUtil.empty(checksumBucket);
            await s3.send(new DeleteBucketCommand({ Bucket: checksumBucket }));
        });

        checksumAlgorithms.forEach(({ algorithm, responseField, internalName }) => {
            it(`should return ${responseField} and ChecksumType when ChecksumMode is ENABLED`, async () => {
                const putRes = await s3.send(new PutObjectCommand({
                    Bucket: checksumBucket,
                    Key: checksumKey,
                    Body: body,
                    ChecksumAlgorithm: algorithm,
                }));
                const storedChecksum = putRes[responseField];
                assert(storedChecksum, `Expected ${responseField} in PutObject response`);

                const getRes = await s3.send(new GetObjectCommand({
                    Bucket: checksumBucket,
                    Key: checksumKey,
                    ChecksumMode: 'ENABLED',
                }));
                assert.strictEqual(getRes[responseField], expectedDigests[internalName],
                    `${responseField} value mismatch`);
                assert.strictEqual(getRes[responseField], storedChecksum);
                assert.strictEqual(getRes.ChecksumType, 'FULL_OBJECT');
            });
        });

        it('should not return checksum headers when ChecksumMode is not set', async () => {
            await s3.send(new PutObjectCommand({
                Bucket: checksumBucket,
                Key: checksumKey,
                Body: body,
                ChecksumAlgorithm: 'SHA256',
            }));

            const getRes = await s3.send(new GetObjectCommand({
                Bucket: checksumBucket,
                Key: checksumKey,
            }));
            assert.strictEqual(getRes.ChecksumSHA256, undefined);
            assert.strictEqual(getRes.ChecksumType, undefined);
        });

        it('should return an error when ChecksumMode is not ENABLED', async () => {
            await s3.send(new PutObjectCommand({
                Bucket: checksumBucket,
                Key: checksumKey,
                Body: body,
            }));

            await assert.rejects(
                s3.send(new GetObjectCommand({
                    Bucket: checksumBucket,
                    Key: checksumKey,
                    ChecksumMode: 'DISABLED',
                })),
                err => {
                    assert.strictEqual(err.name, 'InvalidArgument');
                    return true;
                },
            );
        });
    });
});
