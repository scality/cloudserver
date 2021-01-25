const assert = require('assert');
const async = require('async');
const { errors } = require('arsenal');
const moment = require('moment');
const Promise = require('bluebird');

const changeObjectLock = require('../../../../utilities/objectLock-util');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const changeLockPromise = Promise.promisify(changeObjectLock);

const bucketName = 'alexbucketnottaken';
const objectName = 'someObject';
const partSize = 1024 * 1024 * 5; // 5MB minumum required part size.

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

function checkError(err, code) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.code, code);
}

function dateFromNow(diff) {
    const d = new Date();
    d.setHours(d.getHours() + diff);
    return d.toISOString();
}

function dateConvert(d) {
    return (new Date(d)).toISOString();
}

describe('HEAD object, conditions', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let etag;
        let etagTrim;
        let lastModified;

        before(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return bucketUtil.empty(bucketName).then(() =>
                bucketUtil.deleteOne(bucketName)
            )
            .catch(err => {
                if (err.code !== 'NoSuchBucket') {
                    process.stdout.write(`${err}\n`);
                    throw err;
                }
            })
            .then(() => bucketUtil.createOne(bucketName));
        });

        function requestHead(fields, cb) {
            s3.headObject(Object.assign({
                Bucket: bucketName,
                Key: objectName,
            }, fields), cb);
        }

        beforeEach(() => s3.putObject({
            Bucket: bucketName,
            Key: objectName,
            Body: 'I am the best content ever',
        }).promise().then(res => {
            etag = res.ETag;
            etagTrim = etag.substring(1, etag.length - 1);
            return s3.headObject(
                { Bucket: bucketName, Key: objectName }).promise();
        }).then(res => {
            lastModified = res.LastModified;
        }));

        afterEach(() => bucketUtil.empty(bucketName));

        after(() => bucketUtil.deleteOne(bucketName));

        it('If-Match: returns no error when ETag match, with double quotes ' +
            'around ETag',
            done => {
                requestHead({ IfMatch: etag }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Match: returns no error when one of ETags match, with double ' +
            'quotes around ETag',
            done => {
                requestHead({ IfMatch: `non-matching,${etag}` }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Match: returns no error when ETag match, without double ' +
            'quotes around ETag',
            done => {
                requestHead({ IfMatch: etagTrim }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Match: returns no error when one of ETags match, without ' +
            'double quotes around ETag',
            done => {
                requestHead({ IfMatch: `non-matching,${etagTrim}` }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Match: returns no error when ETag match with *', done => {
            requestHead({ IfMatch: '*' }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-Match: returns PreconditionFailed when ETag does not match',
            done => {
                requestHead({ IfMatch: 'non-matching ETag' }, err => {
                    checkError(err, errors.PreconditionFailed.code);
                    done();
                });
            });

        it('If-None-Match: returns no error when ETag does not match', done => {
            requestHead({ IfNoneMatch: 'non-matching' }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-None-Match: returns no error when all ETags do not match',
            done => {
                requestHead({
                    IfNoneMatch: 'non-matching,non-matching-either',
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-None-Match: returns NotModified when ETag match, with double ' +
            'quotes around ETag',
            done => {
                requestHead({ IfNoneMatch: etag }, err => {
                    checkError(err, 'NotModified');
                    done();
                });
            });

        it('If-None-Match: returns NotModified when one of ETags match, with ' +
            'double quotes around ETag',
            done => {
                requestHead({
                    IfNoneMatch: `non-matching,${etag}`,
                }, err => {
                    checkError(err, 'NotModified');
                    done();
                });
            });

        it('If-None-Match: returns NotModified when ETag match, without ' +
            'double quotes around ETag',
            done => {
                requestHead({ IfNoneMatch: etagTrim }, err => {
                    checkError(err, 'NotModified');
                    done();
                });
            });

        it('If-None-Match: returns NotModified when one of ETags match, ' +
            'without double quotes around ETag',
            done => {
                requestHead({
                    IfNoneMatch: `non-matching,${etagTrim}`,
                }, err => {
                    checkError(err, 'NotModified');
                    done();
                });
            });

        it('If-Modified-Since: returns no error if Last modified date is ' +
            'greater',
            done => {
                requestHead({ IfModifiedSince: dateFromNow(-1) },
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
                requestHead({ IfModifiedSince: dateFromNow(1) },
                    err => {
                        checkError(err, 'NotModified');
                        done();
                    });
            });

        it('If-Modified-Since: returns NotModified if Last modified ' +
            'date is equal',
            done => {
                requestHead({ IfModifiedSince: dateConvert(lastModified) },
                    err => {
                        checkError(err, 'NotModified');
                        done();
                    });
            });

        it('If-Unmodified-Since: returns no error when lastModified date is ' +
            'greater',
            done => {
                requestHead({ IfUnmodifiedSince: dateFromNow(1) }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Unmodified-Since: returns no error when lastModified ' +
            'date is equal',
            done => {
                requestHead({ IfUnmodifiedSince: dateConvert(lastModified) },
                    err => {
                        checkNoError(err);
                        done();
                    });
            });

        it('If-Unmodified-Since: returns PreconditionFailed when ' +
            'lastModified date is lesser',
            done => {
                requestHead({ IfUnmodifiedSince: dateFromNow(-1) }, err => {
                    checkError(err, errors.PreconditionFailed.code);
                    done();
                });
            });

        it('If-Match & If-Unmodified-Since: returns no error when match Etag ' +
            'and lastModified is greater',
            done => {
                requestHead({
                    IfMatch: etagTrim,
                    IfUnmodifiedSince: dateFromNow(-1),
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Match match & If-Unmodified-Since match', done => {
            requestHead({
                IfMatch: etagTrim,
                IfUnmodifiedSince: dateFromNow(1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-Match not match & If-Unmodified-Since not match', done => {
            requestHead({
                IfMatch: 'non-matching',
                IfUnmodifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, errors.PreconditionFailed.code);
                done();
            });
        });

        it('If-Match not match & If-Unmodified-Since match', done => {
            requestHead({
                IfMatch: 'non-matching',
                IfUnmodifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, errors.PreconditionFailed.code);
                done();
            });
        });

        // Skipping this test, because real AWS does not provide error as
        // expected
        it.skip('If-Match match & If-Modified-Since not match', done => {
            requestHead({
                IfMatch: etagTrim,
                IfModifiedSince: dateFromNow(1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-Match match & If-Modified-Since match', done => {
            requestHead({
                IfMatch: etagTrim,
                IfModifiedSince: dateFromNow(-1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-Match not match & If-Modified-Since not match', done => {
            requestHead({
                IfMatch: 'non-matching',
                IfModifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, errors.PreconditionFailed.code);
                done();
            });
        });

        it('If-Match not match & If-Modified-Since match', done => {
            requestHead({
                IfMatch: 'non-matching',
                IfModifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, errors.PreconditionFailed.code);
                done();
            });
        });

        it('If-None-Match & If-Modified-Since: returns NotModified when Etag ' +
            'does not match and lastModified is greater',
            done => {
                requestHead({
                    IfNoneMatch: etagTrim,
                    IfModifiedSince: dateFromNow(-1),
                }, err => {
                    checkError(err, 'NotModified');
                    done();
                });
            });

        it('If-None-Match not match & If-Modified-Since not match', done => {
            requestHead({
                IfNoneMatch: etagTrim,
                IfModifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'NotModified');
                done();
            });
        });

        it('If-None-Match match & If-Modified-Since match', done => {
            requestHead({
                IfNoneMatch: 'non-matching',
                IfModifiedSince: dateFromNow(-1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        // Skipping this test, because real AWS does not provide error as
        // expected
        it.skip('If-None-Match match & If-Modified-Since not match', done => {
            requestHead({
                IfNoneMatch: 'non-matching',
                IfModifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'NotModified');
                done();
            });
        });

        it('If-None-Match match & If-Unmodified-Since match', done => {
            requestHead({
                IfNoneMatch: 'non-matching',
                IfUnmodifiedSince: dateFromNow(1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-None-Match match & If-Unmodified-Since not match', done => {
            requestHead({
                IfNoneMatch: 'non-matching',
                IfUnmodifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, errors.PreconditionFailed.code);
                done();
            });
        });

        it('If-None-Match not match & If-Unmodified-Since match', done => {
            requestHead({
                IfNoneMatch: etagTrim,
                IfUnmodifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'NotModified');
                done();
            });
        });

        it('If-None-Match not match & If-Unmodified-Since not match', done => {
            requestHead({
                IfNoneMatch: etagTrim,
                IfUnmodifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, errors.PreconditionFailed.code);
                done();
            });
        });

        it('WebsiteRedirectLocation is set & it appears in response', done => {
            const redirBktwBody = {
                Bucket: bucketName,
                Key: 'redir_present',
                WebsiteRedirectLocation: 'http://google.com',
                Body: 'hello',
            };
            const redirBkt = {
                Bucket: bucketName,
                Key: 'redir_present',
            };
            s3.putObject(redirBktwBody, err => {
                checkNoError(err);
                s3.headObject(redirBkt, (err, data) => {
                    checkNoError(err);
                    assert.strictEqual(data.WebsiteRedirectLocation,
                            'http://google.com');
                    return done();
                });
            });
        });

        it('Accept-Ranges header should appear in the response', done => {
            const objectName = 'mock-obj';
            const mockPutObjectParams = {
                Bucket: bucketName,
                Key: objectName,
                Body: 'hello',
            };
            const mockHeadObjectParams = {
                Bucket: bucketName,
                Key: objectName,
            };
            s3.putObject(mockPutObjectParams, err => {
                checkNoError(err);
                s3.headObject(mockHeadObjectParams, (err, data) => {
                    checkNoError(err);
                    assert.strictEqual(data.AcceptRanges, 'bytes');
                    done();
                });
            });
        });

        it('WebsiteRedirectLocation is not set & is absent', done => {
            requestHead({}, (err, data) => {
                checkNoError(err);
                assert.strictEqual('WebsiteRedirectLocation' in data,
                  false, 'WebsiteRedirectLocation header is present.');
                done();
            });
        });

        it('PartNumber is set & PartsCount is absent because object is not ' +
        'multipart', done => {
            requestHead({ PartNumber: 1 }, (err, data) => {
                assert.ifError(err);
                assert.strictEqual('PartsCount' in data, false,
                    'PartsCount header is present.');
                done();
            });
        });

        it('PartNumber is set & PartsCount appears in response for ' +
        'multipart object', done => {
            const mpuKey = 'mpukey';
            async.waterfall([
                next => s3.createMultipartUpload({
                    Bucket: bucketName,
                    Key: mpuKey,
                }, next),
                (data, next) => {
                    const uploadId = data.UploadId;
                    s3.uploadPart({
                        Bucket: bucketName,
                        Key: mpuKey,
                        UploadId: uploadId,
                        PartNumber: 1,
                        Body: Buffer.alloc(partSize).fill('a'),
                    }, (err, data) => next(err, uploadId, data.ETag));
                },
                (uploadId, etagOne, next) => s3.uploadPart({
                    Bucket: bucketName,
                    Key: mpuKey,
                    UploadId: uploadId,
                    PartNumber: 2,
                    Body: Buffer.alloc(partSize).fill('z'),
                }, (err, data) => next(err, uploadId, etagOne, data.ETag)),
                (uploadId, etagOne, etagTwo, next) =>
                s3.completeMultipartUpload({
                    Bucket: bucketName,
                    Key: mpuKey,
                    UploadId: uploadId,
                    MultipartUpload: {
                        Parts: [{
                            PartNumber: 1,
                            ETag: etagOne,
                        }, {
                            PartNumber: 2,
                            ETag: etagTwo,
                        }],
                    },
                }, next),
            ], err => {
                assert.ifError(err);
                s3.headObject({
                    Bucket: bucketName,
                    Key: mpuKey,
                    PartNumber: 1,
                }, (err, data) => {
                    assert.ifError(err);
                    assert.strictEqual(data.PartsCount, 2);
                    done();
                });
            });
        });
    });
});

describe('HEAD object with object lock', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const bucket = 'bucket-with-lock';
        const key = 'object-with-lock';
        const formatDate = date => date.toString().slice(0, 20);
        const mockDate = moment().add(1, 'days').toISOString();
        const mockMode = 'GOVERNANCE';
        let versionId;

        beforeEach(() => {
            const params = {
                Bucket: bucket,
                Key: key,
                ObjectLockRetainUntilDate: mockDate,
                ObjectLockMode: mockMode,
                ObjectLockLegalHoldStatus: 'ON',
            };
            return s3.createBucket(
                { Bucket: bucket, ObjectLockEnabledForBucket: true }).promise()
            .then(() => s3.putObject(params).promise())
            .then(() => s3.getObject({ Bucket: bucket, Key: key }).promise())
            /* eslint-disable no-return-assign */
            .then(res => versionId = res.VersionId)
            .catch(err => {
                process.stdout.write('Error in before\n');
                throw err;
            });
        });

        afterEach(() => changeLockPromise([{ bucket, key, versionId }], '')
            .then(() => s3.listObjectVersions({ Bucket: bucket }).promise())
            .then(res => res.Versions.forEach(object => {
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
            .then(() => s3.deleteBucket({ Bucket: bucket }).promise())
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            }));

        it('should return object lock headers if set on the object', done => {
            s3.headObject({ Bucket: bucket, Key: key }, (err, res) => {
                assert.ifError(err);
                assert.strictEqual(res.ObjectLockMode, mockMode);
                const responseDate
                    = formatDate(res.ObjectLockRetainUntilDate.toISOString());
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
            });
        });
    });
});
