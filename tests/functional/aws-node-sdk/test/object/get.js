const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucketName = 'buckettestgetobject';
const objectName = 'someObject';
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

        before(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            // Create a bucket to put object to get later
            s3.createBucket({ Bucket: bucketName }, done);
        });

        after(done => {
            s3.deleteObject({ Bucket: bucketName, Key: objectName }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucketName }, done);
            });
        });


        it('should return an error to get request without a valid bucket name',
            done => {
                s3.getObject({ Bucket: '', Key: 'somekey' }, err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 'MethodNotAllowed');
                    return done();
                });
            });

        it('should return NoSuchKey error when no such object',
            done => {
                s3.getObject({ Bucket: bucketName, Key: 'nope' }, err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 'NoSuchKey');
                    return done();
                });
            });

        describe('Additional headers: [Cache-Control, Content-Disposition, ' +
            'Content-Encoding, Expires]', () => {
            describe('if specified in put object request', () => {
                before(done => {
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
                it('should return additional headers', done => {
                    s3.getObject({ Bucket: bucketName, Key: objectName },
                      (err, res) => {
                          if (err) {
                              return done(err);
                          }
                          assert.strictEqual(res.CacheControl,
                            cacheControl);
                          assert.strictEqual(res.ContentDisposition,
                            contentDisposition);
                          // Should remove V4 streaming value 'aws-chunked'
                          // to be compatible with AWS behavior
                          assert.strictEqual(res.ContentEncoding,
                            'gzip');
                          assert.strictEqual(res.ContentType, contentType);
                          assert.strictEqual(res.Expires,
                              new Date(expires).toGMTString());
                          return done();
                      });
                });
            });

            describe('if response content headers are set in query', () => {
                before(done => {
                    s3.putObject({ Bucket: bucketName, Key: objectName },
                        err => done(err));
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
                    s3.getObject(params, (err, res) => {
                        if (err) {
                            return done(err);
                        }
                        assert.strictEqual(res.CacheControl,
                          cacheControl);
                        assert.strictEqual(res.ContentDisposition,
                          contentDisposition);
                        assert.strictEqual(res.ContentEncoding,
                          contentEncoding);
                        assert.strictEqual(res.ContentLanguage,
                            contentLanguage);
                        assert.strictEqual(res.ContentType, contentType);
                        assert.strictEqual(res.Expires, expires);
                        return done();
                    });
                });
            });
        });

        describe('x-amz-website-redirect-location header', () => {
            before(done => {
                const params = {
                    Bucket: bucketName,
                    Key: objectName,
                    WebsiteRedirectLocation: '/',
                };
                s3.putObject(params, err => done(err));
            });
            it('should return website redirect header if specified in ' +
                'objectPUT request', done => {
                s3.getObject({ Bucket: bucketName, Key: objectName },
                  (err, res) => {
                      if (err) {
                          return done(err);
                      }
                      assert.strictEqual(res.WebsiteRedirectLocation, '/');
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

            it('should not return "x-amz-tagging-count" if no tag ' +
            'associated with the object',
            done => {
                s3.getObject(params, (err, data) => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(data.TagCount, undefined);
                    return done();
                });
            });

            describe('tag associated with the object', () => {
                beforeEach(done => {
                    s3.putObjectTagging(paramsTagging, done);
                });
                it('should return "x-amz-tagging-count" header that provides ' +
                'the count of number of tags associated with the object',
                done => {
                    s3.getObject(params, (err, data) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(data.TagCount, 1);
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
                        IfModifiedSince: dateFromNow(-1),
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
    });
});
