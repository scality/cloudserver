const assert = require('assert');
const { errors } = require('arsenal');


const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucketName = 'alexbucketnottaken';
const objectName = 'someObject';

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

        beforeEach(() => s3.putObjectAsync({
            Bucket: bucketName,
            Key: objectName,
            Body: 'I am the best content ever',
        }).then(res => {
            etag = res.ETag;
            etagTrim = etag.substring(1, etag.length - 1);
            return s3.headObjectAsync({ Bucket: bucketName, Key: objectName });
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
    });
});
