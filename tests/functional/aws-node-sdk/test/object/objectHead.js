const { promisify } = require('util');
const assert = require('assert');
const async = require('async');
const { errorInstances } = require('arsenal');
const moment = require('moment');
const {
    HeadObjectCommand,
    PutObjectCommand,
    CreateBucketCommand,
    DeleteBucketCommand,
    GetObjectCommand,
    ListObjectVersionsCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
} = require('@aws-sdk/client-s3');
// In older versions of @aws-sdk/middleware-flexible-checksums (<3.972), the
// CRC64NVME implementation must be patched in manually from the CRT package.
// Newer versions handle this internally via @aws-sdk/crc64-nvme, so the export
// no longer exists and no registration is needed.
const { crc64NvmeCrtContainer } = require('@aws-sdk/middleware-flexible-checksums');
if (crc64NvmeCrtContainer) {
    const { CrtCrc64Nvme } = require('@aws-sdk/crc64-nvme-crt');
    crc64NvmeCrtContainer.CrtCrc64Nvme = CrtCrc64Nvme;
}

const { algorithms } = require('../../../../../lib/api/apiUtils/integrity/validateChecksums');
const changeObjectLock = require('../../../../utilities/objectLock-util');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const checkError = require('../../lib/utility/checkError');

const changeLockPromise = promisify(changeObjectLock);

const bucketName = 'alexbucketnottaken';
const objectName = 'someObject';
const partSize = 1024 * 1024 * 5; // 5MB minumum required part size.

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

function dateFromNow(diff) {
    const d = new Date();
    d.setHours(d.getHours() + diff);
    return d;
}

function dateConvert(d) {
    return new Date(d);
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
                if (err.name !== 'NoSuchBucket') {
                    process.stdout.write(`${err}\n`);
                    throw err;
                }
            })
            .then(() => bucketUtil.createOne(bucketName));
        });

        function requestHead(fields, cb) {
            s3.send(new HeadObjectCommand(Object.assign({
                Bucket: bucketName,
                Key: objectName,
            }, fields))).then(res => cb(null, res)).catch(cb);
        }

        beforeEach(() => s3.send(new PutObjectCommand({
            Bucket: bucketName,
            Key: objectName,
            Body: 'I am the best content ever',
        }))
        .then(res => {
            etag = res.ETag;
            etagTrim = etag.substring(1, etag.length - 1);
            return s3.send(new HeadObjectCommand(
                { Bucket: bucketName, Key: objectName }));
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
                    assert.equal(err.$metadata.httpStatusCode, 412);
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
                    assert.equal(err.$metadata.httpStatusCode, 304);
                    done();
                });
            });

        it('If-None-Match: returns NotModified when one of ETags match, with ' +
            'double quotes around ETag',
            done => {
                requestHead({
                    IfNoneMatch: `non-matching,${etag}`,
                }, err => {
                    assert.equal(err.$metadata.httpStatusCode, 304);
                    done();
                });
            });

        it('If-None-Match: returns NotModified when ETag match, without ' +
            'double quotes around ETag',
            done => {
                requestHead({ IfNoneMatch: etagTrim }, err => {
                    assert.equal(err.$metadata.httpStatusCode, 304);
                    done();
                });
            });

        it('If-None-Match: returns NotModified when one of ETags match, ' +
            'without double quotes around ETag',
            done => {
                requestHead({
                    IfNoneMatch: `non-matching,${etagTrim}`,
                }, err => {
                    assert.equal(err.$metadata.httpStatusCode, 304);
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
                        checkError(err, errorInstances.NotModified.code);
                        done();
                    });
            });

        it('If-Modified-Since: returns NotModified if Last modified ' +
            'date is equal',
            done => {
                requestHead({ IfModifiedSince: dateConvert(lastModified) },
                    err => {
                        assert.equal(err.$metadata.httpStatusCode, 304);
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
                    assert.equal(err.$metadata.httpStatusCode, 412);
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
                assert.equal(err.$metadata.httpStatusCode, 412);
                done();
            });
        });

        it('If-Match not match & If-Unmodified-Since match', done => {
            requestHead({
                IfMatch: 'non-matching',
                IfUnmodifiedSince: dateFromNow(1),
            }, err => {
                assert.equal(err.$metadata.httpStatusCode, 412);
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
                assert.equal(err.$metadata.httpStatusCode, 412);
                done();
            });
        });

        it('If-Match not match & If-Modified-Since match', done => {
            requestHead({
                IfMatch: 'non-matching',
                IfModifiedSince: dateFromNow(-1),
            }, err => {
                assert.equal(err.$metadata.httpStatusCode, 412);
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
                    assert.equal(err.$metadata.httpStatusCode, 304);
                    done();
                });
            });

        it('If-None-Match not match & If-Modified-Since not match', done => {
            requestHead({
                IfNoneMatch: etagTrim,
                IfModifiedSince: dateFromNow(1),
            }, err => {
                assert.equal(err.$metadata.httpStatusCode, 304);
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
                assert.equal(err.$metadata.httpStatusCode, 304);
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
                assert.equal(err.$metadata.httpStatusCode, 412);
                done();
            });
        });

        it('If-None-Match not match & If-Unmodified-Since match', done => {
            requestHead({
                IfNoneMatch: etagTrim,
                IfUnmodifiedSince: dateFromNow(1),
            }, err => {
                assert.equal(err.$metadata.httpStatusCode, 304);
                done();
            });
        });

        it('If-None-Match not match & If-Unmodified-Since not match', done => {
            requestHead({
                IfNoneMatch: etagTrim,
                IfUnmodifiedSince: dateFromNow(-1),
            }, err => {
                assert.equal(err.$metadata.httpStatusCode, 412);
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
            s3.send(new PutObjectCommand(redirBktwBody)).then(() => {
                s3.send(new HeadObjectCommand(redirBkt)).then(data => {
                    assert.strictEqual(data.WebsiteRedirectLocation,
                            'http://google.com');
                    done();
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
            s3.send(new PutObjectCommand(mockPutObjectParams)).then(() => {
                s3.send(new HeadObjectCommand(mockHeadObjectParams)).then(data => {
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
                next => s3.send(new CreateMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: mpuKey,
                })).then(data => next(null, data)).catch(next),
                (data, next) => {
                    const uploadId = data.UploadId;
                    s3.send(new UploadPartCommand({
                        Bucket: bucketName,
                        Key: mpuKey,
                        UploadId: uploadId,
                        PartNumber: 1,
                        Body: Buffer.alloc(partSize).fill('a'),
                    })).then(data => next(null, uploadId, data.ETag)).catch(next);
                },
                (uploadId, etagOne, next) => s3.send(new UploadPartCommand({
                    Bucket: bucketName,
                    Key: mpuKey,
                    UploadId: uploadId,
                    PartNumber: 2,
                    Body: Buffer.alloc(partSize).fill('z'),
                })).then(data => next(null, uploadId, etagOne, data.ETag)).catch(next),
                (uploadId, etagOne, etagTwo, next) =>
                s3.send(new CompleteMultipartUploadCommand({
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
                })).then(data => next(null, data)).catch(next),
            ], err => {
                assert.ifError(err);
                s3.send(new HeadObjectCommand({
                    Bucket: bucketName,
                    Key: mpuKey,
                    PartNumber: 1,
                })).then(data => {
                    assert.strictEqual(data.PartsCount, 2);
                    done();
                });
            });
        });
    });
});

describe('HEAD object checksum mode', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        const checksumBucket = 'checksum-headobject-test';
        const checksumKey = 'checksum-test-object';
        const body = Buffer.from('checksum test body');

        // Expected base64-encoded digests of `body` for each algorithm,
        // computed once in the before hook (crc64nvme digest is async).
        const expectedDigests = {};

        before(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            await s3.send(new CreateBucketCommand({ Bucket: checksumBucket }));

            for (const { internalName } of checksumAlgorithms) {
                // algorithms[internalName].digest() returns a base64 string
                expectedDigests[internalName] =
                    await algorithms[internalName].digest(body);
            }
        });

        after(async () => {
            await bucketUtil.empty(checksumBucket);
            await s3.send(new DeleteBucketCommand({ Bucket: checksumBucket }));
        });

        const checksumAlgorithms = [
            { algorithm: 'SHA256', responseField: 'ChecksumSHA256', internalName: 'sha256' },
            { algorithm: 'SHA1',   responseField: 'ChecksumSHA1',   internalName: 'sha1'   },
            { algorithm: 'CRC32',  responseField: 'ChecksumCRC32',  internalName: 'crc32'  },
            { algorithm: 'CRC32C', responseField: 'ChecksumCRC32C', internalName: 'crc32c' },
            { algorithm: 'CRC64NVME', responseField: 'ChecksumCRC64NVME', internalName: 'crc64nvme' },
        ];

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

                const headRes = await s3.send(new HeadObjectCommand({
                    Bucket: checksumBucket,
                    Key: checksumKey,
                    ChecksumMode: 'ENABLED',
                }));
                assert.strictEqual(headRes[responseField], expectedDigests[internalName],
                    `${responseField} value mismatch`);
                assert.strictEqual(headRes[responseField], storedChecksum);
                assert.strictEqual(headRes.ChecksumType, 'FULL_OBJECT');
            });
        });

        it('should not return checksum headers when ChecksumMode is not set', async () => {
            await s3.send(new PutObjectCommand({
                Bucket: checksumBucket,
                Key: checksumKey,
                Body: body,
                ChecksumAlgorithm: 'SHA256',
            }));

            const headRes = await s3.send(new HeadObjectCommand({
                Bucket: checksumBucket,
                Key: checksumKey,
            }));
            assert.strictEqual(headRes.ChecksumSHA256, undefined);
            assert.strictEqual(headRes.ChecksumType, undefined);
        });

        it('should return an error when ChecksumMode is not ENABLED', async () => {
            await s3.send(new PutObjectCommand({
                Bucket: checksumBucket,
                Key: checksumKey,
                Body: body,
            }));

            await assert.rejects(
                s3.send(new HeadObjectCommand({
                    Bucket: checksumBucket,
                    Key: checksumKey,
                    ChecksumMode: 'DISABLED',
                })),
            );
        });
    });
});

const isCEPH = process.env.CI_CEPH !== undefined;
const describeSkipIfCeph = isCEPH ? describe.skip : describe;

describeSkipIfCeph('HEAD object with object lock', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const bucket = 'bucket-with-lock';
        const key = 'object-with-lock';
        const formatDate = date => moment(date).format('YYYY-MM-DDTHH:mm:ss.SSS[Z]');
        const mockDate = moment().add(1, 'days');
        const mockMode = 'GOVERNANCE';
        let versionId;

        beforeEach(async () => {
            const params = {
                Bucket: bucket,
                Key: key,
                ObjectLockRetainUntilDate: mockDate.toDate(),
                ObjectLockMode: mockMode,
                ObjectLockLegalHoldStatus: 'ON',
            };
            await s3.send(new CreateBucketCommand({
                Bucket: bucket,
                ObjectLockEnabledForBucket: true,
            }));
            await s3.send(new PutObjectCommand(params));
            const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
             
            versionId = res.VersionId;
        });

        afterEach(async () => {
            await changeLockPromise([{ bucket, key, versionId }], '');
            const res = await s3.send(new ListObjectVersionsCommand({ Bucket: bucket }));
            res.Versions?.forEach(object => {
                const params = [
                    {
                        bucket,
                        key: object.Key,
                        versionId: object.VersionId,
                    },
                ];
                changeLockPromise(params, '');
            });
            await bucketUtil.empty(bucket);
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        it('should return object lock headers if set on the object', async () => {
            const res = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
            assert.strictEqual(res.ObjectLockMode, mockMode);
            const responseDate= formatDate(res.ObjectLockRetainUntilDate);
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
            await changeLockPromise(objectWithLock, '');
        });
    });
});
