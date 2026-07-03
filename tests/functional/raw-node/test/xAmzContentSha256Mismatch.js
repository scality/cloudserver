const assert = require('assert');
const crypto = require('crypto');
const async = require('async');

const { makeS3Request } = require('../utils/makeRequest');
const HttpRequestAuthV4 = require('../utils/HttpRequestAuthV4');

const config = require('../../config.json');
const { checksumedMethods } = require('../../../../lib/api/apiUtils/integrity/validateChecksums');

// Regression test for S3C-10916: "[SigV4] x-amz-content-sha256 value not checked".
//
// CloudServer used to trust x-amz-content-sha256 without recomputing the body's
// SHA256, accepting a request signed with a wrong-but-well-formed hash. The fix
// verifies the header against the body on the buffered and streaming paths. These
// tests assert the AWS-correct 400 XAmzContentSHA256Mismatch, passing against real
// AWS (AWS_ON_AIR=true) and CloudServer with the fix.

const bucket = 'contentsha256mismatchbucket';
const objectKey = 'key';

const authCredentials = {
    accessKey: config.accessKey,
    secretKey: config.secretKey,
};

const host = process.env.AWS_ON_AIR ? 's3.amazonaws.com' : '127.0.0.1';
const port = process.env.AWS_ON_AIR ? 80 : 8000;
const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

const objData = Buffer.from('the real request body content');
const realSha256Hex = crypto.createHash('sha256').update(objData).digest('hex');
const emptyBody = Buffer.alloc(0);
const emptySha256Hex = crypto.createHash('sha256').update(emptyBody).digest('hex');
const wrongSha256Hex = crypto.createHash('sha256').update('completely different content').digest('hex');
const invalidSha256 = 'xxx';

// An arbitrary body that is never parsed: the x-amz-content-sha256 check rejects
// the request before the handler reads it, so its content is irrelevant.
const fakeBody = Buffer.from('not parsed before the content-sha256 check');

const bufferedEndpoints = {
    multiObjectDelete: { method: 'POST', suffix: '?delete' },
    bucketPut: { method: 'PUT', suffix: '' }, // CreateBucket (no subresource)
    bucketPutACL: { method: 'PUT', suffix: '?acl' },
    bucketPutCors: { method: 'PUT', suffix: '?cors' },
    bucketPutEncryption: { method: 'PUT', suffix: '?encryption' },
    bucketPutLifecycle: { method: 'PUT', suffix: '?lifecycle' },
    bucketPutLogging: { method: 'PUT', suffix: '?logging' },
    bucketPutNotification: { method: 'PUT', suffix: '?notification' },
    bucketPutPolicy: { method: 'PUT', suffix: '?policy' },
    bucketPutReplication: { method: 'PUT', suffix: '?replication' },
    bucketPutTagging: { method: 'PUT', suffix: '?tagging' },
    bucketPutVersioning: { method: 'PUT', suffix: '?versioning' },
    bucketPutWebsite: { method: 'PUT', suffix: '?website' },
    bucketPutObjectLock: { method: 'PUT', suffix: '?object-lock' },
    objectPutACL: { method: 'PUT', suffix: `/${objectKey}?acl` },
    objectPutLegalHold: { method: 'PUT', suffix: `/${objectKey}?legal-hold` },
    objectPutRetention: { method: 'PUT', suffix: `/${objectKey}?retention` },
    objectPutTagging: { method: 'PUT', suffix: `/${objectKey}?tagging` },
    objectRestore: { method: 'POST', suffix: `/${objectKey}?restore` },
    completeMultipartUpload: { method: 'POST', suffix: `/${objectKey}?uploadId=fakeUploadId` },
};

const scalityExtensionEndpoints = {
    bucketUpdateQuota: { method: 'PUT', suffix: '?quota' },
    bucketPutRateLimit: { method: 'PUT', suffix: '?rate-limit' },
};

function doRequest(method, url, headers, body, callback) {
    const req = new HttpRequestAuthV4(url, Object.assign({ method, headers }, authCredentials), res => {
        let data = '';
        res.on('data', chunk => {
            data += chunk;
        });
        res.on('end', () =>
            callback(null, {
                statusCode: res.statusCode,
                body: data,
                headers: res.headers,
            }),
        );
    });
    req.on('error', callback);
    req.write(body);
    req.end();
}

const doPutRequest = (url, headers, body, callback) => doRequest('PUT', url, headers, body, callback);

function makeMismatchTests(urlFn, body = objData, correctHex = realSha256Hex) {
    it('should reject a body whose x-amz-content-sha256 does not match with 400 XAmzContentSHA256Mismatch', done => {
        doPutRequest(
            urlFn(),
            {
                'x-amz-content-sha256': wrongSha256Hex,
                'content-length': body.length,
            },
            body,
            (err, res) => {
                assert.ifError(err);
                assert.strictEqual(res.statusCode, 400, `expected 400, got ${res.statusCode}: ${res.body}`);
                assert.match(
                    res.body,
                    /XAmzContentSHA256Mismatch/,
                    `expected XAmzContentSHA256Mismatch in "${res.body}"`,
                );
                done();
            },
        );
    });

    it('should accept a body whose x-amz-content-sha256 matches', done => {
        doPutRequest(
            urlFn(),
            {
                'x-amz-content-sha256': correctHex,
                'content-length': body.length,
            },
            body,
            (err, res) => {
                assert.ifError(err);
                assert.strictEqual(res.statusCode, 200, `expected 200, got ${res.statusCode}: ${res.body}`);
                done();
            },
        );
    });

    it('should reject an invalid x-amz-content-sha256 value with 400 InvalidArgument', done => {
        doPutRequest(
            urlFn(),
            {
                'x-amz-content-sha256': invalidSha256,
                'content-length': body.length,
            },
            body,
            (err, res) => {
                assert.ifError(err);
                assert.strictEqual(res.statusCode, 400, `expected 400, got ${res.statusCode}: ${res.body}`);
                assert.match(res.body, /InvalidArgument/, `expected InvalidArgument in "${res.body}"`);
                done();
            },
        );
    });
}

describe('SigV4 x-amz-content-sha256 body checksum validation (S3C-10916)', () => {
    describe('PutObject', () => {
        before(done => {
            makeS3Request({ method: 'PUT', authCredentials, bucket }, err => {
                assert.ifError(err);
                done();
            });
        });

        after(done => {
            makeS3Request({ method: 'DELETE', authCredentials, bucket, objectKey }, () => {
                makeS3Request({ method: 'DELETE', authCredentials, bucket }, err => {
                    assert.ifError(err);
                    done();
                });
            });
        });

        makeMismatchTests(() => `http://${host}:${port}/${bucket}/${objectKey}`);

        describe('with an empty body (zero-byte path)', () => {
            makeMismatchTests(() => `http://${host}:${port}/${bucket}/${objectKey}`, emptyBody, emptySha256Hex);
        });
    });

    describe('UploadPart', () => {
        let uploadId;

        before(done => {
            async.series(
                [
                    next => makeS3Request({ method: 'PUT', authCredentials, bucket }, next),
                    next =>
                        makeS3Request(
                            {
                                method: 'POST',
                                authCredentials,
                                bucket,
                                objectKey,
                                queryObj: { uploads: '' },
                            },
                            (err, res) => {
                                if (err) {
                                    return next(err);
                                }
                                const match = res.body.match(/<UploadId>([^<]+)<\/UploadId>/);
                                assert(match, `missing UploadId in response: ${res.body}`);
                                uploadId = match[1];
                                return next();
                            },
                        ),
                ],
                err => {
                    assert.ifError(err);
                    done();
                },
            );
        });

        after(done => {
            async.series(
                [
                    next =>
                        makeS3Request(
                            {
                                method: 'DELETE',
                                authCredentials,
                                bucket,
                                objectKey,
                                queryObj: { uploadId },
                            },
                            next,
                        ),
                    // Delete the object key first (defensive: clears any state left by a previous run).
                    next => makeS3Request({ method: 'DELETE', authCredentials, bucket, objectKey }, () => next()),
                    next => makeS3Request({ method: 'DELETE', authCredentials, bucket }, next),
                ],
                err => {
                    assert.ifError(err);
                    done();
                },
            );
        });

        makeMismatchTests(() => `http://${host}:${port}/${bucket}/${objectKey}?partNumber=1&uploadId=${uploadId}`);

        describe('with an empty body (zero-byte path)', () => {
            makeMismatchTests(
                () => `http://${host}:${port}/${bucket}/${objectKey}?partNumber=1&uploadId=${uploadId}`,
                emptyBody,
                emptySha256Hex,
            );
        });
    });

    // Non-streaming (buffered) write path: validation happens in
    // validateMethodChecksumNoChunking, before the handler reads the body. A
    // mismatched hash must therefore be rejected on every concerned endpoint.
    describe('buffered endpoints', () => {
        before(done => {
            makeS3Request({ method: 'PUT', authCredentials, bucket }, err => {
                assert.ifError(err);
                done();
            });
        });

        after(done => {
            makeS3Request({ method: 'DELETE', authCredentials, bucket }, err => {
                assert.ifError(err);
                done();
            });
        });

        // Regression sweep: a wrong-but-well-formed hash is rejected everywhere.
        Object.entries(bufferedEndpoints).forEach(([apiMethod, ep]) => {
            it(`should return 400 XAmzContentSHA256Mismatch for ${apiMethod}`, done => {
                doRequest(
                    ep.method,
                    `http://${host}:${port}/${bucket}${ep.suffix}`,
                    {
                        'x-amz-content-sha256': wrongSha256Hex,
                        'content-length': fakeBody.length,
                    },
                    fakeBody,
                    (err, res) => {
                        assert.ifError(err);
                        assert.strictEqual(res.statusCode, 400, `expected 400, got ${res.statusCode}: ${res.body}`);
                        assert.match(
                            res.body,
                            /XAmzContentSHA256Mismatch/,
                            `expected XAmzContentSHA256Mismatch in "${res.body}"`,
                        );
                        done();
                    },
                );
            });
        });

        it('should return 400 XAmzContentSHA256Mismatch for a zero-byte buffered body', done => {
            const ep = bufferedEndpoints.bucketPutCors;
            doRequest(
                ep.method,
                `http://${host}:${port}/${bucket}${ep.suffix}`,
                {
                    'x-amz-content-sha256': wrongSha256Hex,
                    'content-length': emptyBody.length,
                },
                emptyBody,
                (err, res) => {
                    assert.ifError(err);
                    assert.strictEqual(res.statusCode, 400, `expected 400, got ${res.statusCode}: ${res.body}`);
                    assert.match(
                        res.body,
                        /XAmzContentSHA256Mismatch/,
                        `expected XAmzContentSHA256Mismatch in "${res.body}"`,
                    );
                    done();
                },
            );
        });

        // Fails if a new checksumed/buffered method is added without test coverage.
        it('should exercise every buffered checksumed method', () => {
            const expected = new Set([...Object.keys(checksumedMethods), 'completeMultipartUpload']);
            const covered = new Set(Object.keys(bufferedEndpoints));
            expected.forEach(method => assert(covered.has(method), `missing buffered-endpoint coverage for ${method}`));
        });
    });

    // Scality-only admin extensions: same choke point, but absent from AWS.
    describe('buffered Scality extensions (skipped on AWS)', () => {
        Object.entries(scalityExtensionEndpoints).forEach(([apiMethod, ep]) => {
            itSkipIfAWS(`should return 400 XAmzContentSHA256Mismatch for ${apiMethod}`, done => {
                doRequest(
                    ep.method,
                    `http://${host}:${port}/${bucket}${ep.suffix}`,
                    {
                        'x-amz-content-sha256': wrongSha256Hex,
                        'content-length': fakeBody.length,
                    },
                    fakeBody,
                    (err, res) => {
                        assert.ifError(err);
                        assert.strictEqual(res.statusCode, 400, `expected 400, got ${res.statusCode}: ${res.body}`);
                        assert.match(
                            res.body,
                            /XAmzContentSHA256Mismatch/,
                            `expected XAmzContentSHA256Mismatch in "${res.body}"`,
                        );
                        done();
                    },
                );
            });
        });
    });
});
