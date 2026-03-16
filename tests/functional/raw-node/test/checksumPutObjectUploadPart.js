const assert = require('assert');
const crypto = require('crypto');
const async = require('async');

const { makeS3Request } = require('../utils/makeRequest');
const HttpRequestAuthV4 = require('../utils/HttpRequestAuthV4');

const bucket = 'checksumrejectionbucket';
const objectKey = 'key';
const objData = Buffer.alloc(1, 'a');
const objDataSha256Hex = crypto.createHash('sha256').update(objData).digest('hex');

const authCredentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

const algos = [
    { name: 'crc32', wrongDigest: 'AAAAAA==' },
    { name: 'crc32c', wrongDigest: 'AAAAAA==' },
    { name: 'crc64nvme', wrongDigest: 'AAAAAAAAAAA=' },
    { name: 'sha1', wrongDigest: 'AAAAAAAAAAAAAAAAAAAAAAAAAAA=' },
    { name: 'sha256', wrongDigest: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' },
];

// Build the raw chunked body for STREAMING-UNSIGNED-PAYLOAD-TRAILER
function buildTrailerBody(algoName, wrongDigest) {
    const chunkSize = objData.length.toString(16);
    return `${chunkSize}\r\n${objData.toString()}\r\n0\r\nx-amz-checksum-${algoName}:${wrongDigest}\r\n`;
}

function doPutRequest(url, headers, body, callback) {
    const req = new HttpRequestAuthV4(
        url,
        Object.assign({ method: 'PUT', headers }, authCredentials),
        res => {
            let data = '';
            res.on('data', chunk => { data += chunk; });
            res.on('end', () => callback(null, { statusCode: res.statusCode, body: data }));
        }
    );
    req.on('error', callback);
    req.write(body);
    req.end();
}

const protocols = [
    {
        name: 'UNSIGNED-PAYLOAD',
        buildHeaders: algo => ({
            'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
            'content-length': objData.length,
            [`x-amz-checksum-${algo.name}`]: algo.wrongDigest,
        }),
        buildBody: () => objData,
    },
    {
        name: 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
        buildHeaders: algo => ({
            // No x-amz-content-sha256: HttpRequestAuthV4 defaults to
            // STREAMING-AWS4-HMAC-SHA256-PAYLOAD and handles chunk signing.
            'content-length': objData.length,
            [`x-amz-checksum-${algo.name}`]: algo.wrongDigest,
        }),
        buildBody: () => objData,
    },
    {
        name: 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
        buildHeaders: algo => {
            const body = buildTrailerBody(algo.name, algo.wrongDigest);
            return {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': `x-amz-checksum-${algo.name}`,
                'x-amz-decoded-content-length': objData.length,
                'content-length': body.length,
            };
        },
        buildBody: algo => buildTrailerBody(algo.name, algo.wrongDigest),
    },
    {
        name: 'valid x-amz-content-sha256',
        buildHeaders: algo => ({
            'x-amz-content-sha256': objDataSha256Hex,
            'content-length': objData.length,
            [`x-amz-checksum-${algo.name}`]: algo.wrongDigest,
        }),
        buildBody: () => objData,
    },
];

function assertBadDigest(err, res, done) {
    assert.ifError(err);
    assert.strictEqual(res.statusCode, 400, `expected 400, got ${res.statusCode}: ${res.body}`);
    assert(res.body.includes('BadDigest'), `missing BadDigest in "${res.body}"`);
    done();
}

// Constants for protocol scenario tests

const trailerContent = Buffer.from('trailer content'); // 15 bytes, hex 'f'
const trailerContentSha256 = '4x74k2oA6j6knXzpDwogNkS6E3MM49tPpJMjfD+ES68=';
// md5("trailer content") in base64 — used in testS3PutTrailerWithContentMD5
const trailerContentMd5B64 = crypto.createHash('md5').update(trailerContent).digest('base64');

const testContent2 = Buffer.from('test content');
const testContent2Sha256Hex = crypto.createHash('sha256').update(testContent2).digest('hex');
const testContent2Sha256B64 = crypto.createHash('sha256').update(testContent2).digest('base64');

// Build a STREAMING-UNSIGNED-PAYLOAD-TRAILER body with "trailer content" as the
// data and a sha256 trailer. Uses the '\n\r\n\r\n\r\n' ending that AWS SDK sends
// (TrailingChecksumTransform strips the trailing \n from the parsed trailer line).
function buildOkTrailerBody() {
    const hexLen = trailerContent.length.toString(16); // 'f'
    return `${hexLen}\r\ntrailer content\r\n0\r\nx-amz-checksum-sha256:${trailerContentSha256}\n\r\n\r\n\r\n`;
}

// Assert that the response has the given HTTP status code and (optionally)
// that the body contains the expected error code string.
// Returns a (err, res, done) callback suitable for use with doPutRequest.
function assertStatus(expectedStatus, expectedCode, expectedMessage) {
    return (err, res, done) => {
        assert.ifError(err);
        assert.strictEqual(res.statusCode, expectedStatus,
            `expected ${expectedStatus}, got ${res.statusCode}: ${res.body}`);
        if (expectedCode) {
            assert(res.body.includes(expectedCode),
                `expected "${expectedCode}" in body: "${res.body}"`);
        }
        if (expectedMessage) {
            assert(res.body.includes(expectedMessage),
                `expected "${expectedMessage}" in body: "${res.body}"`);
        }
        done();
    };
}

const msgMalformedTrailer = 'The request contained trailing data that was not well-formed' +
    ' or did not conform to our published schema.';
const msgSdkMissingTrailer = 'x-amz-sdk-checksum-algorithm specified, but no corresponding' +
    ' x-amz-checksum-* or x-amz-trailer headers were found.';

// Create the 24 common protocol-scenario tests for a given URL factory.
// urlFn() is called lazily at test runtime so that uploadId is available.
function makeScenarioTests(urlFn) {
    itSkipIfAWS(
        'testS3PutNoChecksum: signed sha256 in x-amz-content-sha256, no x-amz-checksum header -> 200 OK',
        done => {
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': testContent2Sha256Hex,
                'content-length': testContent2.length,
            }, testContent2, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutChecksum: correct sha256 checksum with x-amz-sdk-checksum-algorithm -> 200 OK',
        done => {
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': testContent2Sha256Hex,
                'x-amz-sdk-checksum-algorithm': 'SHA256',
                'x-amz-checksum-sha256': testContent2Sha256B64,
                'content-length': testContent2.length,
            }, testContent2, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutChecksumKo: wrong sha256 checksum with x-amz-sdk-checksum-algorithm -> 400 BadDigest',
        done => {
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': testContent2Sha256Hex,
                'x-amz-sdk-checksum-algorithm': 'SHA256',
                'x-amz-checksum-sha256': 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
                'content-length': testContent2.length,
            }, testContent2, (err, res) => assertStatus(400, 'BadDigest',
                'The SHA256 you specified did not match the calculated checksum.')(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutChecksumUnsignedPayload: UNSIGNED-PAYLOAD with correct sha256 checksum -> 200 OK',
        done => {
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
                'x-amz-sdk-checksum-algorithm': 'SHA256',
                'x-amz-checksum-sha256': testContent2Sha256B64,
                'content-length': testContent2.length,
            }, testContent2, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerNoBody: TRAILER with empty body -> 400 IncompleteBody',
        done => {
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': 0,
            }, Buffer.alloc(0), (err, res) => assertStatus(400, 'IncompleteBody',
                'The request body terminated unexpectedly')(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerOk: TRAILER with correct sha256 checksum -> 200 OK',
        done => {
            const body = buildOkTrailerBody();
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerBadDecodedLen: wrong x-amz-decoded-content-length (7 but actual is 32) -> 500 InternalError',
        done => {
            // Two chunks of 16 bytes each with a valid crc64nvme trailer.
            const body =
                '10\r\n0123456789abcdef\r\n' +
                '10\r\n0123456789abcdef\r\n' +
                '0\r\nx-amz-checksum-crc64nvme:skQv82y5rgE=\r\n\r\n\r\n';
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-crc64nvme',
                'x-amz-decoded-content-length': 7, // wrong: actual content is 32 bytes
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(500, 'InternalError',
                'We encountered an internal error. Please try again.')(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerAlgoMismatch: x-amz-trailer says sha1 but body trailer has sha256 ' +
        ' -> 400 MalformedTrailerError',
        done => {
            // Header announces sha1 but the actual trailer line carries sha256.
            const body =
                `f\r\ntrailer content\r\n0\r\nx-amz-checksum-sha256:${trailerContentSha256}\n\r\n\r\n\r\n`;
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha1',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(400, 'MalformedTrailerError',
                msgMalformedTrailer)(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerBadDigest: TRAILER with wrong sha256 checksum -> 400 BadDigest',
        done => {
            const wrongSha256 = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=';
            const body =
                `f\r\ntrailer content\r\n0\r\nx-amz-checksum-sha256:${wrongSha256}\n\r\n\r\n\r\n`;
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(400, 'BadDigest',
                'The SHA256 you specified did not match the calculated checksum.')(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerAndChecksum: x-amz-trailer + x-amz-checksum-crc32 header -> 400 InvalidRequest',
        done => {
            const body = buildOkTrailerBody();
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-checksum-crc32': 'H+Yzmw==', // crc32("trailer content")
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(400, 'InvalidRequest',
                'Expecting a single x-amz-checksum- header')(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerNoTrailerInHeader: no x-amz-trailer header but body has trailer -> 400 MalformedTrailerError',
        done => {
            const body = buildOkTrailerBody();
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                // no x-amz-trailer header
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(400, 'MalformedTrailerError',
                msgMalformedTrailer)(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerContentLength: TRAILER with explicit Content-Length -> 200 OK',
        done => {
            const body = buildOkTrailerBody();
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerSDK: TRAILER with matching x-amz-sdk-checksum-algorithm:SHA256 -> 200 OK',
        done => {
            const body = buildOkTrailerBody();
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-sdk-checksum-algorithm': 'SHA256',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerSDKMismatch: x-amz-sdk-checksum-algorithm:SHA1 but x-amz-trailer is sha256 ' +
        '-> 400 InvalidRequest',
        done => {
            const body = buildOkTrailerBody();
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-sdk-checksum-algorithm': 'SHA1', // mismatch
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(400, 'InvalidRequest',
                'Value for x-amz-sdk-checksum-algorithm header is invalid.')(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerUnknownTrailerAlgo: x-amz-trailer:x-amz-checksum-sha3 (unknown) -> 400 InvalidRequest',
        done => {
            const body = buildOkTrailerBody();
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha3',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(400, 'InvalidRequest',
                'The value specified in the x-amz-trailer header is not supported')(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerInvalidTrailerHeader: x-amz-trailer with non-checksum value -> 400 InvalidRequest',
        done => {
            const body = buildOkTrailerBody();
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'AAAAAAAAAAAAAAAAAAAAA',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(400, 'InvalidRequest',
                'The value specified in the x-amz-trailer header is not supported')(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerInvalidTrailerBody: trailer body has invalid base64 checksum value -> 400 InvalidRequest',
        done => {
            const body = 'f\r\ntrailer content\r\n0\r\nx-amz-checksum-sha256:BAD\n\r\n\r\n\r\n';
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(400, 'InvalidRequest',
                'Value for x-amz-checksum-sha256 trailing header is invalid.')(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerSDKMissingTrailer: x-amz-sdk-checksum-algorithm without x-amz-trailer -> 400 InvalidRequest',
        done => {
            const body = buildOkTrailerBody();
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                // no x-amz-trailer
                'x-amz-sdk-checksum-algorithm': 'SHA256',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(400, 'InvalidRequest',
                msgSdkMissingTrailer)(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerNoTrailerInBody: x-amz-trailer header but body has no trailer -> 400 MalformedTrailerError',
        done => {
            // Body ends with "0\r\n\r\n" — empty trailer section, no checksum line.
            const body = 'f\r\ntrailer content\r\n0\r\n\r\n';
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(400, 'MalformedTrailerError',
                msgMalformedTrailer)(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerNoTrailerInBodyAndHeader: no x-amz-trailer, no body trailer -> 200 OK',
        done => {
            // No x-amz-trailer header; body just has chunked data with no trailer.
            const body = 'f\r\ntrailer content\r\n0\r\n\r\n';
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                // no x-amz-trailer
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerDataAfterCRLF: data after final CRLF is ignored -> 200 OK',
        done => {
            // No x-amz-trailer; after the terminating CRLF there is extra data.
            // TrailingChecksumTransform discards everything after streamClosed=true.
            const body = 'f\r\ntrailer content\r\n0\r\n\r\nRANDOM DATA IGNORED';
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                // no x-amz-trailer
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerWithContentMD5: TRAILER + correct Content-MD5 header -> 200 OK',
        done => {
            const body = buildOkTrailerBody();
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-sdk-checksum-algorithm': 'SHA256',
                'content-md5': trailerContentMd5B64,
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'testS3PutTrailerWithWhitespace: trailer line with whitespace around name and value -> 200 OK',
        done => {
            // TrailingChecksumTransform trims both name and value, so whitespace is accepted.
            const body =
                `f\r\ntrailer content\r\n0\r\n x-amz-checksum-sha256  :    ${trailerContentSha256}  \n\r\n\r\n\r\n`;
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(200)(err, res, done));
        });
}

describe('PutObject: bad checksum is rejected', () => {
    before(done => {
        makeS3Request({ method: 'PUT', authCredentials, bucket }, err => {
            assert.ifError(err);
            done();
        });
    });

    after(done => {
        // Delete the object key first (defensive: clears any state left by a previous run).
        makeS3Request({ method: 'DELETE', authCredentials, bucket, objectKey }, () => {
            makeS3Request({ method: 'DELETE', authCredentials, bucket }, err => {
                assert.ifError(err);
                done();
            });
        });
    });

    for (const protocol of protocols) {
        for (const algo of algos) {
            itSkipIfAWS(
                `${protocol.name} with wrong x-amz-checksum-${algo.name}: returns 400 BadDigest`,
                done => {
                    const url = `http://localhost:8000/${bucket}/${objectKey}`;
                    doPutRequest(url, protocol.buildHeaders(algo), protocol.buildBody(algo),
                        (err, res) => assertBadDigest(err, res, done));
                }
            );
        }
    }
});

describe('UploadPart: bad checksum is rejected', () => {
    let uploadId;

    before(done => {
        async.series([
            next => makeS3Request({ method: 'PUT', authCredentials, bucket }, next),
            next => makeS3Request({
                method: 'POST',
                authCredentials,
                bucket,
                objectKey,
                queryObj: { uploads: '' },
            }, (err, res) => {
                if (err) { return next(err); }
                const match = res.body.match(/<UploadId>([^<]+)<\/UploadId>/);
                assert(match, `missing UploadId in response: ${res.body}`);
                uploadId = match[1];
                return next();
            }),
        ], err => {
            assert.ifError(err);
            done();
        });
    });

    after(done => {
        async.series([
            next => makeS3Request({
                method: 'DELETE',
                authCredentials,
                bucket,
                objectKey,
                queryObj: { uploadId },
            }, next),
            // Delete the object key first (defensive: clears any state left by a previous run).
            next => makeS3Request({ method: 'DELETE', authCredentials, bucket, objectKey }, () => next()),
            next => makeS3Request({ method: 'DELETE', authCredentials, bucket }, next),
        ], err => {
            assert.ifError(err);
            done();
        });
    });

    for (const protocol of protocols) {
        for (const algo of algos) {
            itSkipIfAWS(
                `${protocol.name} with wrong x-amz-checksum-${algo.name}: returns 400 BadDigest`,
                done => {
                    const url = `http://localhost:8000/${bucket}/${objectKey}` +
                        `?partNumber=1&uploadId=${uploadId}`;
                    doPutRequest(url, protocol.buildHeaders(algo), protocol.buildBody(algo),
                        (err, res) => assertBadDigest(err, res, done));
                }
            );
        }
    }
});

describe('PutObject: trailer and checksum protocol scenarios', () => {
    before(done => {
        makeS3Request({ method: 'PUT', authCredentials, bucket }, err => {
            assert.ifError(err);
            done();
        });
    });

    after(done => {
        // Some scenario tests store objects; delete the object key before removing the bucket.
        makeS3Request({ method: 'DELETE', authCredentials, bucket, objectKey }, () => {
            makeS3Request({ method: 'DELETE', authCredentials, bucket }, err => {
                assert.ifError(err);
                done();
            });
        });
    });

    makeScenarioTests(() => `http://localhost:8000/${bucket}/${objectKey}`);

});

describe('UploadPart: trailer and checksum protocol scenarios', () => {
    let uploadId2;

    before(done => {
        async.series([
            next => makeS3Request({ method: 'PUT', authCredentials, bucket }, next),
            next => makeS3Request({
                method: 'POST',
                authCredentials,
                bucket,
                objectKey,
                queryObj: { uploads: '' },
            }, (err, res) => {
                if (err) { return next(err); }
                const match = res.body.match(/<UploadId>([^<]+)<\/UploadId>/);
                assert(match, `missing UploadId in response: ${res.body}`);
                uploadId2 = match[1];
                return next();
            }),
        ], err => {
            assert.ifError(err);
            done();
        });
    });

    after(done => {
        async.series([
            next => makeS3Request({
                method: 'DELETE',
                authCredentials,
                bucket,
                objectKey,
                queryObj: { uploadId: uploadId2 },
            }, next),
            // Delete the object key first (defensive: clears any state left by a previous run).
            next => makeS3Request({ method: 'DELETE', authCredentials, bucket, objectKey }, () => next()),
            next => makeS3Request({ method: 'DELETE', authCredentials, bucket }, next),
        ], err => {
            assert.ifError(err);
            done();
        });
    });

    makeScenarioTests(
        () => `http://localhost:8000/${bucket}/${objectKey}?partNumber=1&uploadId=${uploadId2}`
    );
});
