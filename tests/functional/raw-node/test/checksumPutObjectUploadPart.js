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

// Build a STREAMING-UNSIGNED-PAYLOAD-TRAILER body for the given data buffer and
// checksum algorithm. When digest is omitted the correct checksum is computed on
// the fly via crypto (works for sha256 and sha1). Pass an explicit digest to
// inject a wrong value for negative tests.
function buildTrailerBody(body, algoName, digest) {
    const hexLen = body.length.toString(16);
    const actualDigest = digest !== undefined
        ? digest
        : crypto.createHash(algoName).update(body).digest('base64');
    return `${hexLen}\r\n${body.toString()}\r\n0\r\nx-amz-checksum-${algoName}:${actualDigest}\n\r\n\r\n\r\n`;
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
            const body = buildTrailerBody(objData, algo.name, algo.wrongDigest);
            return {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': `x-amz-checksum-${algo.name}`,
                'x-amz-decoded-content-length': objData.length,
                'content-length': Buffer.byteLength(body),
            };
        },
        buildBody: algo => buildTrailerBody(objData, algo.name, algo.wrongDigest),
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
const trailerContentSha256 = crypto.createHash('sha256').update(trailerContent).digest('base64');
// md5("trailer content") in base64 — used in testS3PutTrailerWithContentMD5
const trailerContentMd5B64 = crypto.createHash('md5').update(trailerContent).digest('base64');

const testContent2 = Buffer.from('test content');
const testContent2Sha256Hex = crypto.createHash('sha256').update(testContent2).digest('hex');
const testContent2Sha256B64 = crypto.createHash('sha256').update(testContent2).digest('base64');

const largeBody = Buffer.alloc(10 * 1024 * 1024, 'a');
const largeBodySha256B64 = crypto.createHash('sha256').update(largeBody).digest('base64');


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
        'should return 200 for signed sha256 in x-amz-content-sha256, no x-amz-checksum header',
        done => {
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': testContent2Sha256Hex,
                'content-length': testContent2.length,
            }, testContent2, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'should return 200 for correct sha256 checksum with x-amz-sdk-checksum-algorithm',
        done => {
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': testContent2Sha256Hex,
                'x-amz-sdk-checksum-algorithm': 'SHA256',
                'x-amz-checksum-sha256': testContent2Sha256B64,
                'content-length': testContent2.length,
            }, testContent2, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'should return 400 BadDigest for wrong sha256 checksum with x-amz-sdk-checksum-algorithm',
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
        'should return 200 for UNSIGNED-PAYLOAD with correct sha256 checksum',
        done => {
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
                'x-amz-sdk-checksum-algorithm': 'SHA256',
                'x-amz-checksum-sha256': testContent2Sha256B64,
                'content-length': testContent2.length,
            }, testContent2, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'should return 400 IncompleteBody for TRAILER with empty body',
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
        'should return 200 for TRAILER with correct sha256 checksum',
        done => {
            const body = buildTrailerBody(trailerContent, 'sha256');
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'should return 500 InternalError for wrong x-amz-decoded-content-length',
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
        'should return 400 MalformedTrailerError when x-amz-trailer says sha1 but body trailer has sha256',
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
        'should return 400 BadDigest for TRAILER with wrong sha256 checksum',
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
        'should return 400 InvalidRequest for x-amz-trailer + x-amz-checksum-crc32 header',
        done => {
            const body = buildTrailerBody(trailerContent, 'sha256');
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
        'should return 400 MalformedTrailerError when no x-amz-trailer header but body has trailer',
        done => {
            const body = buildTrailerBody(trailerContent, 'sha256');
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                // no x-amz-trailer header
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(400, 'MalformedTrailerError',
                msgMalformedTrailer)(err, res, done));
        });

    itSkipIfAWS(
        'should return 200 for TRAILER with explicit Content-Length',
        done => {
            const body = buildTrailerBody(trailerContent, 'sha256');
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'should return 200 for TRAILER with matching x-amz-sdk-checksum-algorithm:SHA256',
        done => {
            const body = buildTrailerBody(trailerContent, 'sha256');
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-sdk-checksum-algorithm': 'SHA256',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'should return 400 InvalidRequest when x-amz-sdk-checksum-algorithm:SHA1 but x-amz-trailer is sha256',
        done => {
            const body = buildTrailerBody(trailerContent, 'sha256');
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
        'should return 400 InvalidRequest for x-amz-trailer:x-amz-checksum-sha3 (unknown algo)',
        done => {
            const body = buildTrailerBody(trailerContent, 'sha256');
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha3',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(400, 'InvalidRequest',
                'The value specified in the x-amz-trailer header is not supported')(err, res, done));
        });

    itSkipIfAWS(
        'should return 400 InvalidRequest for x-amz-trailer with non-checksum value',
        done => {
            const body = buildTrailerBody(trailerContent, 'sha256');
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'AAAAAAAAAAAAAAAAAAAAA',
                'x-amz-decoded-content-length': trailerContent.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(400, 'InvalidRequest',
                'The value specified in the x-amz-trailer header is not supported')(err, res, done));
        });

    itSkipIfAWS(
        'should return 400 InvalidRequest for trailer body with invalid base64 checksum value',
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
        'should return 400 InvalidRequest for x-amz-sdk-checksum-algorithm without x-amz-trailer',
        done => {
            const body = buildTrailerBody(trailerContent, 'sha256');
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
        'should return 400 MalformedTrailerError when x-amz-trailer header present but body has no trailer',
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
        'should return 200 when no x-amz-trailer and no body trailer',
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
        'should return 200 and ignore data after final CRLF',
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
        'should return 200 for TRAILER with correct Content-MD5 header',
        done => {
            const body = buildTrailerBody(trailerContent, 'sha256');
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
        'should return 200 for trailer line with whitespace around name and value',
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

// Large-body tests: 10MB of 'a's, verifying that streaming checksumming
// accumulates data across chunks correctly.
function makeLargeBodyTests(urlFn) {
    itSkipIfAWS(
        'should return 200 for UNSIGNED-PAYLOAD with correct sha256 checksum on 10MB body',
        done => {
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
                'x-amz-checksum-sha256': largeBodySha256B64,
                'content-length': largeBody.length,
            }, largeBody, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'should return 400 BadDigest for UNSIGNED-PAYLOAD with wrong sha256 checksum on 10MB body',
        done => {
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
                'x-amz-checksum-sha256': 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
                'content-length': largeBody.length,
            }, largeBody, (err, res) => assertStatus(400, 'BadDigest')(err, res, done));
        });

    itSkipIfAWS(
        'should return 200 for STREAMING-UNSIGNED-PAYLOAD-TRAILER with correct sha256 checksum on 10MB body',
        done => {
            const body = buildTrailerBody(largeBody, 'sha256');
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-decoded-content-length': largeBody.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(200)(err, res, done));
        });

    itSkipIfAWS(
        'should return 400 BadDigest for STREAMING-UNSIGNED-PAYLOAD-TRAILER with wrong sha256 checksum on 10MB body',
        done => {
            const body = buildTrailerBody(largeBody, 'sha256', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=');
            doPutRequest(urlFn(), {
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-sha256',
                'x-amz-decoded-content-length': largeBody.length,
                'content-length': Buffer.byteLength(body),
            }, body, (err, res) => assertStatus(400, 'BadDigest')(err, res, done));
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
                `should return 400 BadDigest for ${protocol.name} with wrong x-amz-checksum-${algo.name}`,
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
                `should return 400 BadDigest for ${protocol.name} with wrong x-amz-checksum-${algo.name}`,
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

describe('PutObject: large body streaming checksums', () => {
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

    makeLargeBodyTests(() => `http://localhost:8000/${bucket}/${objectKey}`);
});

describe('UploadPart: large body streaming checksums', () => {
    let uploadId3;

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
                uploadId3 = match[1];
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
                queryObj: { uploadId: uploadId3 },
            }, next),
            next => makeS3Request({ method: 'DELETE', authCredentials, bucket, objectKey }, () => next()),
            next => makeS3Request({ method: 'DELETE', authCredentials, bucket }, next),
        ], err => {
            assert.ifError(err);
            done();
        });
    });

    makeLargeBodyTests(
        () => `http://localhost:8000/${bucket}/${objectKey}?partNumber=1&uploadId=${uploadId3}`
    );
});
