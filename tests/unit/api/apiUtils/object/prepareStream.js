const assert = require('assert');
const crypto = require('crypto');
const { errors } = require('arsenal');

const { prepareStream } = require('../../../../../lib/api/apiUtils/object/prepareStream');
const ChecksumTransform = require('../../../../../lib/auth/streamingV4/ChecksumTransform');
const ContentSHA256Transform = require('../../../../../lib/auth/streamingV4/ContentSHA256Transform');
const V4Transform = require('../../../../../lib/auth/streamingV4/V4Transform');
const TrailingChecksumTransform = require('../../../../../lib/auth/streamingV4/trailingChecksumTransform');
const { DummyRequestLogger } = require('../../../helpers');
const DummyRequest = require('../../../DummyRequest');
const { defaultChecksumData } = require('../../../../../lib/api/apiUtils/integrity/validateChecksums');

const log = new DummyRequestLogger();
const defaultChecksums = { primary: defaultChecksumData, secondary: null };

// A literal payload hash is only verified for SigV4 header-authenticated
// requests, so these tests carry an AWS4 Authorization header.
const sigV4Auth =
    'AWS4-HMAC-SHA256 Credential=AK/20210101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc';
const bodyData = 'the streamed body';
const bodyHex = crypto.createHash('sha256').update(Buffer.from(bodyData)).digest('hex');

function makeRequest(headers, body) {
    return new DummyRequest({ headers }, body != null ? Buffer.from(body) : undefined);
}

function makeChecksums(algo, expected, isTrailer) {
    return { primary: { algorithm: algo, expected, isTrailer: !!isTrailer }, secondary: null };
}

const mockV4Params = {
    accessKey: 'AKIAIOSFODNN7EXAMPLE',
    signatureFromRequest: 'abc123',
    region: 'us-east-1',
    scopeDate: '20210101',
    timestamp: '20210101T000000Z',
    credentialScope: '20210101/us-east-1/s3/aws4_request',
};

describe('prepareStream', () => {
    describe('return value shape', () => {
        it('should return { error: null, stream: ChecksumTransform } for UNSIGNED-PAYLOAD', () => {
            const request = makeRequest({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            const result = prepareStream(request, null, defaultChecksums, log, () => {});
            assert.strictEqual(result.error, null);
            assert(result.stream instanceof ChecksumTransform);
        });

        it('should return the primary ChecksumTransform as the last stream of the pipeline', () => {
            const request = makeRequest({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            const result = prepareStream(request, null, defaultChecksums, log, () => {});
            assert.strictEqual(result.primaryChecksumStream, result.stream);
        });

        it('should return { error: BadRequest, stream: null } for unsupported x-amz-content-sha256', () => {
            const request = makeRequest({
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
            });
            const result = prepareStream(request, null, defaultChecksums, log, () => {});
            assert.strictEqual(result.error.message, 'BadRequest');
            assert.strictEqual(result.stream, null);
        });
    });

    describe('STREAMING-AWS4-HMAC-SHA256-PAYLOAD', () => {
        it('should return ChecksumTransform as final stream with valid streamingV4Params', () => {
            const request = makeRequest({ 'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD' });
            const result = prepareStream(request, mockV4Params, defaultChecksums, log, () => {});
            assert.strictEqual(result.error, null);
            assert(result.stream instanceof ChecksumTransform);
        });

        it('should use crc64nvme when default checksums are passed', () => {
            const request = makeRequest({ 'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD' });
            const result = prepareStream(request, mockV4Params, defaultChecksums, log, () => {});
            assert.strictEqual(result.stream.algoName, 'crc64nvme');
        });

        it('should use crc32c algorithm when crc32c checksums are passed', () => {
            const request = makeRequest({
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
            });
            const checksums = makeChecksums('crc32c', 'AAAAAA==');
            const result = prepareStream(request, mockV4Params, checksums, log, () => {});
            assert.strictEqual(result.error, null);
            assert.strictEqual(result.stream.algoName, 'crc32c');
            assert.strictEqual(result.stream.expectedDigest, 'AAAAAA==');
        });

        it('should return InvalidArgument error with null streamingV4Params', () => {
            const request = makeRequest({ 'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD' });
            const result = prepareStream(request, null, defaultChecksums, log, () => {});
            assert.deepStrictEqual(result.error, errors.InvalidArgument);
            assert.strictEqual(result.stream, null);
        });

        it('should return InvalidArgument error with non-object streamingV4Params', () => {
            const request = makeRequest({ 'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD' });
            const result = prepareStream(request, 'not-an-object', defaultChecksums, log, () => {});
            assert.deepStrictEqual(result.error, errors.InvalidArgument);
            assert.strictEqual(result.stream, null);
        });
    });

    describe('STREAMING-UNSIGNED-PAYLOAD-TRAILER', () => {
        it('should return ChecksumTransform with isTrailer=true', () => {
            const request = makeRequest({
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-crc32',
            });
            const checksums = makeChecksums('crc32', undefined, true);
            const result = prepareStream(request, null, checksums, log, () => {});
            assert.strictEqual(result.error, null);
            assert(result.stream instanceof ChecksumTransform);
            assert.strictEqual(result.stream.isTrailer, true);
            assert.strictEqual(result.stream.algoName, 'crc32');
        });

        it('should call setExpectedChecksum on ChecksumTransform when trailer event fires', done => {
            const body = '0\r\nx-amz-checksum-crc32:AAAAAA==\r\n';
            const request = makeRequest(
                {
                    'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                    'x-amz-trailer': 'x-amz-checksum-crc32',
                },
                body,
            );
            const checksums = makeChecksums('crc32', undefined, true);
            const result = prepareStream(request, null, checksums, log, done);
            result.stream.resume();
            result.stream.on('finish', () => {
                assert.strictEqual(result.stream.trailerChecksumName, 'x-amz-checksum-crc32');
                assert.strictEqual(result.stream.trailerChecksumValue, 'AAAAAA==');
                done();
            });
            result.stream.on('error', done);
        });

        it('should call errCb when TrailingChecksumTransform emits an error', done => {
            // malformed chunked data triggers an error in TrailingChecksumTransform
            const request = makeRequest(
                {
                    'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                    'x-amz-trailer': 'x-amz-checksum-crc32',
                },
                'zz\r\n',
            ); // invalid hex chunk size
            const checksums = makeChecksums('crc32', undefined, true);
            prepareStream(request, null, checksums, log, err => {
                assert.strictEqual(err.message, 'InvalidArgument');
                done();
            });
        });

        it('should call errCb when ChecksumTransform emits an error', done => {
            const request = makeRequest({
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-trailer': 'x-amz-checksum-crc32',
            });
            const checksums = makeChecksums('crc32', undefined, true);
            const result = prepareStream(request, null, checksums, log, err => {
                assert.deepStrictEqual(err, errors.InternalError);
                done();
            });
            result.stream.emit('error', errors.InternalError);
        });
    });

    describe('UNSIGNED-PAYLOAD', () => {
        it('should return ChecksumTransform as final stream', () => {
            const request = makeRequest({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            const result = prepareStream(request, null, defaultChecksums, log, () => {});
            assert.strictEqual(result.error, null);
            assert(result.stream instanceof ChecksumTransform);
        });

        it('should set algorithm and expected digest from checksums on ChecksumTransform', () => {
            const request = makeRequest({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            const checksums = makeChecksums('crc32', 'AAAAAA==');
            const result = prepareStream(request, null, checksums, log, () => {});
            assert.strictEqual(result.stream.algoName, 'crc32');
            assert.strictEqual(result.stream.expectedDigest, 'AAAAAA==');
        });

        it('should call errCb when ChecksumTransform emits an error', done => {
            const request = makeRequest({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            const result = prepareStream(request, null, defaultChecksums, log, err => {
                assert.deepStrictEqual(err, errors.InternalError);
                done();
            });
            result.stream.emit('error', errors.InternalError);
        });
    });

    describe('secondary checksum (dual-checksum)', () => {
        it('should return secondaryChecksumStream when secondary is provided for UNSIGNED-PAYLOAD', () => {
            const request = makeRequest({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            const checksums = {
                primary: { algorithm: 'crc64nvme', isTrailer: false, expected: undefined },
                secondary: { algorithm: 'crc32', isTrailer: false, expected: 'DUoRhQ==' },
            };
            const result = prepareStream(request, null, checksums, log, () => {});
            assert.strictEqual(result.error, null);
            assert(result.stream instanceof ChecksumTransform);
            assert.strictEqual(result.stream.algoName, 'crc64nvme');
            assert(result.secondaryChecksumStream instanceof ChecksumTransform);
            assert.strictEqual(result.secondaryChecksumStream.algoName, 'crc32');
        });

        it('should return null secondaryChecksumStream when secondary is null', () => {
            const request = makeRequest({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            const result = prepareStream(request, null, defaultChecksums, log, () => {});
            assert.strictEqual(result.secondaryChecksumStream, null);
        });

        it('should return secondaryChecksumStream for STREAMING-AWS4-HMAC-SHA256-PAYLOAD', () => {
            const request = makeRequest({ 'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD' });
            const checksums = {
                primary: { algorithm: 'crc64nvme', isTrailer: false, expected: undefined },
                secondary: { algorithm: 'sha256', isTrailer: false, expected: undefined },
            };
            const result = prepareStream(request, mockV4Params, checksums, log, () => {});
            assert.strictEqual(result.error, null);
            assert.strictEqual(result.stream.algoName, 'crc64nvme');
            assert(result.secondaryChecksumStream instanceof ChecksumTransform);
            assert.strictEqual(result.secondaryChecksumStream.algoName, 'sha256');
        });

        it('should wire trailer to secondaryChecksumStream for STREAMING-UNSIGNED-PAYLOAD-TRAILER', done => {
            const body = '0\r\nx-amz-checksum-sha256:test-value\r\n';
            const request = makeRequest(
                {
                    'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                    'x-amz-trailer': 'x-amz-checksum-sha256',
                },
                body,
            );
            const checksums = {
                primary: { algorithm: 'crc64nvme', isTrailer: false, expected: undefined },
                secondary: { algorithm: 'sha256', isTrailer: true, expected: undefined },
            };
            const result = prepareStream(request, null, checksums, log, () => {});
            assert.strictEqual(result.stream.algoName, 'crc64nvme');
            assert.strictEqual(result.secondaryChecksumStream.algoName, 'sha256');
            result.stream.resume();
            result.stream.on('finish', () => {
                assert.strictEqual(result.secondaryChecksumStream.trailerChecksumName, 'x-amz-checksum-sha256');
                assert.strictEqual(result.secondaryChecksumStream.trailerChecksumValue, 'test-value');
                // Primary should NOT have trailer set
                assert.strictEqual(result.stream.trailerChecksumValue, undefined);
                done();
            });
            result.stream.on('error', done);
        });
    });

    describe('no checksum requested', () => {
        [null, undefined, {}, { primary: null, secondary: null }].forEach(checksums => {
            it(`should not create any ChecksumTransform with ${JSON.stringify(checksums)}`, () => {
                const request = makeRequest({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
                const result = prepareStream(request, null, checksums, log, () => {});
                assert.strictEqual(result.error, null);
                assert.strictEqual(result.stream, request);
                assert.strictEqual(result.primaryChecksumStream, null);
                assert.strictEqual(result.secondaryChecksumStream, null);
                assert.strictEqual(result.contentSHA256Stream, null);
            });
        });

        it('should return the request itself when there is no x-amz-content-sha256 header', () => {
            const request = makeRequest({});
            const result = prepareStream(request, null, null, log, () => {});
            assert.strictEqual(result.error, null);
            assert.strictEqual(result.stream, request);
            assert.strictEqual(result.primaryChecksumStream, null);
            assert.strictEqual(result.secondaryChecksumStream, null);
            assert.strictEqual(result.contentSHA256Stream, null);
        });

        it('should still validate a literal x-amz-content-sha256 payload hash', done => {
            const request = makeRequest({ authorization: sigV4Auth, 'x-amz-content-sha256': bodyHex }, bodyData);
            const result = prepareStream(request, null, null, log, done);
            assert.strictEqual(result.primaryChecksumStream, null);
            assert.strictEqual(result.secondaryChecksumStream, null);
            assert(result.contentSHA256Stream instanceof ContentSHA256Transform);
            assert.strictEqual(result.stream, result.contentSHA256Stream);
            result.stream.resume();
            result.stream.on('finish', () => {
                assert.strictEqual(result.contentSHA256Stream.validateChecksum(), null);
                done();
            });
            result.stream.on('error', done);
        });

        it('should return the V4Transform for STREAMING-AWS4-HMAC-SHA256-PAYLOAD', () => {
            const request = makeRequest({ 'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD' });
            const result = prepareStream(request, mockV4Params, null, log, () => {});
            assert.strictEqual(result.error, null);
            assert(result.stream instanceof V4Transform);
            assert.strictEqual(result.primaryChecksumStream, null);
            assert.strictEqual(result.secondaryChecksumStream, null);
            assert.strictEqual(result.contentSHA256Stream, null);
        });

        it('should return the TrailingChecksumTransform for STREAMING-UNSIGNED-PAYLOAD-TRAILER', () => {
            const request = makeRequest({ 'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER' });
            const result = prepareStream(request, null, null, log, () => {});
            assert.strictEqual(result.error, null);
            assert(result.stream instanceof TrailingChecksumTransform);
            assert.strictEqual(result.primaryChecksumStream, null);
            assert.strictEqual(result.secondaryChecksumStream, null);
            assert.strictEqual(result.contentSHA256Stream, null);
        });

        it('should still reject an unsupported x-amz-content-sha256', () => {
            const request = makeRequest({
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
            });
            const result = prepareStream(request, null, null, log, () => {});
            assert.strictEqual(result.error.message, 'BadRequest');
            assert.strictEqual(result.stream, null);
        });
    });

    describe('default (no x-amz-content-sha256)', () => {
        it('should return ChecksumTransform with crc64nvme algorithm when default checksums passed', () => {
            const request = makeRequest({});
            const result = prepareStream(request, null, defaultChecksums, log, () => {});
            assert.strictEqual(result.error, null);
            assert(result.stream instanceof ChecksumTransform);
            assert.strictEqual(result.stream.algoName, 'crc64nvme');
        });

        it('should return BadRequest for unsupported x-amz-content-sha256 value', () => {
            const request = makeRequest({
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
            });
            const result = prepareStream(request, null, defaultChecksums, log, () => {});
            assert.strictEqual(result.error.message, 'BadRequest');
            assert.strictEqual(result.stream, null);
        });

        it('should call errCb when ChecksumTransform emits an error', done => {
            const request = makeRequest({});
            const result = prepareStream(request, null, defaultChecksums, log, err => {
                assert.deepStrictEqual(err, errors.InternalError);
                done();
            });
            result.stream.emit('error', errors.InternalError);
        });
    });

    describe('default (literal sha256 payload hash)', () => {
        it('should return a ContentSHA256Transform for a SigV4 header-auth request with a hex value', () => {
            const request = makeRequest({ authorization: sigV4Auth, 'x-amz-content-sha256': bodyHex });
            const result = prepareStream(request, null, defaultChecksums, log, () => {});
            assert.strictEqual(result.error, null);
            assert(result.stream instanceof ChecksumTransform);
            assert(result.contentSHA256Stream instanceof ContentSHA256Transform);
        });

        it('should return null contentSHA256Stream for UNSIGNED-PAYLOAD', () => {
            const request = makeRequest({ authorization: sigV4Auth, 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
            const result = prepareStream(request, null, defaultChecksums, log, () => {});
            assert.strictEqual(result.contentSHA256Stream, null);
        });

        it('should return null contentSHA256Stream for non-SigV4 (SigV2) auth even with a hex value', () => {
            const request = makeRequest({ authorization: 'AWS AKID:sig', 'x-amz-content-sha256': bodyHex });
            const result = prepareStream(request, null, defaultChecksums, log, () => {});
            assert.strictEqual(result.contentSHA256Stream, null);
        });

        it('should return null contentSHA256Stream when there is no authorization header', () => {
            const request = makeRequest({ 'x-amz-content-sha256': bodyHex });
            const result = prepareStream(request, null, defaultChecksums, log, () => {});
            assert.strictEqual(result.contentSHA256Stream, null);
        });

        it('should accumulate the body sha256 so validateChecksum passes when the hex matches', done => {
            const request = makeRequest({ authorization: sigV4Auth, 'x-amz-content-sha256': bodyHex }, bodyData);
            const result = prepareStream(request, null, defaultChecksums, log, done);
            result.stream.resume();
            result.stream.on('finish', () => {
                assert.strictEqual(result.contentSHA256Stream.validateChecksum(), null);
                done();
            });
            result.stream.on('error', done);
        });

        it('should pipe contentSHA256Stream upstream of a secondary checksum stream', done => {
            const request = makeRequest({ authorization: sigV4Auth, 'x-amz-content-sha256': bodyHex }, bodyData);
            const checksums = {
                primary: { algorithm: 'crc64nvme', isTrailer: false, expected: undefined },
                secondary: { algorithm: 'crc32', isTrailer: false, expected: undefined },
            };
            const result = prepareStream(request, null, checksums, log, done);
            assert(result.contentSHA256Stream instanceof ContentSHA256Transform);
            assert(result.secondaryChecksumStream instanceof ChecksumTransform);
            result.stream.resume();
            result.stream.on('finish', () => {
                // the content stream saw the full body even with a secondary present
                assert.strictEqual(result.contentSHA256Stream.validateChecksum(), null);
                done();
            });
            result.stream.on('error', done);
        });

        it('should invoke errCb only once when multiple streams error', () => {
            const request = makeRequest({ authorization: sigV4Auth, 'x-amz-content-sha256': bodyHex });
            let count = 0;
            const result = prepareStream(request, null, defaultChecksums, log, () => {
                count += 1;
            });
            result.contentSHA256Stream.emit('error', errors.InternalError);
            result.stream.emit('error', errors.InternalError);
            assert.strictEqual(count, 1);
        });
    });
});
