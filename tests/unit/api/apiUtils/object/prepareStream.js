const assert = require('assert');
const { errors } = require('arsenal');

const { prepareStream } = require('../../../../../lib/api/apiUtils/object/prepareStream');
const ChecksumTransform = require('../../../../../lib/auth/streamingV4/ChecksumTransform');
const { DummyRequestLogger } = require('../../../helpers');
const DummyRequest = require('../../../DummyRequest');

const log = new DummyRequestLogger();

function makeRequest(headers, body) {
    return new DummyRequest({ headers }, body != null ? Buffer.from(body) : undefined);
}

const mockV4Params = {
    accessKey: 'AKIAIOSFODNN7EXAMPLE',
    signatureFromRequest: 'abc123',
    region: 'us-east-1',
    scopeDate: '20210101',
    timestamp: '20210101T000000Z',
    credentialScope: '20210101/us-east-1/s3/aws4_request',
};

describe('prepareStream return value shape', () => {
    it('returns { error: null, stream: ChecksumTransform } for UNSIGNED-PAYLOAD', () => {
        const request = makeRequest({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
        const result = prepareStream(request, null, log, () => {});
        assert.strictEqual(result.error, null);
        assert(result.stream instanceof ChecksumTransform);
    });

    it('returns { error: InvalidRequest, stream: null } on invalid checksum headers', () => {
        const request = makeRequest({
            'x-amz-checksum-crc32': 'AAAAAA==',
            'x-amz-checksum-sha256': 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
        });
        const result = prepareStream(request, null, log, () => {});
        assert(result.error.is.InvalidRequest);
        assert.strictEqual(result.stream, null);
    });

    it('returns { error: BadRequest, stream: null } for unsupported x-amz-content-sha256', () => {
        const request = makeRequest({
            'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
        });
        const result = prepareStream(request, null, log, () => {});
        assert(result.error.is.BadRequest);
        assert.strictEqual(result.stream, null);
    });
});

describe('prepareStream STREAMING-AWS4-HMAC-SHA256-PAYLOAD', () => {
    it('with valid streamingV4Params: returns ChecksumTransform as final stream', () => {
        const request = makeRequest({ 'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD' });
        const result = prepareStream(request, mockV4Params, log, () => {});
        assert.strictEqual(result.error, null);
        assert(result.stream instanceof ChecksumTransform);
    });

    it('with valid streamingV4Params: returned stream uses crc64nvme by default', () => {
        const request = makeRequest({ 'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD' });
        const result = prepareStream(request, mockV4Params, log, () => {});
        assert.strictEqual(result.stream.algoName, 'crc64nvme');
    });

    it('with x-amz-checksum-crc32c header: ChecksumTransform uses crc32c algorithm', () => {
        const request = makeRequest({
            'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
            'x-amz-checksum-crc32c': 'AAAAAA==',
        });
        const result = prepareStream(request, mockV4Params, log, () => {});
        assert.strictEqual(result.error, null);
        assert.strictEqual(result.stream.algoName, 'crc32c');
        assert.strictEqual(result.stream.expectedDigest, 'AAAAAA==');
    });

    it('with null streamingV4Params: returns InvalidArgument error', () => {
        const request = makeRequest({ 'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD' });
        const result = prepareStream(request, null, log, () => {});
        assert.deepStrictEqual(result.error, errors.InvalidArgument);
        assert.strictEqual(result.stream, null);
    });

    it('with non-object streamingV4Params (string): returns InvalidArgument error', () => {
        const request = makeRequest({ 'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD' });
        const result = prepareStream(request, 'not-an-object', log, () => {});
        assert.deepStrictEqual(result.error, errors.InvalidArgument);
        assert.strictEqual(result.stream, null);
    });
});

describe('prepareStream STREAMING-UNSIGNED-PAYLOAD-TRAILER', () => {
    it('returns ChecksumTransform with isTrailer=true', () => {
        const request = makeRequest({
            'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
            'x-amz-trailer': 'x-amz-checksum-crc32',
        });
        const result = prepareStream(request, null, log, () => {});
        assert.strictEqual(result.error, null);
        assert(result.stream instanceof ChecksumTransform);
        assert.strictEqual(result.stream.isTrailer, true);
        assert.strictEqual(result.stream.algoName, 'crc32');
    });

    it('trailer event from TrailingChecksumTransform calls setExpectedChecksum on ChecksumTransform', done => {
        const body = '0\r\nx-amz-checksum-crc32:AAAAAA==\r\n';
        const request = makeRequest({
            'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
            'x-amz-trailer': 'x-amz-checksum-crc32',
        }, body);
        const result = prepareStream(request, null, log, done);
        result.stream.resume();
        result.stream.on('finish', () => {
            assert.strictEqual(result.stream.trailerChecksumName, 'x-amz-checksum-crc32');
            assert.strictEqual(result.stream.trailerChecksumValue, 'AAAAAA==');
            done();
        });
        result.stream.on('error', done);
    });

    it('errCb is called when TrailingChecksumTransform emits an error', done => {
        // malformed chunked data triggers an error in TrailingChecksumTransform
        const request = makeRequest({
            'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
            'x-amz-trailer': 'x-amz-checksum-crc32',
        }, 'zz\r\n'); // invalid hex chunk size
        prepareStream(request, null, log, err => {
            assert(err.is.InvalidArgument);
            done();
        });
    });

    it('errCb is called when ChecksumTransform emits an error', done => {
        const request = makeRequest({
            'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
            'x-amz-trailer': 'x-amz-checksum-crc32',
        });
        const result = prepareStream(request, null, log, err => {
            assert.deepStrictEqual(err, errors.InternalError);
            done();
        });
        result.stream.emit('error', errors.InternalError);
    });
});

describe('prepareStream UNSIGNED-PAYLOAD', () => {
    it('returns ChecksumTransform as final stream', () => {
        const request = makeRequest({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
        const result = prepareStream(request, null, log, () => {});
        assert.strictEqual(result.error, null);
        assert(result.stream instanceof ChecksumTransform);
    });

    it('ChecksumTransform receives the algorithm and expected digest from headers', () => {
        const request = makeRequest({
            'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
            'x-amz-checksum-crc32': 'AAAAAA==',
        });
        const result = prepareStream(request, null, log, () => {});
        assert.strictEqual(result.stream.algoName, 'crc32');
        assert.strictEqual(result.stream.expectedDigest, 'AAAAAA==');
    });

    it('errCb is called when ChecksumTransform emits an error', done => {
        const request = makeRequest({ 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' });
        const result = prepareStream(request, null, log, err => {
            assert.deepStrictEqual(err, errors.InternalError);
            done();
        });
        result.stream.emit('error', errors.InternalError);
    });
});

describe('prepareStream default (no x-amz-content-sha256)', () => {
    it('no x-amz-content-sha256 header: returns ChecksumTransform with crc64nvme algorithm', () => {
        const request = makeRequest({});
        const result = prepareStream(request, null, log, () => {});
        assert.strictEqual(result.error, null);
        assert(result.stream instanceof ChecksumTransform);
        assert.strictEqual(result.stream.algoName, 'crc64nvme');
    });

    it('unsupported x-amz-content-sha256 (STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER): returns BadRequest', () => {
        const request = makeRequest({
            'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
        });
        const result = prepareStream(request, null, log, () => {});
        assert(result.error.is.BadRequest);
        assert.strictEqual(result.stream, null);
    });

    it('errCb is called when ChecksumTransform emits an error', done => {
        const request = makeRequest({});
        const result = prepareStream(request, null, log, err => {
            assert.deepStrictEqual(err, errors.InternalError);
            done();
        });
        result.stream.emit('error', errors.InternalError);
    });
});
