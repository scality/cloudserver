const assert = require('assert');

const validatePayloadProtocol = require('../../../../lib/api/apiUtils/object/validatePayloadProtocol');
const { unsupportedSignatureChecksums, supportedSignatureChecksums } = require('../../../../constants');

// validatePayloadProtocol only validates x-amz-content-sha256 for SigV4
// header-authenticated requests (Authorization: "AWS4-...").
const sigV4 = 'AWS4-HMAC-SHA256 Credential=AK/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc';
const sigV2 = 'AWS AKID:signature';
const validSha256Hex = 'a'.repeat(64);

// build SigV4 header-auth headers, merging any extras
const v4 = extra => Object.assign({ authorization: sigV4 }, extra);

describe('validatePayloadProtocol', () => {
    it('should return null for a valid hex sha256', () =>
        assert.ifError(validatePayloadProtocol(v4({ 'x-amz-content-sha256': validSha256Hex }))));

    supportedSignatureChecksums.forEach(protocol => {
        it(`should return null for supported protocol ${protocol}`, () =>
            assert.ifError(validatePayloadProtocol(v4({ 'x-amz-content-sha256': protocol }))));
    });

    it('should return null for non-SigV4 header auth, even with an invalid value', () =>
        assert.ifError(validatePayloadProtocol({ authorization: sigV2, 'x-amz-content-sha256': 'BAD' })));

    unsupportedSignatureChecksums.forEach(protocol => {
        it(`should return BadRequest for unsupported protocol ${protocol}`, () => {
            const err = validatePayloadProtocol(v4({ 'x-amz-content-sha256': protocol }));
            assert(err instanceof Error, 'expected an error');
            assert.strictEqual(err.message, 'BadRequest');
            assert.strictEqual(err.code, 400);
            assert.match(err.description, /is not supported/);
        });
    });

    it('should return InvalidArgument for a non-hex x-amz-content-sha256', () => {
        const err = validatePayloadProtocol(v4({ 'x-amz-content-sha256': 'BAD' }));
        assert(err instanceof Error, 'expected an error');
        assert.strictEqual(err.message, 'InvalidArgument');
        assert.strictEqual(err.code, 400);
        assert.match(err.description, /x-amz-content-sha256 must be/);
    });
});
