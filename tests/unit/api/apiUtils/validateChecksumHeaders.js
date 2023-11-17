const assert = require('assert');

const validateChecksumHeaders = require('../../../../lib/api/apiUtils/object/validateChecksumHeaders');
const { unsupportedSignatureChecksums, supportedSignatureChecksums } = require('../../../../constants');

const passingCases = [
    {
        description: 'should return null if no checksum headers are present',
        headers: {},
    },
    {
        description: 'should return null if UNSIGNED-PAYLOAD is used',
        headers: {
            'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
        },
    },
    {
        description: 'should return null if a sha256 checksum is used',
        headers: {
            'x-amz-content-sha256': 'thisIs64CharactersLongAndThatsAllWeCheckFor1234567890abcdefghijk',
        },
    },
];

supportedSignatureChecksums.forEach(checksum => {
    passingCases.push({
        description: `should return null if ${checksum} is used`,
        headers: {
            'x-amz-content-sha256': checksum,
        },
    });
});

const failingCases = [
    {
        description: 'should return BadRequest if a trailing checksum is used',
        headers: {
            'x-amz-trailer': 'test',
        },
    },
    {
        description: 'should return BadRequest if an unknown algo is used',
        headers: {
            'x-amz-content-sha256': 'UNSUPPORTED-CHECKSUM',
        },
    },
];

unsupportedSignatureChecksums.forEach(checksum => {
    failingCases.push({
        description: `should return BadRequest if ${checksum} is used`,
        headers: {
            'x-amz-content-sha256': checksum,
        },
    });
});


describe('validateChecksumHeaders', () => {
    passingCases.forEach(testCase => {
        it(testCase.description, () => {
            const result = validateChecksumHeaders(testCase.headers);
            assert.ifError(result);
        });
    });

    failingCases.forEach(testCase => {
        it(testCase.description, () => {
            const result = validateChecksumHeaders(testCase.headers);
            assert(result instanceof Error, 'Expected an error to be returned');
            assert.strictEqual(result.is.BadRequest, true);
            assert.strictEqual(result.code, 400);
        });
    });
});
