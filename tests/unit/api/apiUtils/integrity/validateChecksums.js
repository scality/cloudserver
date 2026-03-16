const assert = require('assert');
const crypto = require('crypto');
const sinon = require('sinon');

const {
    validateChecksumsNoChunking,
    ChecksumError,
    validateMethodChecksumNoChunking,
    checksumedMethods,
    getChecksumDataFromHeaders,
    arsenalErrorFromChecksumError,
} = require('../../../../../lib/api/apiUtils/integrity/validateChecksums');
const { errors: ArsenalErrors } = require('arsenal');
const { config } = require('../../../../../lib/Config');

describe('validateChecksumsNoChunking MD5', () => {
    describe('with valid Content-MD5 header', () => {
        it('should return null when MD5 matches the body content', async () => {
            const body = 'Hello, World!';
            const expectedMd5 = crypto.createHash('md5').update(body, 'utf8').digest('base64');
            const headers = {
                'content-md5': expectedMd5
            };

            const result = await validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result, null);
        });
    });

    describe('with MD5 mismatch', () => {
        it('should return MD5Mismatch error when checksums do not match', async () => {
            const body = 'Hello, World!';
            const wrongMd5 = '1B2M2Y8AsgTpgAmY7PhCfg==';
            const expectedMd5 = crypto.createHash('md5').update(body, 'utf8').digest('base64');
            const headers = {
                'content-md5': wrongMd5
            };

            const result = await validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MD5Mismatch);
            assert.strictEqual(result.details.calculated, expectedMd5);
            assert.strictEqual(result.details.expected, wrongMd5);
        });
    });

    describe('without Content-MD5 header', () => {
        it('should return MissingChecksum error when no content-md5 header is present', async () => {
            const body = 'Hello, World!';
            const headers = {};

            const result = await validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MissingChecksum);
            assert.strictEqual(result.details, null);
        });

        it('should return MissingChecksum error when headers object is undefined', async () => {
            const body = 'Hello, World!';
            const headers = undefined;

            const result = await validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MissingChecksum);
            assert.strictEqual(result.details, null);
        });

        it('should return MD5Invalid error when content-md5 header is undefined', async () => {
            const body = 'Hello, World!';
            const headers = {
                'content-type': 'application/json',
                'content-md5': undefined
            };

            const result = await validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MD5Invalid);
            assert.strictEqual(result.details.expected, undefined);
        });

        it('should return MD5Invalid error when content-md5 header is null', async () => {
            const body = 'Hello, World!';
            const headers = {
                'content-type': 'application/json',
                'content-md5': null
            };

            const result = await validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MD5Invalid);
            assert.strictEqual(result.details.expected, null);
        });

        it('should return MD5Invalid error when content-md5 header is empty string', async () => {
            const body = 'Hello, World!';
            const headers = {
                'content-type': 'application/json',
                'content-md5': ''
            };

            const result = await validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MD5Invalid);
            assert.strictEqual(result.details.expected, '');
        });
    });
});

describe('validateChecksumsNoChunking CRC32, CRC32C, SHA1, SHA256, CRC64NVME', () => {
    const algos = [
        { name: 'crc32', data: 'crc32 data', digest: 'xCSBHA==', invalid: 'x', validWrong: 'AAAAAA==' },
        { name: 'crc32c', data: 'crc32c data', digest: 'oEjFGQ==', invalid: 'x', validWrong: 'AAAAAA==' },
        {
            name: 'sha1', data: 'sha1 data', digest: 'roREeoJPb6jNZz8PPT+/KtdXm0o=', invalid: 'x',
            validWrong: 'AAAAAAAAAAAAAAAAAAAAAAAAAAA='
        },
        {
            name: 'sha256', data: 'sha256 data',
            digest: 'jS/UevcoKxbM33kmPFujS72ior/9/i374VmGvbTAwAc=', invalid: 'x',
            validWrong: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA='
        },
        {
            name: 'crc64nvme', data: 'crc64nvme data', digest: 'Tpz+dGVqyhg=', invalid: 'x',
            validWrong: 'AAAAAAAAAAA='
        },
    ];

    for (const algo of algos) {
        it(`should return Mismatch error when wrong x-amz-checksum-${algo.name}`, async () => {
            const body = algo.data;
            const headers = {
                'content-type': 'application/json',
                [`x-amz-checksum-${algo.name}`]: algo.validWrong,
            };
            const result = await validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.XAmzMismatch);
            assert.strictEqual(result.details.calculated, algo.digest);
            assert.strictEqual(result.details.expected, algo.validWrong);
        });
    }

    for (const algo of algos) {
        it(`should return InvalidValue error when invalid x-amz-checksum-${algo.name}`, async () => {
            const body = algo.data;
            const headers = {
                'content-type': 'application/json',
                [`x-amz-checksum-${algo.name}`]: algo.invalid,
            };
            const result = await validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MalformedChecksum);
            assert.strictEqual(result.details.algorithm, algo.name);
            assert.strictEqual(result.details.expected, algo.invalid);
        });
    }

    for (const algo of algos) {
        it(`should return null when x-amz-checksum-${algo.name} match`, async () => {
            const body = algo.data;
            const headers = {
                'content-type': 'application/json',
                [`x-amz-checksum-${algo.name}`]: algo.digest,
            };
            const result = await validateChecksumsNoChunking(headers, body);
            assert.ifError(result);
        });
    }

    for (const algo of algos) {
        it(`should return error if missing corresponding x-amz-checksum-${algo.name}`, async () => {
            const body = algo.data;
            const headers = {
                'content-type': 'application/json',
                'x-amz-sdk-checksum-algorithm': algo.name,
            };
            const result = await validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MissingCorresponding);
            assert.strictEqual(result.details.expected, algo.name);
        });
    }

    for (const algo of algos) {
        it(`should return Mismatch error when wrong x-amz-checksum-${algo.name} x-amz-sdk-checksum case`, async () => {
            const body = algo.data;
            const headers = {
                'content-type': 'application/json',
                'x-amz-sdk-checksum-algorithm': algo.name,
                [`x-amz-checksum-${algo.name}`]: algo.validWrong,
            };
            const result = await validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.XAmzMismatch);
            assert.strictEqual(result.details.calculated, algo.digest);
            assert.strictEqual(result.details.expected, algo.validWrong);
        });
    }

    for (const algo of algos) {
        it(`should return null when x-amz-checksum-${algo.name} match x-amz-sdk-checksum case`, async () => {
            const body = algo.data;
            const headers = {
                'content-type': 'application/json',
                'x-amz-sdk-checksum-algorithm': algo.name,
                [`x-amz-checksum-${algo.name}`]: algo.digest,
            };
            const result = await validateChecksumsNoChunking(headers, body);
            assert.ifError(result);
        });
    }

    for (const algo of algos) {
        it(`should return null when x-amz-checksum-${algo.name} match, x-amz-sdk-checksum uppercase`, async () => {
            const body = algo.data;
            const headers = {
                'content-type': 'application/json',
                'x-amz-sdk-checksum-algorithm': algo.name.toUpperCase(),
                [`x-amz-checksum-${algo.name}`]: algo.digest,
            };
            const result = await validateChecksumsNoChunking(headers, body);
            assert.ifError(result);
        });
    }

    it('should return error MultipleChecksumTypes if multiple x-amz-checksum- present', async () => {
        const headers = {
            'content-type': 'application/json',
            'x-amz-checksum-sha1': '',
            'x-amz-checksum-sha256': '',
        };
        const result = await validateChecksumsNoChunking(headers, undefined);
        assert.strictEqual(result.error, ChecksumError.MultipleChecksumTypes);
        assert.deepStrictEqual(result.details.algorithms, ['x-amz-checksum-sha1', 'x-amz-checksum-sha256']);
    });

    for (const algo of algos) {
        it('should return error AlgoNotSupported if x-amz-sdk-checksum-algorithm algo not supported', async () => {
            const headers = {
                'content-type': 'application/json',
                'x-amz-sdk-checksum-algorithm': 'BAD',
                [`x-amz-checksum-${algo.name}`]: algo.digest,
            };
            const result = await validateChecksumsNoChunking(headers, algo.data);
            assert.strictEqual(result.error, ChecksumError.AlgoNotSupportedSDK);
            assert.strictEqual(result.details.algorithm, 'BAD');
        });
    }

    for (const algo of algos) {
        it('should return error AlgoNotSupported if x-amz-sdk-checksum-algorithm not a string', async () => {
            const headers = {
                'content-type': 'application/json',
                'x-amz-sdk-checksum-algorithm': 1234,
                [`x-amz-checksum-${algo.name}`]: algo.digest,
            };
            const result = await validateChecksumsNoChunking(headers, algo.data);
            assert.strictEqual(result.error, ChecksumError.AlgoNotSupportedSDK);
            assert.strictEqual(result.details.algorithm, 1234);
        });
    }

    it('should return error AlgoNotSupported if x-amz-checksum- algo not supported', async () => {
        const headers = {
            'content-type': 'application/json',
            'x-amz-checksum-BAD': 'DIGEST',
        };
        const result = await validateChecksumsNoChunking(headers, undefined);
        assert.strictEqual(result.error, ChecksumError.AlgoNotSupported);
        assert.strictEqual(result.details.algorithm, 'BAD');
    });

    it('should return error AlgoNotSupported if x-amz-checksum- algo is not a string', async () => {
        const headers = {
            'content-type': 'application/json',
            'x-amz-checksum-1234': 1234,
        };
        const result = await validateChecksumsNoChunking(headers, undefined);
        assert.strictEqual(result.error, ChecksumError.AlgoNotSupported);
        assert.strictEqual(result.details.algorithm, '1234');
    });

    it('should return error MissingChecksum if no x-amz-checksum- header', async () => {
        const headers = {
            'content-type': 'application/json',
        };
        const result = await validateChecksumsNoChunking(headers, undefined);
        assert.strictEqual(result.error, ChecksumError.MissingChecksum);
        assert.strictEqual(result.details, null);
    });
});

describe('validateMethodChecksumNoChunking', () => {
    let sandbox;
    let originalIntegrityChecks;

    beforeEach(() => {
        sandbox = sinon.createSandbox();
        originalIntegrityChecks = { ...config.integrityChecks };
    });

    afterEach(() => {
        sandbox.restore();
        config.integrityChecks = originalIntegrityChecks;
    });

    describe('when checksum mismatches', () => {
        Object.keys(checksumedMethods).forEach(method => {
            it(`should return BadDigest error for ${method} when checksum mismatch`, async () => {
                config.integrityChecks[method] = true;

                const body = 'Hello, World!';
                const wrongMd5 = '1B2M2Y8AsgTpgAmY7PhCfg==';
                const request = {
                    apiMethod: method,
                    headers: {
                        'content-md5': wrongMd5
                    }
                };
                const log = { debug: sandbox.stub() };

                const result = await validateMethodChecksumNoChunking(request, body, log);

                assert.deepStrictEqual(result, ArsenalErrors.BadDigest, 'Expected BadDigest error');
                assert(log.debug.calledOnce);
            });
        });
    });

    describe('when checksum mismatches', () => {
        Object.keys(checksumedMethods).forEach(method => {
            it(`should return InvalidDigest error for ${method} when checksum mismatch`, async () => {
                config.integrityChecks[method] = true;

                const body = 'Hello, World!';
                const wrongMd5 = 'wrongchecksum123=';
                const request = {
                    apiMethod: method,
                    headers: {
                        'content-md5': wrongMd5
                    }
                };
                const log = { debug: sandbox.stub() };

                const result = await validateMethodChecksumNoChunking(request, body, log);

                assert.deepStrictEqual(result, ArsenalErrors.InvalidDigest, 'Expected BadDigest error');
                assert(log.debug.calledOnce);
            });
        });
    });

    describe('when no checksum is provided', () => {
        Object.keys(checksumedMethods).forEach(method => {
            it(`should return null for ${method} when no checksum is provided`, async () => {
                config.integrityChecks[method] = true;

                const body = 'Hello, World!';
                const request = {
                    apiMethod: method,
                    headers: {}
                };
                const log = { debug: sandbox.stub() };

                const result = await validateMethodChecksumNoChunking(request, body, log);

                assert.strictEqual(result, null);
                assert(log.debug.notCalled);
            });
        });
    });

    describe('when checksum matches', () => {
        Object.keys(checksumedMethods).forEach(method => {
            it(`should return null for ${method} when checksum matches`, async () => {
                config.integrityChecks[method] = true;

                const body = 'Hello, World!';
                const correctMd5 = crypto.createHash('md5').update(body, 'utf8').digest('base64');
                const request = {
                    apiMethod: method,
                    headers: {
                        'content-md5': correctMd5
                    }
                };
                const log = { debug: sandbox.stub() };

                const result = await validateMethodChecksumNoChunking(request, body, log);

                assert.strictEqual(result, null);
                assert(log.debug.notCalled);
            });
        });
    });

    describe('when method is disabled in config', () => {
        Object.keys(checksumedMethods).forEach(method => {
            it(`should return null for ${method} when disabled, even with checksum mismatch`, async () => {
                config.integrityChecks[method] = false;

                const body = 'Hello, World!';
                const wrongMd5 = 'wrongchecksum123=';
                const request = {
                    apiMethod: method,
                    headers: {
                        'content-md5': wrongMd5
                    }
                };
                const log = { debug: sandbox.stub() };

                const result = await validateMethodChecksumNoChunking(request, body, log);

                assert.strictEqual(result, null);
                assert(log.debug.notCalled);
            });
        });
    });

    describe('when method is not in validation function mapping', () => {
        it('should return null for unsupported method even when enabled in config', async () => {
            const unsupportedMethod = 'someUnsupportedMethod';
            config.integrityChecks[unsupportedMethod] = true;

            const body = 'Hello, World!';
            const wrongMd5 = 'wrongchecksum123=';
            const request = {
                apiMethod: unsupportedMethod,
                headers: {
                    'content-md5': wrongMd5
                }
            };
            const log = { debug: sandbox.stub() };

            const result = await validateMethodChecksumNoChunking(request, body, log);

            assert.strictEqual(result, null);
            assert(log.debug.notCalled);
        });
    });

    describe('edge cases', () => {
        it('should return null when request has no apiMethod', async () => {
            const body = 'Hello, World!';
            const request = {
                headers: {
                    'content-md5': 'wrongchecksum123='
                }
            };
            const log = { debug: sandbox.stub() };

            const result = await validateMethodChecksumNoChunking(request, body, log);

            assert.strictEqual(result, null);
        });

        it('should return null when request apiMethod is undefined in config', async () => {
            const body = 'Hello, World!';
            const request = {
                apiMethod: 'nonExistentMethod',
                headers: {
                    'content-md5': 'wrongchecksum123='
                }
            };
            const log = { debug: sandbox.stub() };

            const result = await validateMethodChecksumNoChunking(request, body, log);

            assert.strictEqual(result, null);
        });
    });
});

describe('getChecksumDataFromHeaders', () => {
    // Valid-format digests (correct length and base64, content not verified by getChecksumDataFromHeaders)
    const validDigests = {
        crc32: 'AAAAAA==',     // 8 chars
        crc32c: 'AAAAAA==',     // 8 chars
        sha1: 'AAAAAAAAAAAAAAAAAAAAAAAAAAA=', // 28 chars
        sha256: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=', // 44 chars
        crc64nvme: 'AAAAAAAAAAA=', // 12 chars
    };

    it('no headers: returns crc64nvme with isTrailer=false and expected=undefined', () => {
        const result = getChecksumDataFromHeaders({});
        assert.deepStrictEqual(result, { algorithm: 'crc64nvme', isTrailer: false, expected: undefined });
    });

    it('no checksum headers, no trailer, no sdk algo: returns crc64nvme default', () => {
        const result = getChecksumDataFromHeaders({ 'content-type': 'application/octet-stream' });
        assert.deepStrictEqual(result, { algorithm: 'crc64nvme', isTrailer: false, expected: undefined });
    });

    for (const [algo, digest] of Object.entries(validDigests)) {
        it(`x-amz-checksum-${algo} with valid digest: returns algorithm, isTrailer=false, expected`, () => {
            const result = getChecksumDataFromHeaders({ [`x-amz-checksum-${algo}`]: digest });
            assert.deepStrictEqual(result, { algorithm: algo, isTrailer: false, expected: digest });
        });
    }

    it('x-amz-checksum-unknown-algo: returns AlgoNotSupported error', () => {
        const result = getChecksumDataFromHeaders({ 'x-amz-checksum-md4': 'AAAAAA==' });
        assert.strictEqual(result.error, ChecksumError.AlgoNotSupported);
        assert.strictEqual(result.details.algorithm, 'md4');
    });

    it('x-amz-checksum-crc32 with malformed digest (wrong length): returns MalformedChecksum error', () => {
        const result = getChecksumDataFromHeaders({ 'x-amz-checksum-crc32': 'AAAAA==' }); // 7 chars, crc32 needs 8
        assert.strictEqual(result.error, ChecksumError.MalformedChecksum);
        assert.strictEqual(result.details.algorithm, 'crc32');
    });

    it('x-amz-checksum-crc32 with malformed digest (invalid base64): returns MalformedChecksum error', () => {
        const result = getChecksumDataFromHeaders({ 'x-amz-checksum-crc32': '!!!!!!!!' });
        assert.strictEqual(result.error, ChecksumError.MalformedChecksum);
        assert.strictEqual(result.details.algorithm, 'crc32');
    });

    it('two x-amz-checksum- headers: returns MultipleChecksumTypes error', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-checksum-crc32': validDigests.crc32,
            'x-amz-checksum-sha256': validDigests.sha256,
        });
        assert.strictEqual(result.error, ChecksumError.MultipleChecksumTypes);
    });

    it('x-amz-sdk-checksum-algorithm with no x-amz-checksum- and no x-amz-trailer: returns MissingCorresponding',
        () => {
            const result = getChecksumDataFromHeaders({ 'x-amz-sdk-checksum-algorithm': 'crc32' });
            assert.strictEqual(result.error, ChecksumError.MissingCorresponding);
            assert.strictEqual(result.details.expected, 'crc32');
        });

    it('x-amz-checksum-crc32 with matching x-amz-sdk-checksum-algorithm CRC32: returns success', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-checksum-crc32': validDigests.crc32,
            'x-amz-sdk-checksum-algorithm': 'crc32',
        });
        assert.deepStrictEqual(result, { algorithm: 'crc32', isTrailer: false, expected: validDigests.crc32 });
    });

    it('x-amz-checksum-crc32 with mismatched x-amz-sdk-checksum-algorithm SHA256: returns AlgoNotSupportedSDK', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-checksum-crc32': validDigests.crc32,
            'x-amz-sdk-checksum-algorithm': 'sha256',
        });
        assert.strictEqual(result.error, ChecksumError.AlgoNotSupportedSDK);
        assert.strictEqual(result.details.algorithm, 'sha256');
    });

    it('x-amz-checksum-crc32 with non-string x-amz-sdk-checksum-algorithm: returns AlgoNotSupportedSDK', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-checksum-crc32': validDigests.crc32,
            'x-amz-sdk-checksum-algorithm': 1234,
        });
        assert.strictEqual(result.error, ChecksumError.AlgoNotSupportedSDK);
        assert.strictEqual(result.details.algorithm, 1234);
    });

    it('x-amz-checksum-crc32 with unknown x-amz-sdk-checksum-algorithm: returns AlgoNotSupportedSDK', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-checksum-crc32': validDigests.crc32,
            'x-amz-sdk-checksum-algorithm': 'md4',
        });
        assert.strictEqual(result.error, ChecksumError.AlgoNotSupportedSDK);
        assert.strictEqual(result.details.algorithm, 'md4');
    });

    it('x-amz-trailer: x-amz-checksum-crc32: returns isTrailer=true', () => {
        const result = getChecksumDataFromHeaders({ 'x-amz-trailer': 'x-amz-checksum-crc32' });
        assert.deepStrictEqual(result, { algorithm: 'crc32', isTrailer: true, expected: undefined });
    });

    it('x-amz-trailer: x-amz-checksum-crc64nvme: returns isTrailer=true', () => {
        const result = getChecksumDataFromHeaders({ 'x-amz-trailer': 'x-amz-checksum-crc64nvme' });
        assert.deepStrictEqual(result, { algorithm: 'crc64nvme', isTrailer: true, expected: undefined });
    });

    it('x-amz-trailer with unsupported value (not x-amz-checksum- prefix): returns TrailerNotSupported', () => {
        const result = getChecksumDataFromHeaders({ 'x-amz-trailer': 'x-custom-header' });
        assert.strictEqual(result.error, ChecksumError.TrailerNotSupported);
        assert.strictEqual(result.details.value, 'x-custom-header');
    });

    it('x-amz-trailer: x-amz-checksum-unknown-algo: returns TrailerNotSupported', () => {
        const result = getChecksumDataFromHeaders({ 'x-amz-trailer': 'x-amz-checksum-md4' });
        assert.strictEqual(result.error, ChecksumError.TrailerNotSupported);
        assert.strictEqual(result.details.value, 'x-amz-checksum-md4');
    });

    it('x-amz-trailer with also an x-amz-checksum- header: returns TrailerAndChecksum', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-trailer': 'x-amz-checksum-crc32',
            'x-amz-checksum-crc32': validDigests.crc32,
        });
        assert.strictEqual(result.error, ChecksumError.TrailerAndChecksum);
    });

    it('x-amz-trailer with matching x-amz-sdk-checksum-algorithm: returns success', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-trailer': 'x-amz-checksum-crc32',
            'x-amz-sdk-checksum-algorithm': 'crc32',
        });
        assert.deepStrictEqual(result, { algorithm: 'crc32', isTrailer: true, expected: undefined });
    });

    it('x-amz-trailer with mismatched x-amz-sdk-checksum-algorithm: returns AlgoNotSupportedSDK', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-trailer': 'x-amz-checksum-crc32',
            'x-amz-sdk-checksum-algorithm': 'sha256',
        });
        assert.strictEqual(result.error, ChecksumError.AlgoNotSupportedSDK);
        assert.strictEqual(result.details.algorithm, 'sha256');
    });
});

describe('arsenalErrorFromChecksumError', () => {
    it('MissingChecksum: returns null', () => {
        const result = arsenalErrorFromChecksumError({ error: ChecksumError.MissingChecksum, details: null });
        assert.strictEqual(result, null);
    });

    it('XAmzMismatch (crc32): returns BadDigest mentioning CRC32', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.XAmzMismatch,
            details: { algorithm: 'crc32', calculated: 'a', expected: 'b' },
        });
        assert(result.is.BadDigest);
        assert.strictEqual(result.description, 'The CRC32 you specified did not match the calculated checksum.');
    });

    it('XAmzMismatch (sha256): returns BadDigest mentioning SHA256', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.XAmzMismatch,
            details: { algorithm: 'sha256', calculated: 'a', expected: 'b' },
        });
        assert(result.is.BadDigest);
        assert.strictEqual(result.description, 'The SHA256 you specified did not match the calculated checksum.');
    });

    it('AlgoNotSupported: returns InvalidRequest', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.AlgoNotSupported,
            details: { algorithm: 'md4' },
        });
        assert(result.is.InvalidRequest);
    });

    it('AlgoNotSupportedSDK: returns InvalidRequest', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.AlgoNotSupportedSDK,
            details: { algorithm: 'md4' },
        });
        assert(result.is.InvalidRequest);
    });

    it('MissingCorresponding: returns InvalidRequest', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.MissingCorresponding,
            details: { expected: 'crc32' },
        });
        assert(result.is.InvalidRequest);
    });

    it('MultipleChecksumTypes: returns InvalidRequest', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.MultipleChecksumTypes,
            details: { algorithms: ['x-amz-checksum-crc32', 'x-amz-checksum-sha256'] },
        });
        assert(result.is.InvalidRequest);
    });

    it('MalformedChecksum (crc32): returns InvalidRequest mentioning crc32', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.MalformedChecksum,
            details: { algorithm: 'crc32', expected: 'bad' },
        });
        assert(result.is.InvalidRequest);
        assert.strictEqual(result.description, 'Value for x-amz-checksum-crc32 header is invalid.');
    });

    it('MD5Invalid: returns InvalidDigest', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.MD5Invalid,
            details: { expected: 'bad' },
        });
        assert.deepStrictEqual(result, ArsenalErrors.InvalidDigest);
    });

    it('TrailerAlgoMismatch: returns MalformedTrailerError', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.TrailerAlgoMismatch,
            details: { algorithm: 'crc32' },
        });
        assert.deepStrictEqual(result, ArsenalErrors.MalformedTrailerError);
    });

    it('TrailerMissing: returns MalformedTrailerError', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.TrailerMissing,
            details: { expectedTrailer: 'x-amz-checksum-crc32' },
        });
        assert.deepStrictEqual(result, ArsenalErrors.MalformedTrailerError);
    });

    it('TrailerUnexpected: returns MalformedTrailerError', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.TrailerUnexpected,
            details: { name: 'x-amz-checksum-crc32', val: 'AAAAAA==' },
        });
        assert.deepStrictEqual(result, ArsenalErrors.MalformedTrailerError);
    });

    it('TrailerChecksumMalformed: returns InvalidRequest mentioning the algorithm', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.TrailerChecksumMalformed,
            details: { algorithm: 'sha256', expected: 'bad' },
        });
        assert(result.is.InvalidRequest);
        assert.strictEqual(result.description, 'Value for x-amz-checksum-sha256 trailing header is invalid.');
    });

    it('TrailerAndChecksum: returns InvalidRequest', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.TrailerAndChecksum,
            details: { trailer: 'x-amz-checksum-crc32', checksum: ['x-amz-checksum-crc32'] },
        });
        assert(result.is.InvalidRequest);
    });

    it('TrailerNotSupported: returns InvalidRequest', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.TrailerNotSupported,
            details: { value: 'x-custom-header' },
        });
        assert(result.is.InvalidRequest);
    });

    it('unknown error type (default): returns BadDigest', () => {
        const result = arsenalErrorFromChecksumError({ error: 'SomeUnknownError', details: null });
        assert.deepStrictEqual(result, ArsenalErrors.BadDigest);
    });
});
