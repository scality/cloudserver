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
    getChecksumDataFromMPUHeaders,
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

    it('should return crc64nvme with isTrailer=false and expected=undefined when no headers', () => {
        const result = getChecksumDataFromHeaders({});
        assert.deepStrictEqual(result, { algorithm: 'crc64nvme', isTrailer: false, expected: undefined });
    });

    it('should return crc64nvme default when no checksum headers, no trailer, no sdk algo', () => {
        const result = getChecksumDataFromHeaders({ 'content-type': 'application/octet-stream' });
        assert.deepStrictEqual(result, { algorithm: 'crc64nvme', isTrailer: false, expected: undefined });
    });

    for (const [algo, digest] of Object.entries(validDigests)) {
        it(`should return algorithm, isTrailer=false and expected for x-amz-checksum-${algo} with valid digest`, () => {
            const result = getChecksumDataFromHeaders({ [`x-amz-checksum-${algo}`]: digest });
            assert.deepStrictEqual(result, { algorithm: algo, isTrailer: false, expected: digest });
        });
    }

    it('should return AlgoNotSupported error for x-amz-checksum-unknown-algo', () => {
        const result = getChecksumDataFromHeaders({ 'x-amz-checksum-md4': 'AAAAAA==' });
        assert.strictEqual(result.error, ChecksumError.AlgoNotSupported);
        assert.strictEqual(result.details.algorithm, 'md4');
    });

    it('should return MalformedChecksum error for x-amz-checksum-crc32 with malformed digest (wrong length)', () => {
        const result = getChecksumDataFromHeaders({ 'x-amz-checksum-crc32': 'AAAAA==' }); // 7 chars, crc32 needs 8
        assert.strictEqual(result.error, ChecksumError.MalformedChecksum);
        assert.strictEqual(result.details.algorithm, 'crc32');
    });

    it('should return MalformedChecksum error for x-amz-checksum-crc32 with malformed digest (invalid base64)', () => {
        const result = getChecksumDataFromHeaders({ 'x-amz-checksum-crc32': '!!!!!!!!' });
        assert.strictEqual(result.error, ChecksumError.MalformedChecksum);
        assert.strictEqual(result.details.algorithm, 'crc32');
    });

    it('should return MultipleChecksumTypes error for two x-amz-checksum- headers', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-checksum-crc32': validDigests.crc32,
            'x-amz-checksum-sha256': validDigests.sha256,
        });
        assert.strictEqual(result.error, ChecksumError.MultipleChecksumTypes);
    });

    it('should return MissingCorresponding when x-amz-sdk-checksum-algorithm has no x-amz-checksum- or x-amz-trailer',
        () => {
            const result = getChecksumDataFromHeaders({ 'x-amz-sdk-checksum-algorithm': 'crc32' });
            assert.strictEqual(result.error, ChecksumError.MissingCorresponding);
            assert.strictEqual(result.details.expected, 'crc32');
        });

    it('should return success for x-amz-checksum-crc32 with matching x-amz-sdk-checksum-algorithm CRC32', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-checksum-crc32': validDigests.crc32,
            'x-amz-sdk-checksum-algorithm': 'crc32',
        });
        assert.deepStrictEqual(result, { algorithm: 'crc32', isTrailer: false, expected: validDigests.crc32 });
    });

    it('should return AlgoNotSupportedSDK for x-amz-checksum-crc32 with mismatched x-amz-sdk-checksum-algorithm SHA256',
        () => {
            const result = getChecksumDataFromHeaders({
                'x-amz-checksum-crc32': validDigests.crc32,
                'x-amz-sdk-checksum-algorithm': 'sha256',
            });
            assert.strictEqual(result.error, ChecksumError.AlgoNotSupportedSDK);
            assert.strictEqual(result.details.algorithm, 'sha256');
        });

    it('should return AlgoNotSupportedSDK for x-amz-checksum-crc32 with non-string x-amz-sdk-checksum-algorithm',
        () => {
            const result = getChecksumDataFromHeaders({
                'x-amz-checksum-crc32': validDigests.crc32,
                'x-amz-sdk-checksum-algorithm': 1234,
            });
            assert.strictEqual(result.error, ChecksumError.AlgoNotSupportedSDK);
            assert.strictEqual(result.details.algorithm, 1234);
        });

    it('should return AlgoNotSupportedSDK for x-amz-checksum-crc32 with unknown x-amz-sdk-checksum-algorithm', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-checksum-crc32': validDigests.crc32,
            'x-amz-sdk-checksum-algorithm': 'md4',
        });
        assert.strictEqual(result.error, ChecksumError.AlgoNotSupportedSDK);
        assert.strictEqual(result.details.algorithm, 'md4');
    });

    it('should return isTrailer=true for x-amz-trailer: x-amz-checksum-crc32', () => {
        const result = getChecksumDataFromHeaders({ 'x-amz-trailer': 'x-amz-checksum-crc32' });
        assert.deepStrictEqual(result, { algorithm: 'crc32', isTrailer: true, expected: undefined });
    });

    it('should return isTrailer=true for x-amz-trailer: x-amz-checksum-crc64nvme', () => {
        const result = getChecksumDataFromHeaders({ 'x-amz-trailer': 'x-amz-checksum-crc64nvme' });
        assert.deepStrictEqual(result, { algorithm: 'crc64nvme', isTrailer: true, expected: undefined });
    });

    it('should return TrailerNotSupported for x-amz-trailer with unsupported value (not x-amz-checksum- prefix)',
        () => {
            const result = getChecksumDataFromHeaders({ 'x-amz-trailer': 'x-custom-header' });
            assert.strictEqual(result.error, ChecksumError.TrailerNotSupported);
            assert.strictEqual(result.details.value, 'x-custom-header');
        });

    it('should return TrailerNotSupported for x-amz-trailer: x-amz-checksum-unknown-algo', () => {
        const result = getChecksumDataFromHeaders({ 'x-amz-trailer': 'x-amz-checksum-md4' });
        assert.strictEqual(result.error, ChecksumError.TrailerNotSupported);
        assert.strictEqual(result.details.value, 'x-amz-checksum-md4');
    });

    it('should return TrailerAndChecksum error for x-amz-trailer with also an x-amz-checksum- header', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-trailer': 'x-amz-checksum-crc32',
            'x-amz-checksum-crc32': validDigests.crc32,
        });
        assert.strictEqual(result.error, ChecksumError.TrailerAndChecksum);
    });

    it('should return success for x-amz-trailer with matching x-amz-sdk-checksum-algorithm', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-trailer': 'x-amz-checksum-crc32',
            'x-amz-sdk-checksum-algorithm': 'crc32',
        });
        assert.deepStrictEqual(result, { algorithm: 'crc32', isTrailer: true, expected: undefined });
    });

    it('should return AlgoNotSupportedSDK for x-amz-trailer with mismatched x-amz-sdk-checksum-algorithm', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-trailer': 'x-amz-checksum-crc32',
            'x-amz-sdk-checksum-algorithm': 'sha256',
        });
        assert.strictEqual(result.error, ChecksumError.AlgoNotSupportedSDK);
        assert.strictEqual(result.details.algorithm, 'sha256');
    });
});

describe('arsenalErrorFromChecksumError', () => {
    it('should return null for MissingChecksum', () => {
        const result = arsenalErrorFromChecksumError({ error: ChecksumError.MissingChecksum, details: null });
        assert.strictEqual(result, null);
    });

    it('should return BadDigest mentioning CRC32 for XAmzMismatch with crc32', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.XAmzMismatch,
            details: { algorithm: 'crc32', calculated: 'a', expected: 'b' },
        });
        assert.strictEqual(result.message, 'BadDigest');
        assert.strictEqual(result.description, 'The CRC32 you specified did not match the calculated checksum.');
    });

    it('should return BadDigest mentioning SHA256 for XAmzMismatch with sha256', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.XAmzMismatch,
            details: { algorithm: 'sha256', calculated: 'a', expected: 'b' },
        });
        assert.strictEqual(result.message, 'BadDigest');
        assert.strictEqual(result.description, 'The SHA256 you specified did not match the calculated checksum.');
    });

    it('should return InvalidRequest for AlgoNotSupported', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.AlgoNotSupported,
            details: { algorithm: 'md4' },
        });
        assert.strictEqual(result.message, 'InvalidRequest');
    });

    it('should return InvalidRequest for AlgoNotSupportedSDK', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.AlgoNotSupportedSDK,
            details: { algorithm: 'md4' },
        });
        assert.strictEqual(result.message, 'InvalidRequest');
    });

    it('should return InvalidRequest for MissingCorresponding', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.MissingCorresponding,
            details: { expected: 'crc32' },
        });
        assert.strictEqual(result.message, 'InvalidRequest');
    });

    it('should return InvalidRequest for MultipleChecksumTypes', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.MultipleChecksumTypes,
            details: { algorithms: ['x-amz-checksum-crc32', 'x-amz-checksum-sha256'] },
        });
        assert.strictEqual(result.message, 'InvalidRequest');
    });

    it('should return InvalidRequest mentioning crc32 for MalformedChecksum', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.MalformedChecksum,
            details: { algorithm: 'crc32', expected: 'bad' },
        });
        assert.strictEqual(result.message, 'InvalidRequest');
        assert.strictEqual(result.description, 'Value for x-amz-checksum-crc32 header is invalid.');
    });

    it('should return InvalidDigest for MD5Invalid', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.MD5Invalid,
            details: { expected: 'bad' },
        });
        assert.deepStrictEqual(result, ArsenalErrors.InvalidDigest);
    });

    it('should return MalformedTrailerError for TrailerAlgoMismatch', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.TrailerAlgoMismatch,
            details: { algorithm: 'crc32' },
        });
        assert.deepStrictEqual(result, ArsenalErrors.MalformedTrailerError);
    });

    it('should return MalformedTrailerError for TrailerMissing', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.TrailerMissing,
            details: { expectedTrailer: 'x-amz-checksum-crc32' },
        });
        assert.deepStrictEqual(result, ArsenalErrors.MalformedTrailerError);
    });

    it('should return MalformedTrailerError for TrailerUnexpected', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.TrailerUnexpected,
            details: { name: 'x-amz-checksum-crc32', val: 'AAAAAA==' },
        });
        assert.deepStrictEqual(result, ArsenalErrors.MalformedTrailerError);
    });

    it('should return InvalidRequest mentioning the algorithm for TrailerChecksumMalformed', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.TrailerChecksumMalformed,
            details: { algorithm: 'sha256', expected: 'bad' },
        });
        assert.strictEqual(result.message, 'InvalidRequest');
        assert.strictEqual(result.description, 'Value for x-amz-checksum-sha256 trailing header is invalid.');
    });

    it('should return InvalidRequest for TrailerAndChecksum', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.TrailerAndChecksum,
            details: { trailer: 'x-amz-checksum-crc32', checksum: ['x-amz-checksum-crc32'] },
        });
        assert.strictEqual(result.message, 'InvalidRequest');
    });

    it('should return InvalidRequest for TrailerNotSupported', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.TrailerNotSupported,
            details: { value: 'x-custom-header' },
        });
        assert.strictEqual(result.message, 'InvalidRequest');
    });

    it('should return BadDigest for unknown error type (default)', () => {
        const result = arsenalErrorFromChecksumError({ error: 'SomeUnknownError', details: null });
        assert.deepStrictEqual(result, ArsenalErrors.BadDigest);
    });

    it('should return InvalidRequest for MPUAlgoNotSupported', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.MPUAlgoNotSupported,
            details: { algorithm: 'md4' },
        });
        assert.strictEqual(result.message, 'InvalidRequest');
        assert.match(result.description, /\[CRC32, CRC32C, CRC64NVME, SHA1, SHA256\]/);
    });

    it('should return InvalidRequest for MPUTypeInvalid', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.MPUTypeInvalid,
            details: { type: 'BADTYPE' },
        });
        assert.strictEqual(result.message, 'InvalidRequest');
        assert.strictEqual(result.description,
            'Value for x-amz-checksum-type header is invalid.');
    });

    it('should return InvalidRequest for MPUTypeWithoutAlgo', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.MPUTypeWithoutAlgo,
            details: { type: 'COMPOSITE' },
        });
        assert.strictEqual(result.message, 'InvalidRequest');
        assert.match(result.description,
            /x-amz-checksum-type header can only be used with the x-amz-checksum-algorithm header/);
    });

    it('should return InvalidRequest for MPUInvalidCombination mentioning type and algorithm', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.MPUInvalidCombination,
            details: { algorithm: 'sha256', type: 'FULL_OBJECT' },
        });
        assert.strictEqual(result.message, 'InvalidRequest');
        assert.strictEqual(result.description,
            'The FULL_OBJECT checksum type cannot be used with the SHA256 checksum algorithm.');
    });
});

describe('getChecksumDataFromMPUHeaders', () => {
    describe('no checksum headers (default)', () => {
        it('should return crc64nvme/FULL_OBJECT with isDefault=true when no headers', () => {
            const result = getChecksumDataFromMPUHeaders({});
            assert.deepStrictEqual(result, {
                algorithm: 'crc64nvme', type: 'FULL_OBJECT', isDefault: true,
            });
        });
    });

    describe('algorithm only (no type header)', () => {
        const algoDefaults = {
            crc32: 'COMPOSITE',
            crc32c: 'COMPOSITE',
            crc64nvme: 'FULL_OBJECT',
            sha1: 'COMPOSITE',
            sha256: 'COMPOSITE',
        };

        for (const [algo, expectedType] of Object.entries(algoDefaults)) {
            it(`should default to ${expectedType} for ${algo}`, () => {
                const result = getChecksumDataFromMPUHeaders({
                    'x-amz-checksum-algorithm': algo,
                });
                assert.deepStrictEqual(result, {
                    algorithm: algo, type: expectedType, isDefault: false,
                });
            });
        }

        it('should accept uppercase algorithm (CRC32) and normalize to lowercase', () => {
            const result = getChecksumDataFromMPUHeaders({
                'x-amz-checksum-algorithm': 'CRC32',
            });
            assert.deepStrictEqual(result, {
                algorithm: 'crc32', type: 'COMPOSITE', isDefault: false,
            });
        });

        it('should accept mixed case algorithm (Sha256) and normalize to lowercase', () => {
            const result = getChecksumDataFromMPUHeaders({
                'x-amz-checksum-algorithm': 'Sha256',
            });
            assert.deepStrictEqual(result, {
                algorithm: 'sha256', type: 'COMPOSITE', isDefault: false,
            });
        });
    });

    describe('algorithm + type (valid combinations)', () => {
        const validCombinations = [
            ['crc32', 'FULL_OBJECT'],
            ['crc32', 'COMPOSITE'],
            ['crc32c', 'FULL_OBJECT'],
            ['crc32c', 'COMPOSITE'],
            ['crc64nvme', 'FULL_OBJECT'],
            ['sha1', 'COMPOSITE'],
            ['sha256', 'COMPOSITE'],
        ];

        for (const [algo, type] of validCombinations) {
            it(`should accept ${algo} + ${type}`, () => {
                const result = getChecksumDataFromMPUHeaders({
                    'x-amz-checksum-algorithm': algo,
                    'x-amz-checksum-type': type,
                });
                assert.deepStrictEqual(result, {
                    algorithm: algo, type, isDefault: false,
                });
            });
        }
    });

    describe('unknown algorithm', () => {
        it('should return MPUAlgoNotSupported for unknown algorithm', () => {
            const result = getChecksumDataFromMPUHeaders({
                'x-amz-checksum-algorithm': 'md4',
            });
            assert.strictEqual(result.error, ChecksumError.MPUAlgoNotSupported);
            assert.strictEqual(result.details.algorithm, 'md4');
        });

        it('should return MPUAlgoNotSupported even when type is also invalid', () => {
            const result = getChecksumDataFromMPUHeaders({
                'x-amz-checksum-algorithm': 'md4',
                'x-amz-checksum-type': 'BADTYPE',
            });
            assert.strictEqual(result.error, ChecksumError.MPUAlgoNotSupported);
        });
    });

    describe('unknown type', () => {
        it('should return MPUTypeInvalid for unknown type value', () => {
            const result = getChecksumDataFromMPUHeaders({
                'x-amz-checksum-algorithm': 'crc32',
                'x-amz-checksum-type': 'BADTYPE',
            });
            assert.strictEqual(result.error, ChecksumError.MPUTypeInvalid);
            assert.strictEqual(result.details.type, 'BADTYPE');
        });
    });

    describe('type without algorithm', () => {
        it('should return MPUTypeWithoutAlgo when only type header is sent', () => {
            const result = getChecksumDataFromMPUHeaders({
                'x-amz-checksum-type': 'COMPOSITE',
            });
            assert.strictEqual(result.error, ChecksumError.MPUTypeWithoutAlgo);
            assert.strictEqual(result.details.type, 'COMPOSITE');
        });

        it('should return MPUTypeWithoutAlgo even when type value is invalid', () => {
            const result = getChecksumDataFromMPUHeaders({
                'x-amz-checksum-type': 'BADTYPE',
            });
            assert.strictEqual(result.error, ChecksumError.MPUTypeWithoutAlgo);
        });
    });

    describe('invalid algorithm + type combinations', () => {
        const invalidCombinations = [
            ['sha1', 'FULL_OBJECT'],
            ['sha256', 'FULL_OBJECT'],
            ['crc64nvme', 'COMPOSITE'],
        ];

        for (const [algo, type] of invalidCombinations) {
            it(`should return MPUInvalidCombination for ${algo} + ${type}`, () => {
                const result = getChecksumDataFromMPUHeaders({
                    'x-amz-checksum-algorithm': algo,
                    'x-amz-checksum-type': type,
                });
                assert.strictEqual(result.error, ChecksumError.MPUInvalidCombination);
                assert.strictEqual(result.details.algorithm, algo);
                assert.strictEqual(result.details.type, type);
            });
        }
    });
});
