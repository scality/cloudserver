const assert = require('assert');
const crypto = require('crypto');

const { DummyRequestLogger } = require('../../../helpers');
const {
    validateChecksumsNoChunking,
    ChecksumError,
    ContentSHA256Type,
    parseContentSHA256,
    validateXAmzContentSHA256,
    validateMethodChecksumNoChunking,
    checksumedMethods,
    getChecksumDataFromHeaders,
    arsenalErrorFromChecksumError,
    getChecksumDataFromMPUHeaders,
    validateCompleteMultipartUploadChecksum,
    validateCompleteMPUChecksumType,
    getCopyObjectChecksumAlgorithm,
    areChecksumsEnabled,
} = require('../../../../../lib/api/apiUtils/integrity/validateChecksums');
const { config } = require('../../../../../lib/Config');
const { errors: ArsenalErrors } = require('arsenal');

describe('validateChecksumsNoChunking MD5', () => {
    describe('with valid Content-MD5 header', () => {
        it('should return null when MD5 matches the body content', async () => {
            const body = 'Hello, World!';
            const expectedMd5 = crypto.createHash('md5').update(body, 'utf8').digest('base64');
            const headers = {
                'content-md5': expectedMd5,
            };

            const result = await validateChecksumsNoChunking(headers, body);
            assert.ifError(result);
        });
    });

    describe('with MD5 mismatch', () => {
        it('should return MD5Mismatch error when checksums do not match', async () => {
            const body = 'Hello, World!';
            const wrongMd5 = '1B2M2Y8AsgTpgAmY7PhCfg==';
            const expectedMd5 = crypto.createHash('md5').update(body, 'utf8').digest('base64');
            const headers = {
                'content-md5': wrongMd5,
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
                'content-md5': undefined,
            };

            const result = await validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MD5Invalid);
            assert.strictEqual(result.details.expected, undefined);
        });

        it('should return MD5Invalid error when content-md5 header is null', async () => {
            const body = 'Hello, World!';
            const headers = {
                'content-type': 'application/json',
                'content-md5': null,
            };

            const result = await validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MD5Invalid);
            assert.strictEqual(result.details.expected, null);
        });

        it('should return MD5Invalid error when content-md5 header is empty string', async () => {
            const body = 'Hello, World!';
            const headers = {
                'content-type': 'application/json',
                'content-md5': '',
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
            name: 'sha1',
            data: 'sha1 data',
            digest: 'roREeoJPb6jNZz8PPT+/KtdXm0o=',
            invalid: 'x',
            validWrong: 'AAAAAAAAAAAAAAAAAAAAAAAAAAA=',
        },
        {
            name: 'sha256',
            data: 'sha256 data',
            digest: 'jS/UevcoKxbM33kmPFujS72ior/9/i374VmGvbTAwAc=',
            invalid: 'x',
            validWrong: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
        },
        {
            name: 'crc64nvme',
            data: 'crc64nvme data',
            digest: 'Tpz+dGVqyhg=',
            invalid: 'x',
            validWrong: 'AAAAAAAAAAA=',
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

    it('should ignore x-amz-checksum-algorithm alongside a valid x-amz-checksum-<algo> value', async () => {
        const body = 'sha256 data';
        const headers = {
            'content-type': 'application/json',
            'x-amz-checksum-algorithm': 'SHA256',
            'x-amz-checksum-sha256': 'jS/UevcoKxbM33kmPFujS72ior/9/i374VmGvbTAwAc=',
        };
        const result = await validateChecksumsNoChunking(headers, body);
        assert.ifError(result);
    });

    it('should ignore x-amz-checksum-type alongside a valid x-amz-checksum-<algo> value', async () => {
        const body = 'sha256 data';
        const headers = {
            'content-type': 'application/json',
            'x-amz-checksum-type': 'FULL_OBJECT',
            'x-amz-checksum-sha256': 'jS/UevcoKxbM33kmPFujS72ior/9/i374VmGvbTAwAc=',
        };
        const result = await validateChecksumsNoChunking(headers, body);
        assert.ifError(result);
    });

    it('should ignore both x-amz-checksum-algorithm and x-amz-checksum-type when combined with a value', async () => {
        const body = 'sha256 data';
        const headers = {
            'content-type': 'application/json',
            'x-amz-checksum-algorithm': 'SHA256',
            'x-amz-checksum-type': 'FULL_OBJECT',
            'x-amz-checksum-sha256': 'jS/UevcoKxbM33kmPFujS72ior/9/i374VmGvbTAwAc=',
        };
        const result = await validateChecksumsNoChunking(headers, body);
        assert.ifError(result);
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
    describe('when checksum mismatches', () => {
        Object.keys(checksumedMethods).forEach(method => {
            it(`should return BadDigest error for ${method} when checksum mismatch`, async () => {
                const body = 'Hello, World!';
                const wrongMd5 = '1B2M2Y8AsgTpgAmY7PhCfg==';
                const request = {
                    apiMethod: method,
                    headers: {
                        'content-md5': wrongMd5,
                    },
                };
                const log = new DummyRequestLogger();

                const result = await validateMethodChecksumNoChunking(request, body, log);

                assert.deepStrictEqual(result, ArsenalErrors.BadDigest, 'Expected BadDigest error');
            });
        });
    });

    describe('when checksum mismatches', () => {
        Object.keys(checksumedMethods).forEach(method => {
            it(`should return InvalidDigest error for ${method} when checksum mismatch`, async () => {
                const body = 'Hello, World!';
                const wrongMd5 = 'wrongchecksum123=';
                const request = {
                    apiMethod: method,
                    headers: {
                        'content-md5': wrongMd5,
                    },
                };
                const log = new DummyRequestLogger();

                const result = await validateMethodChecksumNoChunking(request, body, log);

                assert.deepStrictEqual(result, ArsenalErrors.InvalidDigest, 'Expected BadDigest error');
            });
        });
    });

    describe('when no checksum is provided', () => {
        Object.keys(checksumedMethods).forEach(method => {
            it(`should return null for ${method} when no checksum is provided`, async () => {
                const body = 'Hello, World!';
                const request = {
                    apiMethod: method,
                    headers: {},
                };
                const log = new DummyRequestLogger();

                const result = await validateMethodChecksumNoChunking(request, body, log);

                assert.ifError(result);
            });
        });
    });

    describe('when checksum matches', () => {
        Object.keys(checksumedMethods).forEach(method => {
            it(`should return null for ${method} when checksum matches`, async () => {
                const body = 'Hello, World!';
                const correctMd5 = crypto.createHash('md5').update(body, 'utf8').digest('base64');
                const request = {
                    apiMethod: method,
                    headers: {
                        'content-md5': correctMd5,
                    },
                };
                const log = new DummyRequestLogger();

                const result = await validateMethodChecksumNoChunking(request, body, log);

                assert.ifError(result);
            });
        });
    });

    describe('when method is not in validation function mapping', () => {
        it('should return null for unsupported method', async () => {
            const unsupportedMethod = 'someUnsupportedMethod';

            const body = 'Hello, World!';
            const wrongMd5 = 'wrongchecksum123=';
            const request = {
                apiMethod: unsupportedMethod,
                headers: {
                    'content-md5': wrongMd5,
                },
            };
            const log = new DummyRequestLogger();

            const result = await validateMethodChecksumNoChunking(request, body, log);

            assert.ifError(result);
        });
    });

    describe('edge cases', () => {
        it('should return null when request has no apiMethod', async () => {
            const body = 'Hello, World!';
            const request = {
                headers: {
                    'content-md5': 'wrongchecksum123=',
                },
            };
            const log = new DummyRequestLogger();

            const result = await validateMethodChecksumNoChunking(request, body, log);

            assert.ifError(result);
        });

        it('should return null when request apiMethod is undefined in config', async () => {
            const body = 'Hello, World!';
            const request = {
                apiMethod: 'nonExistentMethod',
                headers: {
                    'content-md5': 'wrongchecksum123=',
                },
            };
            const log = new DummyRequestLogger();

            const result = await validateMethodChecksumNoChunking(request, body, log);

            assert.ifError(result);
        });
    });

    // CompleteMPU is not in `checksumedMethods` (x-amz-checksum-* is the
    // final-object checksum, not a body digest) but it still must validate
    // Content-MD5 against the XML body when present.
    describe('completeMultipartUpload (md5-only path)', () => {
        const body = 'Hello, World!';
        const correctMd5 = crypto.createHash('md5').update(body, 'utf8').digest('base64');

        it('should return null when Content-MD5 matches the body', async () => {
            const request = {
                apiMethod: 'completeMultipartUpload',
                headers: { 'content-md5': correctMd5 },
            };
            const log = new DummyRequestLogger();
            const result = await validateMethodChecksumNoChunking(request, body, log);
            assert.ifError(result);
        });

        it('should return BadDigest when Content-MD5 does not match the body', async () => {
            const request = {
                apiMethod: 'completeMultipartUpload',
                headers: { 'content-md5': '1B2M2Y8AsgTpgAmY7PhCfg==' },
            };
            const log = new DummyRequestLogger();
            const result = await validateMethodChecksumNoChunking(request, body, log);
            assert.deepStrictEqual(result, ArsenalErrors.BadDigest);
        });

        it('should return InvalidDigest when Content-MD5 is malformed', async () => {
            const request = {
                apiMethod: 'completeMultipartUpload',
                headers: { 'content-md5': 'wrongchecksum123=' },
            };
            const log = new DummyRequestLogger();
            const result = await validateMethodChecksumNoChunking(request, body, log);
            assert.deepStrictEqual(result, ArsenalErrors.InvalidDigest);
        });

        it('should return null when no Content-MD5 header is present', async () => {
            const request = {
                apiMethod: 'completeMultipartUpload',
                headers: {},
            };
            const log = new DummyRequestLogger();
            const result = await validateMethodChecksumNoChunking(request, body, log);
            assert.ifError(result);
        });

        it('should NOT validate x-amz-checksum-* as a body digest (final-object semantics)', async () => {
            // If we routed CompleteMPU through `defaultValidationFunc` like
            // the other methods, this wrong x-amz-checksum-sha256 (treated
            // as a body digest) would return BadDigest. The md5-only path
            // must ignore it — the final-object validator handles it later.
            const request = {
                apiMethod: 'completeMultipartUpload',
                headers: { 'x-amz-checksum-sha256': 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' },
            };
            const log = new DummyRequestLogger();
            const result = await validateMethodChecksumNoChunking(request, body, log);
            assert.ifError(result);
        });
    });
});

describe('getChecksumDataFromHeaders', () => {
    // Valid-format digests (correct length and base64, content not verified by getChecksumDataFromHeaders)
    const validDigests = {
        crc32: 'AAAAAA==', // 8 chars
        crc32c: 'AAAAAA==', // 8 chars
        sha1: 'AAAAAAAAAAAAAAAAAAAAAAAAAAA=', // 28 chars
        sha256: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=', // 44 chars
        crc64nvme: 'AAAAAAAAAAA=', // 12 chars
    };

    it('should return null when no headers', () => {
        const result = getChecksumDataFromHeaders({});
        assert.ifError(result);
    });

    it('should return null when no checksum headers, no trailer, no sdk algo', () => {
        const result = getChecksumDataFromHeaders({ 'content-type': 'application/octet-stream' });
        assert.ifError(result);
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

    it('should ignore x-amz-checksum-algorithm alongside a valid x-amz-checksum-<algo> value', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-checksum-algorithm': 'SHA256',
            'x-amz-checksum-sha256': validDigests.sha256,
        });
        assert.deepStrictEqual(result, { algorithm: 'sha256', isTrailer: false, expected: validDigests.sha256 });
    });

    it('should ignore x-amz-checksum-type alongside a valid x-amz-checksum-<algo> value', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-checksum-type': 'FULL_OBJECT',
            'x-amz-checksum-sha256': validDigests.sha256,
        });
        assert.deepStrictEqual(result, { algorithm: 'sha256', isTrailer: false, expected: validDigests.sha256 });
    });

    it('should ignore both x-amz-checksum-algorithm and x-amz-checksum-type when combined with a value', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-checksum-algorithm': 'SHA256',
            'x-amz-checksum-type': 'FULL_OBJECT',
            'x-amz-checksum-sha256': validDigests.sha256,
        });
        assert.deepStrictEqual(result, { algorithm: 'sha256', isTrailer: false, expected: validDigests.sha256 });
    });

    it('should return MissingCorresponding when x-amz-sdk-checksum-algorithm has no x-amz-checksum- or trailer', () => {
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

    it('should return AlgoNotSupportedSDK on mismatched x-amz-sdk-checksum-algorithm (SHA256 vs CRC32)', () => {
        const result = getChecksumDataFromHeaders({
            'x-amz-checksum-crc32': validDigests.crc32,
            'x-amz-sdk-checksum-algorithm': 'sha256',
        });
        assert.strictEqual(result.error, ChecksumError.AlgoNotSupportedSDK);
        assert.strictEqual(result.details.algorithm, 'sha256');
    });

    it('should return AlgoNotSupportedSDK for non-string x-amz-sdk-checksum-algorithm', () => {
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

    it('should return TrailerNotSupported for x-amz-trailer value not prefixed by x-amz-checksum-', () => {
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
        assert.ifError(result);
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

    it('should return InvalidRequest with the AWS-shaped message for CopyChecksumAlgoNotSupported', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.CopyChecksumAlgoNotSupported,
            details: { algorithm: 'GARBAGE' },
        });
        assert.strictEqual(result.message, 'InvalidRequest');
        assert.match(
            result.description,
            /Checksum algorithm provided is unsupported/,
            'expected AWS-shaped error description',
        );
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
        assert.strictEqual(result.description, 'Value for x-amz-checksum-type header is invalid.');
    });

    it('should return InvalidRequest for MPUTypeWithoutAlgo', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.MPUTypeWithoutAlgo,
            details: { type: 'COMPOSITE' },
        });
        assert.strictEqual(result.message, 'InvalidRequest');
        assert.match(
            result.description,
            /x-amz-checksum-type header can only be used with the x-amz-checksum-algorithm header/,
        );
    });

    it('should return InvalidRequest for MPUInvalidCombination mentioning type and algorithm', () => {
        const result = arsenalErrorFromChecksumError({
            error: ChecksumError.MPUInvalidCombination,
            details: { algorithm: 'sha256', type: 'FULL_OBJECT' },
        });
        assert.strictEqual(result.message, 'InvalidRequest');
        assert.strictEqual(
            result.description,
            'The FULL_OBJECT checksum type cannot be used with the SHA256 checksum algorithm.',
        );
    });
});

describe('getChecksumDataFromMPUHeaders', () => {
    describe('no checksum headers (default)', () => {
        it('should return crc64nvme/FULL_OBJECT with isDefault=true when no headers', () => {
            const result = getChecksumDataFromMPUHeaders({});
            assert.deepStrictEqual(result, {
                algorithm: 'crc64nvme',
                type: 'FULL_OBJECT',
                isDefault: true,
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
                    algorithm: algo,
                    type: expectedType,
                    isDefault: false,
                });
            });
        }

        it('should accept uppercase algorithm (CRC32) and normalize to lowercase', () => {
            const result = getChecksumDataFromMPUHeaders({
                'x-amz-checksum-algorithm': 'CRC32',
            });
            assert.deepStrictEqual(result, {
                algorithm: 'crc32',
                type: 'COMPOSITE',
                isDefault: false,
            });
        });

        it('should accept mixed case algorithm (Sha256) and normalize to lowercase', () => {
            const result = getChecksumDataFromMPUHeaders({
                'x-amz-checksum-algorithm': 'Sha256',
            });
            assert.deepStrictEqual(result, {
                algorithm: 'sha256',
                type: 'COMPOSITE',
                isDefault: false,
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
                    algorithm: algo,
                    type,
                    isDefault: false,
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

describe('validateCompleteMultipartUploadChecksum', () => {
    // Real-length placeholder digests that pass `isValidDigest`.
    const SHA256_A = 'ypeBEsobvcr6wjGzmiPcTaeG7/gUfE5yuYB3ha/uSLs=';
    const SHA256_B = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=';
    const CRC32_A = 'AAAAAA==';

    it('should return null when no x-amz-checksum-<algo> header is present', () => {
        const err = validateCompleteMultipartUploadChecksum(
            { host: 'example.com' },
            { algorithm: 'sha256', type: 'COMPOSITE', value: `${SHA256_A}-3` },
        );
        assert.ifError(err);
    });

    it('should ignore x-amz-checksum-type and x-amz-checksum-algorithm headers', () => {
        const err = validateCompleteMultipartUploadChecksum(
            {
                'x-amz-checksum-type': 'COMPOSITE',
                'x-amz-checksum-algorithm': 'SHA256',
            },
            { algorithm: 'sha256', type: 'COMPOSITE', value: `${SHA256_A}-3` },
        );
        assert.ifError(err);
    });

    it('should return null when COMPOSITE header value matches (digest-N form)', () => {
        const err = validateCompleteMultipartUploadChecksum(
            { 'x-amz-checksum-sha256': `${SHA256_A}-3` },
            { algorithm: 'sha256', type: 'COMPOSITE', value: `${SHA256_A}-3` },
        );
        assert.ifError(err);
    });

    it('should return null when FULL_OBJECT header value matches (no suffix)', () => {
        // Use crc32 (a dual-form algorithm) for the FULL_OBJECT case — sha256
        // is COMPOSITE-only, so a no-suffix sha256 value is shape-malformed
        // regardless of the MPU's `type`.
        const err = validateCompleteMultipartUploadChecksum(
            { 'x-amz-checksum-crc32': CRC32_A },
            { algorithm: 'crc32', type: 'FULL_OBJECT', value: CRC32_A },
        );
        assert.ifError(err);
    });

    it('should return XAmzMismatch when header value differs', () => {
        const err = validateCompleteMultipartUploadChecksum(
            { 'x-amz-checksum-sha256': `${SHA256_B}-3` },
            { algorithm: 'sha256', type: 'COMPOSITE', value: `${SHA256_A}-3` },
        );
        assert(err);
        assert.strictEqual(err.error, ChecksumError.XAmzMismatch);
        assert.strictEqual(err.details.algorithm, 'sha256');
    });

    it('should return XAmzMismatch when header algorithm differs from MPU', () => {
        const err = validateCompleteMultipartUploadChecksum(
            { 'x-amz-checksum-crc32': CRC32_A },
            { algorithm: 'sha256', type: 'COMPOSITE', value: `${SHA256_A}-3` },
        );
        assert(err);
        assert.strictEqual(err.error, ChecksumError.XAmzMismatch);
        assert.strictEqual(err.details.algorithm, 'crc32');
    });

    it('should return XAmzMismatch when header is present but finalChecksum is null', () => {
        // Use a shape-valid value (sha256 is COMPOSITE-only, so it requires
        // the `-N` suffix) so we exercise the finalChecksum=null path
        // rather than the shape-mismatch path.
        const err = validateCompleteMultipartUploadChecksum({ 'x-amz-checksum-sha256': `${SHA256_A}-3` }, null);
        assert(err);
        assert.strictEqual(err.error, ChecksumError.XAmzMismatch);
    });

    it('should return null when finalChecksum is null and no header present', () => {
        const err = validateCompleteMultipartUploadChecksum({ host: 'example.com' }, null);
        assert.ifError(err);
    });

    it('should return MultipleChecksumTypes when multiple x-amz-checksum-* headers are sent', () => {
        const err = validateCompleteMultipartUploadChecksum(
            {
                'x-amz-checksum-sha256': SHA256_A,
                'x-amz-checksum-crc32': CRC32_A,
            },
            { algorithm: 'sha256', type: 'COMPOSITE', value: `${SHA256_A}-3` },
        );
        assert(err);
        assert.strictEqual(err.error, ChecksumError.MultipleChecksumTypes);
        assert.deepStrictEqual(err.details.algorithms.sort(), ['x-amz-checksum-crc32', 'x-amz-checksum-sha256']);
    });

    it('should return MalformedChecksum when the header value is not a valid digest', () => {
        // AWS S3 returns InvalidRequest "Value for x-amz-checksum-sha256
        // header is invalid." for a malformed value (verified us-east-1,
        // 2026-05-13). Falling through to a misleading "did not match"
        // BadDigest would be wrong.
        const err = validateCompleteMultipartUploadChecksum(
            { 'x-amz-checksum-sha256': '!!!not-base64!!!' },
            { algorithm: 'sha256', type: 'COMPOSITE', value: `${SHA256_A}-3` },
        );
        assert(err);
        assert.strictEqual(err.error, ChecksumError.MalformedChecksum);
        assert.strictEqual(err.details.algorithm, 'sha256');
        assert.strictEqual(err.details.expected, '!!!not-base64!!!');
    });

    it('should return MalformedChecksum when the digest-N prefix is the wrong length', () => {
        // 'abc' is valid base64 chars but not a 44-char SHA256 digest.
        const err = validateCompleteMultipartUploadChecksum(
            { 'x-amz-checksum-sha256': 'abc-3' },
            { algorithm: 'sha256', type: 'COMPOSITE', value: `${SHA256_A}-3` },
        );
        assert(err);
        assert.strictEqual(err.error, ChecksumError.MalformedChecksum);
    });

    it('should return MalformedChecksum when a FULL_OBJECT-only algorithm carries a `-N` suffix', () => {
        // crc64nvme only exists as FULL_OBJECT — a `<digest>-N` value is
        // shape-malformed, not a mismatch. Without the shape check the
        // suffix would be silently stripped and the digest would validate,
        // causing this to fall through as XAmzMismatch.
        const CRC64NVME_VALID = 'AAAAAAAAAAAA'; // 12 chars
        const err = validateCompleteMultipartUploadChecksum(
            { 'x-amz-checksum-crc64nvme': `${CRC64NVME_VALID}-5` },
            { algorithm: 'crc64nvme', type: 'FULL_OBJECT', value: CRC64NVME_VALID },
        );
        assert(err);
        assert.strictEqual(err.error, ChecksumError.MalformedChecksum);
        assert.strictEqual(err.details.algorithm, 'crc64nvme');
        assert.strictEqual(err.details.expected, `${CRC64NVME_VALID}-5`);
    });

    it('should return MalformedChecksum when a COMPOSITE-only algorithm lacks the `-N` suffix', () => {
        // sha1 only exists as COMPOSITE — a bare `<digest>` value is
        // shape-malformed.
        const SHA1_VALID = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAA'; // 28 chars
        const err = validateCompleteMultipartUploadChecksum(
            { 'x-amz-checksum-sha1': SHA1_VALID },
            { algorithm: 'sha1', type: 'COMPOSITE', value: `${SHA1_VALID}-3` },
        );
        assert(err);
        assert.strictEqual(err.error, ChecksumError.MalformedChecksum);
        assert.strictEqual(err.details.algorithm, 'sha1');
    });

    it('should return XAmzMismatch (not MalformedChecksum) when a dual-form algorithm sends the wrong shape', () => {
        // crc32 supports both FULL_OBJECT and COMPOSITE, so `<digest>-N`
        // against a FULL_OBJECT MPU is shape-valid; the type mismatch
        // is a regular value mismatch, not malformed.
        const err = validateCompleteMultipartUploadChecksum(
            { 'x-amz-checksum-crc32': `${CRC32_A}-3` },
            { algorithm: 'crc32', type: 'FULL_OBJECT', value: CRC32_A },
        );
        assert(err);
        assert.strictEqual(err.error, ChecksumError.XAmzMismatch);
    });

    it('should return XAmzMismatch (not MalformedChecksum) when a dual-form algorithm omits a required suffix', () => {
        // Symmetric: bare `<digest>` against a COMPOSITE crc32 MPU is
        // shape-valid (crc32 also supports FULL_OBJECT) but value-mismatched.
        const err = validateCompleteMultipartUploadChecksum(
            { 'x-amz-checksum-crc32': CRC32_A },
            { algorithm: 'crc32', type: 'COMPOSITE', value: `${CRC32_A}-3` },
        );
        assert(err);
        assert.strictEqual(err.error, ChecksumError.XAmzMismatch);
    });

    it('should return AlgoNotSupported when x-amz-checksum-<algo> uses an unsupported algorithm', () => {
        // Must scan ALL x-amz-checksum-* headers, not just the supported
        // algorithms — otherwise a bogus header is silently ignored and the
        // request proceeds.
        const err = validateCompleteMultipartUploadChecksum(
            { 'x-amz-checksum-bad': 'anything' },
            { algorithm: 'sha256', type: 'COMPOSITE', value: `${SHA256_A}-3` },
        );
        assert(err);
        assert.strictEqual(err.error, ChecksumError.AlgoNotSupported);
        assert.strictEqual(err.details.algorithm, 'bad');
    });

    it('should reject an unsupported x-amz-checksum-<algo> header even when finalChecksum is null', () => {
        const err = validateCompleteMultipartUploadChecksum({ 'x-amz-checksum-md5': 'anything' }, null);
        assert(err);
        assert.strictEqual(err.error, ChecksumError.AlgoNotSupported);
        assert.strictEqual(err.details.algorithm, 'md5');
    });
});

describe('getCopyObjectChecksumAlgorithm', () => {
    it('should return algorithm null and no error when the header is absent', () => {
        const result = getCopyObjectChecksumAlgorithm({});
        assert.strictEqual(result.error, null);
        assert.strictEqual(result.algorithm, null);
    });

    it('should ignore unrelated headers', () => {
        const result = getCopyObjectChecksumAlgorithm({
            'content-type': 'application/octet-stream',
            host: 'example.com',
        });
        assert.strictEqual(result.error, null);
        assert.strictEqual(result.algorithm, null);
    });

    const validAlgorithms = [
        ['CRC32', 'crc32'],
        ['CRC32C', 'crc32c'],
        ['CRC64NVME', 'crc64nvme'],
        ['SHA1', 'sha1'],
        ['SHA256', 'sha256'],
        // mixed-case input is lowercased
        ['Sha256', 'sha256'],
        // already-lowercase input is accepted as-is
        ['crc32', 'crc32'],
    ];

    for (const [header, normalized] of validAlgorithms) {
        it(`should return algorithm '${normalized}' for header '${header}'`, () => {
            const result = getCopyObjectChecksumAlgorithm({
                'x-amz-checksum-algorithm': header,
            });
            assert.strictEqual(result.error, null);
            assert.strictEqual(result.algorithm, normalized);
        });
    }

    it('should return CopyChecksumAlgoNotSupported for an unknown algorithm', () => {
        const result = getCopyObjectChecksumAlgorithm({
            'x-amz-checksum-algorithm': 'GARBAGE',
        });
        assert.strictEqual(result.algorithm, null);
        assert(result.error);
        assert.strictEqual(result.error.error, ChecksumError.CopyChecksumAlgoNotSupported);
        assert.strictEqual(result.error.details.algorithm, 'GARBAGE');
    });

    it('should reject algorithms AWS may know about but cloudserver does not support', () => {
        // AWS error message lists MD5/SHA512/XXHASH* as known names; we only
        // accept the five FULL_OBJECT-capable algorithms.
        for (const algo of ['MD5', 'SHA512', 'XXHASH128', 'XXHASH3', 'XXHASH64']) {
            const result = getCopyObjectChecksumAlgorithm({
                'x-amz-checksum-algorithm': algo,
            });
            assert.strictEqual(result.algorithm, null, `expected null algorithm for ${algo}`);
            assert.strictEqual(result.error.error, ChecksumError.CopyChecksumAlgoNotSupported);
        }
    });
});

const sigV4Auth =
    'AWS4-HMAC-SHA256 Credential=AK/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc';

describe('parseContentSHA256', () => {
    // build SigV4 header-auth headers carrying the given x-amz-content-sha256
    const v4 = value => ({ authorization: sigV4Auth, 'x-amz-content-sha256': value });

    it('should return Skip for non-SigV4 header auth, capturing the value', () => {
        assert.deepStrictEqual(parseContentSHA256({ authorization: 'AWS AKID:sig', 'x-amz-content-sha256': 'abc' }), {
            type: ContentSHA256Type.Skip,
            value: 'abc',
        });
    });

    it('should return Skip with null value when there is no auth header', () => {
        assert.deepStrictEqual(parseContentSHA256({}), { type: ContentSHA256Type.Skip, value: null });
    });

    it('should return Absent when SigV4 header auth but the header is missing', () => {
        assert.deepStrictEqual(parseContentSHA256({ authorization: sigV4Auth }), {
            type: ContentSHA256Type.Absent,
            value: null,
        });
    });

    it('should return Unsigned for UNSIGNED-PAYLOAD', () => {
        assert.deepStrictEqual(parseContentSHA256(v4('UNSIGNED-PAYLOAD')), {
            type: ContentSHA256Type.Unsigned,
            value: 'UNSIGNED-PAYLOAD',
        });
    });

    it('should return Streaming (supported) for a supported streaming token', () => {
        const tok = 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD';
        assert.deepStrictEqual(parseContentSHA256(v4(tok)), {
            type: ContentSHA256Type.Streaming,
            value: tok,
            supported: true,
        });
    });

    it('should return Streaming (not supported) for an unsupported streaming token', () => {
        const tok = 'STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD';
        assert.deepStrictEqual(parseContentSHA256(v4(tok)), {
            type: ContentSHA256Type.Streaming,
            value: tok,
            supported: false,
        });
    });

    it('should return HexSHA256 for a hex sha256, preserving the raw value', () => {
        const hex = 'a'.repeat(64);
        assert.deepStrictEqual(parseContentSHA256(v4(hex)), { type: ContentSHA256Type.HexSHA256, value: hex });
    });

    it('should return HexSHA256 for an uppercase hex sha256', () => {
        const hex = 'A'.repeat(64);
        assert.deepStrictEqual(parseContentSHA256(v4(hex)), { type: ContentSHA256Type.HexSHA256, value: hex });
    });

    it('should return Invalid for a malformed value', () => {
        assert.deepStrictEqual(parseContentSHA256(v4('xxx')), { type: ContentSHA256Type.Invalid, value: 'xxx' });
    });
});

describe('validateXAmzContentSHA256', () => {
    const body = 'Hello, World!';
    const correctHex = crypto.createHash('sha256').update(body).digest('hex');
    const wrongHex = crypto.createHash('sha256').update('other').digest('hex');

    it('should return null when the hash matches the body', () => {
        assert.ifError(
            validateXAmzContentSHA256({ authorization: sigV4Auth, 'x-amz-content-sha256': correctHex }, body),
        );
    });

    it('should return null for an uppercase hash that matches (case-insensitive)', () => {
        assert.ifError(
            validateXAmzContentSHA256(
                { authorization: sigV4Auth, 'x-amz-content-sha256': correctHex.toUpperCase() },
                body,
            ),
        );
    });

    it('should return ContentSHA256Mismatch with calculated/expected details on mismatch', () => {
        const result = validateXAmzContentSHA256({ authorization: sigV4Auth, 'x-amz-content-sha256': wrongHex }, body);
        assert.strictEqual(result.error, ChecksumError.ContentSHA256Mismatch);
        assert.strictEqual(result.details.expected, wrongHex);
        assert.strictEqual(result.details.calculated, correctHex);
    });

    it('should return ContentSHA256Missing when the header is absent', () => {
        assert.deepStrictEqual(validateXAmzContentSHA256({ authorization: sigV4Auth }, body), {
            error: ChecksumError.ContentSHA256Missing,
        });
    });

    it('should return ContentSHA256Invalid for a malformed value', () => {
        const result = validateXAmzContentSHA256({ authorization: sigV4Auth, 'x-amz-content-sha256': 'xxx' }, body);
        assert.strictEqual(result.error, ChecksumError.ContentSHA256Invalid);
        assert.strictEqual(result.details.value, 'xxx');
    });

    it('should return null for UNSIGNED-PAYLOAD', () => {
        assert.ifError(
            validateXAmzContentSHA256({ authorization: sigV4Auth, 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD' }, body),
        );
    });

    it('should return null for a streaming token', () => {
        assert.ifError(
            validateXAmzContentSHA256(
                { authorization: sigV4Auth, 'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD' },
                body,
            ),
        );
    });

    it('should return null (skip) for non-SigV4 auth even with a wrong hash', () => {
        assert.ifError(
            validateXAmzContentSHA256({ authorization: 'AWS AKID:sig', 'x-amz-content-sha256': wrongHex }, body),
        );
    });

    it('should return null when the hash matches an empty body', () => {
        const emptyHex = crypto.createHash('sha256').update(Buffer.alloc(0)).digest('hex');
        assert.ifError(
            validateXAmzContentSHA256({ authorization: sigV4Auth, 'x-amz-content-sha256': emptyHex }, Buffer.alloc(0)),
        );
    });

    it('should return ContentSHA256Mismatch for an empty body with a wrong hash', () => {
        const result = validateXAmzContentSHA256(
            { authorization: sigV4Auth, 'x-amz-content-sha256': wrongHex },
            Buffer.alloc(0),
        );
        assert.strictEqual(result.error, ChecksumError.ContentSHA256Mismatch);
        assert.strictEqual(
            result.details.calculated,
            crypto.createHash('sha256').update(Buffer.alloc(0)).digest('hex'),
        );
    });

    describe('mapped through arsenalErrorFromChecksumError', () => {
        it('should map mismatch to XAmzContentSHA256Mismatch (400)', () => {
            const result = validateXAmzContentSHA256(
                { authorization: sigV4Auth, 'x-amz-content-sha256': wrongHex },
                body,
            );
            const err = arsenalErrorFromChecksumError(result);
            assert.strictEqual(err.message, 'XAmzContentSHA256Mismatch');
            assert.strictEqual(err.code, 400);
        });

        it('should map invalid to InvalidArgument (400)', () => {
            const result = validateXAmzContentSHA256({ authorization: sigV4Auth, 'x-amz-content-sha256': 'xxx' }, body);
            const err = arsenalErrorFromChecksumError(result);
            assert.strictEqual(err.message, 'InvalidArgument');
            assert.strictEqual(err.code, 400);
        });

        it('should map missing to InvalidRequest (400)', () => {
            const result = validateXAmzContentSHA256({ authorization: sigV4Auth }, body);
            const err = arsenalErrorFromChecksumError(result);
            assert.strictEqual(err.message, 'InvalidRequest');
            assert.strictEqual(err.code, 400);
        });
    });
});

describe('validateMethodChecksumNoChunking x-amz-content-sha256', () => {
    const body = 'Hello, World!';
    const correctHex = crypto.createHash('sha256').update(body).digest('hex');
    const correctMd5 = crypto.createHash('md5').update(body).digest('base64');

    it('should reject a wrong x-amz-content-sha256 with XAmzContentSHA256Mismatch', async () => {
        const wrongHex = crypto.createHash('sha256').update('other').digest('hex');
        const request = {
            apiMethod: 'bucketPutCors',
            headers: { authorization: sigV4Auth, 'x-amz-content-sha256': wrongHex },
        };
        const result = await validateMethodChecksumNoChunking(request, body, new DummyRequestLogger());
        assert.strictEqual(result.message, 'XAmzContentSHA256Mismatch');
        assert.strictEqual(result.code, 400);
    });

    it('should accept a matching x-amz-content-sha256', async () => {
        const request = {
            apiMethod: 'bucketPutCors',
            headers: { authorization: sigV4Auth, 'x-amz-content-sha256': correctHex, 'content-md5': correctMd5 },
        };
        const result = await validateMethodChecksumNoChunking(request, body, new DummyRequestLogger());
        assert.ifError(result);
    });
});

describe('validateCompleteMPUChecksumType', () => {
    describe('when the header is absent', () => {
        it('should return null whatever the MPU checksum type is', () => {
            assert.strictEqual(validateCompleteMPUChecksumType({}, 'COMPOSITE'), null);
            assert.strictEqual(validateCompleteMPUChecksumType({}, 'FULL_OBJECT'), null);
            assert.strictEqual(validateCompleteMPUChecksumType({}, undefined), null);
        });

        it('should return null for an empty header value', () => {
            assert.strictEqual(validateCompleteMPUChecksumType({ 'x-amz-checksum-type': '' }, 'COMPOSITE'), null);
        });
    });

    describe('when the header matches the MPU checksum type', () => {
        ['COMPOSITE', 'FULL_OBJECT'].forEach(type => {
            it(`should return null for ${type}`, () => {
                assert.strictEqual(validateCompleteMPUChecksumType({ 'x-amz-checksum-type': type }, type), null);
            });
        });

        it('should compare case-insensitively on both sides', () => {
            const lowerHeader = { 'x-amz-checksum-type': 'composite' };
            assert.strictEqual(validateCompleteMPUChecksumType(lowerHeader, 'COMPOSITE'), null);
            const upperHeader = { 'x-amz-checksum-type': 'FULL_OBJECT' };
            assert.strictEqual(validateCompleteMPUChecksumType(upperHeader, 'full_object'), null);
        });
    });

    describe('when the header value is not a valid checksum type', () => {
        it('should return MPUTypeInvalid', () => {
            const result = validateCompleteMPUChecksumType({ 'x-amz-checksum-type': 'BOGUS' }, 'COMPOSITE');
            assert.strictEqual(result.error, ChecksumError.MPUTypeInvalid);
            assert.strictEqual(result.details.type, 'BOGUS');
        });

        it('should take precedence over an unset MPU checksum type', () => {
            const result = validateCompleteMPUChecksumType({ 'x-amz-checksum-type': 'BOGUS' }, undefined);
            assert.strictEqual(result.error, ChecksumError.MPUTypeInvalid);
        });

        it('should map to InvalidRequest (400)', () => {
            const result = validateCompleteMPUChecksumType({ 'x-amz-checksum-type': 'BOGUS' }, 'COMPOSITE');
            const err = arsenalErrorFromChecksumError(result);
            assert.strictEqual(err.message, 'InvalidRequest');
            assert.strictEqual(err.code, 400);
            assert.strictEqual(err.description, 'Value for x-amz-checksum-type header is invalid.');
        });
    });

    describe('when the MPU was created without a checksum type', () => {
        it('should return MPUTypeNotConfigured', () => {
            const result = validateCompleteMPUChecksumType({ 'x-amz-checksum-type': 'COMPOSITE' }, undefined);
            assert.strictEqual(result.error, ChecksumError.MPUTypeNotConfigured);
        });

        it('should map to InvalidRequest (400) describing the legacy upload', () => {
            const result = validateCompleteMPUChecksumType({ 'x-amz-checksum-type': 'COMPOSITE' }, undefined);
            const err = arsenalErrorFromChecksumError(result);
            assert.strictEqual(err.message, 'InvalidRequest');
            assert.strictEqual(err.code, 400);
            assert.strictEqual(
                err.description,
                'The upload was not created with a checksum mode. ' +
                    'The complete request must not include a x-amz-checksum-type header.',
            );
        });

        it('should ignore the header for an external-backend MPU', () => {
            // External backends record no checksum config (CLDSRV-964), so an
            // absent type means "not tracked here", not a legacy upload.
            const headers = { 'x-amz-checksum-type': 'COMPOSITE' };
            assert.strictEqual(validateCompleteMPUChecksumType(headers, undefined, true), null);
        });

        it('should still reject a mismatch on an external-backend MPU that has a type', () => {
            const headers = { 'x-amz-checksum-type': 'COMPOSITE' };
            const result = validateCompleteMPUChecksumType(headers, 'FULL_OBJECT', true);
            assert.strictEqual(result.error, ChecksumError.MPUTypeModeMismatch);
        });
    });

    describe('when the header does not match the MPU checksum type', () => {
        it('should return MPUTypeModeMismatch carrying the MPU type', () => {
            const result = validateCompleteMPUChecksumType({ 'x-amz-checksum-type': 'COMPOSITE' }, 'FULL_OBJECT');
            assert.strictEqual(result.error, ChecksumError.MPUTypeModeMismatch);
            assert.strictEqual(result.details.type, 'FULL_OBJECT');
        });

        it('should map to InvalidRequest (400) naming the mode the MPU was created with', () => {
            const result = validateCompleteMPUChecksumType({ 'x-amz-checksum-type': 'COMPOSITE' }, 'FULL_OBJECT');
            const err = arsenalErrorFromChecksumError(result);
            assert.strictEqual(err.message, 'InvalidRequest');
            assert.strictEqual(err.code, 400);
            assert.strictEqual(
                err.description,
                'The upload was created using the FULL_OBJECT checksum mode. ' +
                    'The complete request must use the same checksum mode.',
            );
        });
    });
});

describe('areChecksumsEnabled', () => {
    let originalIntegrityChecks;

    beforeEach(() => {
        originalIntegrityChecks = config.integrityChecks;
    });

    afterEach(() => {
        config.integrityChecks = originalIntegrityChecks;
    });

    it('should be enabled with the shipped config', () => {
        assert.strictEqual(areChecksumsEnabled(), true);
    });

    it('should be disabled only when enabled is exactly false', () => {
        config.integrityChecks = { enabled: false };
        assert.strictEqual(areChecksumsEnabled(), false);
    });

    it('should be enabled when enabled is true', () => {
        config.integrityChecks = { enabled: true };
        assert.strictEqual(areChecksumsEnabled(), true);
    });

    it('should default to enabled when the section or the key is missing', () => {
        config.integrityChecks = undefined;
        assert.strictEqual(areChecksumsEnabled(), true);
        config.integrityChecks = {};
        assert.strictEqual(areChecksumsEnabled(), true);
    });

    it('should not treat a falsy non-false value as disabled', () => {
        // Guards the `!== false` comparison: only an explicit boolean false
        // turns checksums off, so a mis-typed config cannot silently disable them.
        [0, '', null, 'false'].forEach(value => {
            config.integrityChecks = { enabled: value };
            assert.strictEqual(areChecksumsEnabled(), true, `enabled: ${JSON.stringify(value)}`);
        });
    });
});
