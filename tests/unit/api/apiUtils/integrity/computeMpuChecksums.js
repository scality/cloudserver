const assert = require('assert');
const crypto = require('crypto');

const {
    algorithms,
    computeCompositeMPUChecksum,
    computeFullObjectMPUChecksum,
} = require('../../../../../lib/api/apiUtils/integrity/validateChecksums');

// Random part bodies. Per-test randomness still satisfies the assertions
// because each test only checks combine(parts) === algo(concat(parts)),
// which holds for any byte sequence.
function makeParts(count, size) {
    const parts = [];
    for (let i = 0; i < count; i += 1) {
        parts.push(crypto.randomBytes(size));
    }
    return parts;
}

// -- COMPOSITE ------------------------------------------------------------

describe('computeCompositeMPUChecksum', () => {
    const parts = makeParts(3, 1024);

    const COMPOSITE_ALGOS = ['crc32', 'crc32c', 'sha1', 'sha256'];

    COMPOSITE_ALGOS.forEach(algo => {
        const label = algo.toUpperCase();
        it(`should match ${label}(decode(c1) || ... || decode(cN)) + "-N"`, () => {
            const partChecksums = parts.map(p => algorithms[algo].digest(p));
            const expectedConcat = Buffer.concat(partChecksums.map(c => Buffer.from(c, 'base64')));
            const expected = `${algorithms[algo].digest(expectedConcat)}-3`;

            const got = computeCompositeMPUChecksum(algo, partChecksums);
            assert.strictEqual(got.error, null);
            assert.strictEqual(got.checksum, expected);
        });
    });

    it('should return N=1 for a single part', () => {
        const partChecksums = [algorithms.sha256.digest(parts[0])];
        const got = computeCompositeMPUChecksum('sha256', partChecksums);
        assert.strictEqual(got.error, null);
        assert(got.checksum.endsWith('-1'));
    });

    it('should return an error object on unsupported algorithm', () => {
        const got = computeCompositeMPUChecksum('md5', ['AAAA']);
        assert.strictEqual(got.checksum, null);
        assert(got.error);
        assert.strictEqual(got.error.code, 'MPUAlgoNotSupported');
        assert.deepStrictEqual(got.error.details, { algorithm: 'md5' });
    });

    it('should return an error object for crc64nvme (not allowed for COMPOSITE)', () => {
        const got = computeCompositeMPUChecksum('crc64nvme', ['AQIDBAUGBwg=']);
        assert.strictEqual(got.checksum, null);
        assert.strictEqual(got.error.code, 'MPUAlgoNotSupported');
    });
});

// -- FULL_OBJECT ----------------------------------------------------------

describe('computeFullObjectMPUChecksum', () => {
    // Validation strategy: build N concrete part bodies, run each through the
    // canonical CRC implementation to get the per-part CRC, then compare the
    // combined result against the CRC of the concatenation of all bodies.

    const FULL_OBJECT_ALGOS = ['crc32', 'crc32c', 'crc64nvme'];

    async function buildPartInputs(parts, algo) {
        const partInputs = [];
        for (const b of parts) {
            // `await` is a no-op for the sync CRC32/CRC32C digests and resolves
            // the Promise for the async CRC64NVME digest.
            partInputs.push({
                value: await algorithms[algo].digest(b),
                length: b.length,
            });
        }
        return partInputs;
    }

    FULL_OBJECT_ALGOS.forEach(algo => {
        const label = algo.toUpperCase();

        it(`should match ${label}(concat(parts)) for varied part sizes`, async () => {
            const parts = [
                crypto.randomBytes(5 * 1024 * 1024),
                crypto.randomBytes(5 * 1024 * 1024 + 7),
                crypto.randomBytes(19),
            ];
            const partInputs = await buildPartInputs(parts, algo);
            const result = computeFullObjectMPUChecksum(algo, partInputs);
            const direct = await algorithms[algo].digest(Buffer.concat(parts));
            assert.strictEqual(result.error, null);
            assert.strictEqual(result.checksum, direct);
        });

        it(`should return the part CRC unchanged for a single-part ${label} MPU`, async () => {
            const buf = crypto.randomBytes(15);
            const partCrc = await algorithms[algo].digest(buf);
            const got = computeFullObjectMPUChecksum(algo, [
                {
                    value: partCrc,
                    length: buf.length,
                },
            ]);
            assert.strictEqual(got.error, null);
            assert.strictEqual(got.checksum, partCrc);
        });

        it(`should handle many small ${label} parts (16 × 1 MiB)`, async () => {
            // Exercises multiple combine iterations and the matrix-squaring loop.
            const parts = makeParts(16, 1 * 1024 * 1024);
            const partInputs = await buildPartInputs(parts, algo);
            const result = computeFullObjectMPUChecksum(algo, partInputs);
            const direct = await algorithms[algo].digest(Buffer.concat(parts));
            assert.strictEqual(result.error, null);
            assert.strictEqual(result.checksum, direct);
        });
    });

    it('should return an error object on unsupported algorithm', () => {
        const got = computeFullObjectMPUChecksum('sha256', [{ value: 'AAAA', length: 4 }]);
        assert.strictEqual(got.checksum, null);
        assert(got.error);
        assert.strictEqual(got.error.code, 'MPUAlgoNotSupported');
        assert.deepStrictEqual(got.error.details, { algorithm: 'sha256' });
    });

    it('should handle 10000 CRC64NVME parts of uniform 5 MiB (cache hits)', async function f() {
        // 10 000 parts is the AWS MPU max; CRC64NVME has the largest
        // (64-bit) combine matrix. Validates correctness against the CRC
        // of the equivalent 50 GiB object, computed by streaming the same
        // chunk through CrtCrc64Nvme without materializing the object.
        this.timeout(120000);

        const partLen = 5 * 1024 * 1024;
        const nParts = 10000;
        const chunk = crypto.randomBytes(partLen);
        const partCrc = await algorithms.crc64nvme.digest(chunk);

        const parts = new Array(nParts);
        for (let i = 0; i < nParts; i += 1) {
            parts[i] = { value: partCrc, length: partLen };
        }

        const got = computeFullObjectMPUChecksum('crc64nvme', parts);
        assert.strictEqual(got.error, null);

        const ref = algorithms.crc64nvme.createHash();
        for (let i = 0; i < nParts; i += 1) {
            ref.update(chunk);
        }
        const expected = await algorithms.crc64nvme.digestFromHash(ref);
        assert.strictEqual(got.checksum, expected);
    });

    it('should handle 10000 CRC64NVME parts of distinct lengths (cache misses)', async function f() {
        // Every part has a strictly different length, so each combine call
        // touches a different mix of `len2` bit positions. Validates
        // correctness against a streaming reference over independently
        // generated part bodies.
        this.timeout(60000);

        const baseLen = 64 * 1024;
        const nParts = 10000;
        const parts = new Array(nParts);
        const ref = algorithms.crc64nvme.createHash();
        for (let i = 0; i < nParts; i += 1) {
            const len = baseLen + i;
            const buf = crypto.randomBytes(len);
            parts[i] = {
                value: await algorithms.crc64nvme.digest(buf),
                length: len,
            };
            ref.update(buf);
        }

        const got = computeFullObjectMPUChecksum('crc64nvme', parts);
        assert.strictEqual(got.error, null);

        const expected = await algorithms.crc64nvme.digestFromHash(ref);
        assert.strictEqual(got.checksum, expected);
    });
});
