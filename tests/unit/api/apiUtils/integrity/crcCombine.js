const assert = require('assert');
const crypto = require('crypto');

const { crcCombine, combineCrcs } = require('../../../../../lib/api/apiUtils/integrity/crcCombine');
const { algorithms } = require('../../../../../lib/api/apiUtils/integrity/validateChecksums');

// Reversed polynomial + bit width for each algorithm we use the combine
// routine with. Same values that validateChecksums.js feeds in.
const SPECS = [
    { algo: 'crc32', polyReversed: 0xedb88320n, dim: 32 },
    { algo: 'crc32c', polyReversed: 0x82f63b78n, dim: 32 },
    { algo: 'crc64nvme', polyReversed: 0x9a6c9329ac4bc9b5n, dim: 64 },
];

function base64ToBigInt(b64) {
    const buf = Buffer.from(b64, 'base64');
    let r = 0n;
    for (let i = 0; i < buf.length; i += 1) {
        r = (r << 8n) | BigInt(buf[i]);
    }
    return r;
}

async function crcOf(algo, buf) {
    return base64ToBigInt(await algorithms[algo].digest(buf));
}

describe('crcCombine', () => {
    SPECS.forEach(({ algo, polyReversed, dim }) => {
        const label = algo.toUpperCase();
        const mask = (1n << BigInt(dim)) - 1n;

        describe(`${label} (dim=${dim})`, () => {
            it('should combine(crc1, crc2, len2) to crc(chunk1 ‖ chunk2) for random data', async () => {
                const a = crypto.randomBytes(1024);
                const b = crypto.randomBytes(1024);
                const crc1 = await crcOf(algo, a);
                const crc2 = await crcOf(algo, b);
                const got = crcCombine(crc1, crc2, BigInt(b.length), polyReversed, dim);
                const expected = await crcOf(algo, Buffer.concat([a, b]));
                assert.strictEqual(got, expected);
            });

            it('should return crc1 unchanged when len2 = 0 (identity)', async () => {
                const a = crypto.randomBytes(64);
                const crc1 = await crcOf(algo, a);
                const got = crcCombine(crc1, 0n, 0n, polyReversed, dim);
                assert.strictEqual(got, crc1 & mask);
            });

            it('should equal the original CRC when combined with the CRC of empty', async () => {
                // CRC of an empty chunk under the AWS implementations is 0.
                const a = crypto.randomBytes(128);
                const crc1 = await crcOf(algo, a);
                const crcEmpty = await crcOf(algo, Buffer.alloc(0));
                const got = crcCombine(crc1, crcEmpty, 0n, polyReversed, dim);
                assert.strictEqual(got, crc1 & mask);
            });

            it('should mask the result to `dim` bits', async () => {
                const a = crypto.randomBytes(256);
                const b = crypto.randomBytes(256);
                const got = crcCombine(await crcOf(algo, a), await crcOf(algo, b), BigInt(b.length), polyReversed, dim);
                assert.strictEqual(got & mask, got);
                assert.strictEqual(got >> BigInt(dim), 0n);
            });

            it('should be associative across three chunks', async () => {
                const a = crypto.randomBytes(300);
                const b = crypto.randomBytes(400);
                const c = crypto.randomBytes(500);
                const crcA = await crcOf(algo, a);
                const crcB = await crcOf(algo, b);
                const crcC = await crcOf(algo, c);

                // Left-fold: combine(combine(A,B), C)
                const ab = crcCombine(crcA, crcB, BigInt(b.length), polyReversed, dim);
                const left = crcCombine(ab, crcC, BigInt(c.length), polyReversed, dim);

                // Right-fold: combine(A, combine(B, C), len(B)+len(C))
                const bc = crcCombine(crcB, crcC, BigInt(c.length), polyReversed, dim);
                const right = crcCombine(crcA, bc, BigInt(b.length + c.length), polyReversed, dim);

                assert.strictEqual(left, right);
                const expected = await crcOf(algo, Buffer.concat([a, b, c]));
                assert.strictEqual(left, expected);
            });

            it('should handle single-byte chunks', async () => {
                const a = crypto.randomBytes(1);
                const b = crypto.randomBytes(1);
                const got = crcCombine(await crcOf(algo, a), await crcOf(algo, b), 1n, polyReversed, dim);
                const expected = await crcOf(algo, Buffer.concat([a, b]));
                assert.strictEqual(got, expected);
            });

            it('should handle odd-length chunk2 sizes (not a multiple of 8 bytes)', async () => {
                // Sizes chosen to exercise the matrix-squaring loop's
                // odd/even alternation through both branches.
                const sizes = [1, 7, 15, 33, 257, 1023, 65537];
                const a = crypto.randomBytes(64);
                const crcA = await crcOf(algo, a);
                for (const size of sizes) {
                    const b = crypto.randomBytes(size);
                    const got = crcCombine(crcA, await crcOf(algo, b), BigInt(size), polyReversed, dim);
                    const expected = await crcOf(algo, Buffer.concat([a, b]));
                    assert.strictEqual(got, expected, `failed at size=${size}`);
                }
            });
        });
    });
});

describe('combineCrcs', () => {
    SPECS.forEach(({ algo, polyReversed, dim }) => {
        const label = algo.toUpperCase();

        describe(`${label} (dim=${dim})`, () => {
            it('should return the part CRC unchanged for a single-part input', async () => {
                const buf = crypto.randomBytes(13);
                const partCrc = await algorithms[algo].digest(buf);
                const got = combineCrcs([{ value: partCrc, length: buf.length }], polyReversed, dim);
                assert.strictEqual(got, partCrc);
            });

            it('should match crc(concat) for two parts — base64 in, base64 out', async () => {
                const a = crypto.randomBytes(1024);
                const b = crypto.randomBytes(2048);
                const parts = [
                    { value: await algorithms[algo].digest(a), length: a.length },
                    { value: await algorithms[algo].digest(b), length: b.length },
                ];
                const got = combineCrcs(parts, polyReversed, dim);
                const expected = await algorithms[algo].digest(Buffer.concat([a, b]));
                assert.strictEqual(got, expected);
            });

            it('should match crc(concat) for N parts of varied sizes', async () => {
                const bufs = [
                    crypto.randomBytes(7),
                    crypto.randomBytes(513),
                    crypto.randomBytes(1024),
                    crypto.randomBytes(2049),
                    crypto.randomBytes(64),
                ];
                const parts = [];
                for (const buf of bufs) {
                    parts.push({
                        value: await algorithms[algo].digest(buf),
                        length: buf.length,
                    });
                }
                const got = combineCrcs(parts, polyReversed, dim);
                const expected = await algorithms[algo].digest(Buffer.concat(bufs));
                assert.strictEqual(got, expected);
            });
        });
    });
});
