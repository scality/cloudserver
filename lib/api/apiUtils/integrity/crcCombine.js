// Combine two right-shift CRCs (zlib's gf2_matrix_* trick) without using BigInt
// inside the hot loops. Each GF(2) operator matrix is stored as a Uint32Array
// of `2 * dim` words, where row n is packed as [lo32, hi32]. For 32-bit CRCs
// the high halves stay zero and the per-row loop exits early; for the 64-bit
// CRC (crc64nvme) the pair-of-u32s representation lets every XOR/shift stay on
// 32-bit ints.
//
// References:
//   zlib crc32_combine (canonical C implementation):
//     https://github.com/madler/zlib/blob/master/crc32.c
//   Mark Adler, "How does CRC32 work?" — derivation of the matrix trick:
//     https://stackoverflow.com/a/23126768
//   AWS S3 multipart upload full-object checksums:
//     https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html

function gf2MatrixTimes(mat, vecLo, vecHi) {
    let sumLo = 0;
    let sumHi = 0;
    let lo = vecLo;
    let hi = vecHi;
    let i = 0;
    while ((lo | hi) !== 0) {
        if (lo & 1) {
            sumLo ^= mat[2 * i];
            sumHi ^= mat[2 * i + 1];
        }
        lo = (lo >>> 1) | ((hi & 1) << 31);
        hi = hi >>> 1;
        i += 1;
    }
    return [sumLo >>> 0, sumHi >>> 0];
}

function gf2MatrixSquare(square, mat, dim) {
    for (let n = 0; n < dim; n += 1) {
        const r = gf2MatrixTimes(mat, mat[2 * n], mat[2 * n + 1]);
        // In-place mutation of the caller's scratch buffer is intentional —
        // the callers (combineCrcPair, ensureChainLen) own `square` and re-use
        // it across iterations to avoid re-allocating per squaring step.
        /* eslint-disable no-param-reassign */
        square[2 * n] = r[0];
        square[2 * n + 1] = r[1];
        /* eslint-enable no-param-reassign */
    }
}

// Per (polyReversed, dim), a lazily-grown chain of zero-byte operators.
// state.byteOps[j] is the GF(2) operator for prepending 2^j zero bytes
// (i.e. M^(8 * 2^j)). Building this chain is the dominant cost of combineCrcPair
// and depends only on the polynomial, so we cache it across calls.
const chainCache = new Map();

function getOrInitChain(polyReversed, dim) {
    let state = chainCache.get(polyReversed);
    if (state !== undefined) {
        return state;
    }

    // M^1: one-zero-bit operator. Column 0 is the polynomial; column k>0 is
    // 1 << (k - 1) — what right-shifting a state with bit k set produces.
    const m1 = new Uint32Array(2 * dim);
    m1[0] = Number(polyReversed & 0xffffffffn);
    m1[1] = Number((polyReversed >> 32n) & 0xffffffffn);
    for (let k = 1; k < dim; k += 1) {
        const bit = k - 1;
        if (bit < 32) {
            m1[2 * k] = (1 << bit) >>> 0;
        } else {
            m1[2 * k + 1] = (1 << (bit - 32)) >>> 0;
        }
    }

    const m2 = new Uint32Array(2 * dim);
    gf2MatrixSquare(m2, m1, dim);
    const m4 = new Uint32Array(2 * dim);
    gf2MatrixSquare(m4, m2, dim);
    const m8 = new Uint32Array(2 * dim); // operator for 1 zero byte
    gf2MatrixSquare(m8, m4, dim);

    state = { dim, byteOps: [m8] };
    chainCache.set(polyReversed, state);
    return state;
}

function ensureChainLen(state, j) {
    while (state.byteOps.length <= j) {
        const prev = state.byteOps[state.byteOps.length - 1];
        const next = new Uint32Array(prev.length);
        gf2MatrixSquare(next, prev, state.dim);
        state.byteOps.push(next);
    }
}

/**
 * Combine two CRCs of adjacent byte chunks.
 *
 *   combineCrcPair(crc(a), crc(b), len(b), polyReversed, dim) === crc(a ‖ b)
 *
 * Works for any right-shift CRC of width `dim` (32 or 64) given its
 * bit-reversed polynomial. The squaring chain for `polyReversed` is cached
 * across calls, so the per-call cost is just popcount(len2) cheap operator
 * applications plus the BigInt boundary conversions.
 *
 * @param {bigint} crc1 - CRC of the first chunk
 * @param {bigint} crc2 - CRC of the second chunk
 * @param {bigint} len2 - byte length of the second chunk
 * @param {bigint} polyReversed - bit-reversed polynomial
 * @param {number} dim - CRC width in bits (32 or 64)
 * @returns {bigint} CRC of the concatenated chunk, masked to `dim` bits
 */
function combineCrcPair(crc1, crc2, len2, polyReversed, dim) {
    const mask = (1n << BigInt(dim)) - 1n;
    if (len2 === 0n) {
        return crc1 & mask;
    }

    const state = getOrInitChain(polyReversed, dim);

    let cLo = Number(crc1 & 0xffffffffn);
    let cHi = Number((crc1 >> 32n) & 0xffffffffn);

    // Walk the bits of len2 (each bit represents a power-of-two number of
    // zero bytes to prepend); apply the cached operator for every set bit.
    let n = len2;
    let j = 0;
    while (n !== 0n) {
        if ((n & 1n) === 1n) {
            ensureChainLen(state, j);
            const r = gf2MatrixTimes(state.byteOps[j], cLo, cHi);
            cLo = r[0];
            cHi = r[1];
        }
        n >>= 1n;
        j += 1;
    }

    const c2Lo = Number(crc2 & 0xffffffffn);
    const c2Hi = Number((crc2 >> 32n) & 0xffffffffn);
    cLo = (cLo ^ c2Lo) >>> 0;
    cHi = (cHi ^ c2Hi) >>> 0;

    return ((BigInt(cHi) << 32n) | BigInt(cLo)) & mask;
}

function base64ToBigInt(b64) {
    const buf = Buffer.from(b64, 'base64');
    return BigInt(`0x${buf.toString('hex')}`);
}

function bigIntToBase64(value, dim) {
    const paddedHex = value.toString(16).padStart(dim / 4, '0');
    return Buffer.from(paddedHex, 'hex').toString('base64');
}

/**
 * Combine N per-part CRCs into the full-object CRC, base64-encoded.
 *
 * @param {Array<{value: string, length: number}>} parts - per-part data in
 *   part order; `value` is the base64-encoded per-part CRC, `length` is the
 *   byte length of that part
 * @param {bigint} polyReversed - bit-reversed polynomial
 * @param {number} dim - CRC width in bits (32 or 64)
 * @returns {string} base64-encoded combined CRC
 */
function combinePartCrcs(parts, polyReversed, dim) {
    let combined = base64ToBigInt(parts[0].value);
    for (let i = 1; i < parts.length; i += 1) {
        combined = combineCrcPair(combined, base64ToBigInt(parts[i].value), BigInt(parts[i].length), polyReversed, dim);
    }
    return bigIntToBase64(combined, dim);
}

module.exports = { combinePartCrcs, combineCrcPair };
