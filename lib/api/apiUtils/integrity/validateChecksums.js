const crypto = require('crypto');
const { Crc32 } = require('@aws-crypto/crc32');
const { Crc32c } = require('@aws-crypto/crc32c');
const { CrtCrc64Nvme } = require('@aws-sdk/crc64-nvme-crt');
const { errors: ArsenalErrors, errorInstances } = require('arsenal');
const { config } = require('../../../Config');
const { combinePartCrcs } = require('./crcCombine');

const defaultChecksumData = Object.freeze({ algorithm: 'crc64nvme', isTrailer: false, expected: undefined });

const errAlgoNotSupported = errorInstances.InvalidRequest.customizeDescription(
    'The algorithm type you specified in x-amz-checksum- header is invalid.',
);
const errAlgoNotSupportedSDK = errorInstances.InvalidRequest.customizeDescription(
    'Value for x-amz-sdk-checksum-algorithm header is invalid.',
);
const errMissingCorresponding = errorInstances.InvalidRequest.customizeDescription(
    'x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum-* ' +
        'or x-amz-trailer headers were found.',
);
const errMultipleChecksumTypes = errorInstances.InvalidRequest.customizeDescription(
    'Expecting a single x-amz-checksum- header. Multiple checksum Types are not allowed.',
);
const errTrailerAndChecksum = errorInstances.InvalidRequest.customizeDescription(
    'Expecting a single x-amz-checksum- header',
);
const errTrailerNotSupported = errorInstances.InvalidRequest.customizeDescription(
    'The value specified in the x-amz-trailer header is not supported',
);
const errMPUAlgoNotSupported = errorInstances.InvalidRequest.customizeDescription(
    'Checksum algorithm provided is unsupported. ' +
        'Please try again with any of the valid types: ' +
        '[CRC32, CRC32C, CRC64NVME, SHA1, SHA256]',
);
const errMPUTypeInvalid = errorInstances.InvalidRequest.customizeDescription(
    'Value for x-amz-checksum-type header is invalid.',
);
const errMPUTypeWithoutAlgo = errorInstances.InvalidRequest.customizeDescription(
    'The x-amz-checksum-type header can only be used ' + 'with the x-amz-checksum-algorithm header.',
);

const checksumedMethods = Object.freeze({
    // CompleteMPU's x-amz-checksum-<algo> is the final-object checksum,
    // not a body digest. Validated in completeMultipartUpload.js instead.
    // 'completeMultipartUpload': true,
    multiObjectDelete: true,
    bucketPutACL: true,
    bucketPutCors: true,
    bucketPutEncryption: true,
    bucketPutLifecycle: true,
    bucketPutLogging: true,
    bucketPutNotification: true,
    bucketPutPolicy: true,
    bucketPutReplication: true,
    bucketPutTagging: true,
    bucketPutVersioning: true,
    bucketPutWebsite: true,
    objectPutACL: true,
    objectPutLegalHold: true,
    bucketPutObjectLock: true, // PutObjectLockConfiguration
    objectPutRetention: true,
    objectPutTagging: true,
    objectRestore: true,
});

const ChecksumError = Object.freeze({
    MD5Mismatch: 'MD5Mismatch',
    MD5Invalid: 'MD5Invalid',
    XAmzMismatch: 'XAmzMismatch',
    MissingChecksum: 'MissingChecksum',
    AlgoNotSupported: 'AlgoNotSupported',
    AlgoNotSupportedSDK: 'AlgoNotSupportedSDK',
    MultipleChecksumTypes: 'MultipleChecksumTypes',
    MissingCorresponding: 'MissingCorresponding',
    MalformedChecksum: 'MalformedChecksum',
    TrailerAlgoMismatch: 'TrailerAlgoMismatch',
    TrailerChecksumMalformed: 'TrailerChecksumMalformed',
    TrailerMissing: 'TrailerMissing',
    TrailerUnexpected: 'TrailerUnexpected',
    TrailerAndChecksum: 'TrailerAndChecksum',
    TrailerNotSupported: 'TrailerNotSupported',
    MPUAlgoNotSupported: 'MPUAlgoNotSupported',
    MPUTypeInvalid: 'MPUTypeInvalid',
    MPUTypeWithoutAlgo: 'MPUTypeWithoutAlgo',
    MPUInvalidCombination: 'MPUInvalidCombination',
});

const base64Regex = /^[A-Za-z0-9+/]*={0,2}$/;

function uint32ToBase64(num) {
    const buf = Buffer.alloc(4);
    buf.writeUInt32BE(num, 0);
    return buf.toString('base64');
}

const algorithms = Object.freeze({
    crc64nvme: {
        xmlTag: 'ChecksumCRC64NVME',
        digest: async data => {
            const input = Buffer.isBuffer(data) ? data : Buffer.from(data);
            const crc = new CrtCrc64Nvme();
            crc.update(input);
            const result = await crc.digest();
            return Buffer.from(result).toString('base64');
        },
        digestFromHash: async hash => {
            const result = await hash.digest();
            return Buffer.from(result).toString('base64');
        },
        isValidDigest: expected => typeof expected === 'string' && expected.length === 12 && base64Regex.test(expected),
        createHash: () => new CrtCrc64Nvme(),
    },
    crc32: {
        xmlTag: 'ChecksumCRC32',
        digest: data => {
            const input = Buffer.isBuffer(data) ? data : Buffer.from(data);
            return uint32ToBase64(new Crc32().update(input).digest() >>> 0); // >>> 0 coerce number to uint32
        },
        digestFromHash: hash => {
            const result = hash.digest();
            return uint32ToBase64(result >>> 0);
        },
        isValidDigest: expected => typeof expected === 'string' && expected.length === 8 && base64Regex.test(expected),
        createHash: () => new Crc32(),
    },
    crc32c: {
        xmlTag: 'ChecksumCRC32C',
        digest: data => {
            const input = Buffer.isBuffer(data) ? data : Buffer.from(data);
            return uint32ToBase64(new Crc32c().update(input).digest() >>> 0); // >>> 0 coerce number to uint32
        },
        digestFromHash: hash => uint32ToBase64(hash.digest() >>> 0),
        isValidDigest: expected => typeof expected === 'string' && expected.length === 8 && base64Regex.test(expected),
        createHash: () => new Crc32c(),
    },
    sha1: {
        xmlTag: 'ChecksumSHA1',
        digest: data => {
            const input = Buffer.isBuffer(data) ? data : Buffer.from(data);
            return crypto.createHash('sha1').update(input).digest('base64');
        },
        digestFromHash: hash => hash.digest('base64'),
        isValidDigest: expected => typeof expected === 'string' && expected.length === 28 && base64Regex.test(expected),
        createHash: () => crypto.createHash('sha1'),
    },
    sha256: {
        xmlTag: 'ChecksumSHA256',
        digest: data => {
            const input = Buffer.isBuffer(data) ? data : Buffer.from(data);
            return crypto.createHash('sha256').update(input).digest('base64');
        },
        digestFromHash: hash => hash.digest('base64'),
        isValidDigest: expected => typeof expected === 'string' && expected.length === 44 && base64Regex.test(expected),
        createHash: () => crypto.createHash('sha256'),
    },
});

/**
 * Validate the x-amz-checksum-<algo> header against a buffered body.
 *
 * @param {object} headers - HTTP request headers (lowercased keys)
 * @param {Buffer} body - the entire buffered request body
 * @returns {Promise<null | { error: string, details: object }>} -
 *   null on success; otherwise a ChecksumError with details.
 */
async function validateXAmzChecksums(headers, body) {
    const checksumHeaders = Object.keys(headers).filter(header => header.startsWith('x-amz-checksum-'));
    const xAmzChecksumCnt = checksumHeaders.length;
    if (xAmzChecksumCnt > 1) {
        return { error: ChecksumError.MultipleChecksumTypes, details: { algorithms: checksumHeaders } };
    }

    if (xAmzChecksumCnt === 0 && 'x-amz-sdk-checksum-algorithm' in headers) {
        return {
            error: ChecksumError.MissingCorresponding,
            details: { expected: headers['x-amz-sdk-checksum-algorithm'] },
        };
    } else if (xAmzChecksumCnt === 0) {
        return { error: ChecksumError.MissingChecksum, details: null };
    }

    // No x-amz-sdk-checksum-algorithm we expect one x-amz-checksum-[crc64nvme, crc32, crc32C, sha1, sha256].
    const algo = checksumHeaders[0].slice('x-amz-checksum-'.length);
    if (!(algo in algorithms)) {
        return { error: ChecksumError.AlgoNotSupported, details: { algorithm: algo } };
    }

    const expected = headers[`x-amz-checksum-${algo}`];
    if (!algorithms[algo].isValidDigest(expected)) {
        return { error: ChecksumError.MalformedChecksum, details: { algorithm: algo, expected } };
    }

    const calculated = await algorithms[algo].digest(body);
    if (expected !== calculated) {
        return { error: ChecksumError.XAmzMismatch, details: { algorithm: algo, calculated, expected } };
    }

    // AWS checks x-amz-checksum- first and then x-amz-sdk-checksum-algorithm
    if ('x-amz-sdk-checksum-algorithm' in headers) {
        const sdkAlgo = headers['x-amz-sdk-checksum-algorithm'];
        if (typeof sdkAlgo !== 'string') {
            return { error: ChecksumError.AlgoNotSupportedSDK, details: { algorithm: sdkAlgo } };
        }

        const sdkLowerAlgo = sdkAlgo.toLowerCase();
        if (!(sdkLowerAlgo in algorithms)) {
            return { error: ChecksumError.AlgoNotSupportedSDK, details: { algorithm: sdkAlgo } };
        }

        // If there is a mismatch, AWS returns the same error as if the algo was invalid.
        if (sdkLowerAlgo !== algo) {
            return { error: ChecksumError.AlgoNotSupportedSDK, details: { algorithm: sdkAlgo } };
        }
    }

    return null;
}

/**
 * Extract checksum data from request headers.
 *
 * @param {object} headers - HTTP request headers (lowercased keys)
 * @returns {null
 *   | { algorithm: string, isTrailer: boolean, expected: string|undefined }
 *   | { error: string, details: object }}
 */
function getChecksumDataFromHeaders(headers) {
    const checkSdk = algo => {
        if (!('x-amz-sdk-checksum-algorithm' in headers)) {
            return null;
        }

        const sdkAlgo = headers['x-amz-sdk-checksum-algorithm'];
        if (typeof sdkAlgo !== 'string') {
            return { error: ChecksumError.AlgoNotSupportedSDK, details: { algorithm: sdkAlgo } };
        }

        const sdkLowerAlgo = sdkAlgo.toLowerCase();
        if (!(sdkLowerAlgo in algorithms)) {
            return { error: ChecksumError.AlgoNotSupportedSDK, details: { algorithm: sdkAlgo } };
        }

        // If there is a mismatch, AWS returns the same error as if the algo was invalid.
        if (sdkLowerAlgo !== algo) {
            return { error: ChecksumError.AlgoNotSupportedSDK, details: { algorithm: sdkAlgo } };
        }

        return null;
    };

    const checksumHeaders = Object.keys(headers).filter(header => header.startsWith('x-amz-checksum-'));
    const xAmzChecksumCnt = checksumHeaders.length;
    if (xAmzChecksumCnt > 1) {
        return { error: ChecksumError.MultipleChecksumTypes, details: { algorithms: checksumHeaders } };
    }

    const checksumHeader = xAmzChecksumCnt > 0 ? checksumHeaders[0] : undefined;
    if (checksumHeader === undefined && !('x-amz-trailer' in headers) && 'x-amz-sdk-checksum-algorithm' in headers) {
        return {
            error: ChecksumError.MissingCorresponding,
            details: { expected: headers['x-amz-sdk-checksum-algorithm'] },
        };
    }

    if ('x-amz-trailer' in headers) {
        if (checksumHeader) {
            return {
                error: ChecksumError.TrailerAndChecksum,
                details: { trailer: headers['x-amz-trailer'], checksum: checksumHeaders },
            };
        }

        const trailer = headers['x-amz-trailer'];
        if (!trailer.startsWith('x-amz-checksum-')) {
            return { error: ChecksumError.TrailerNotSupported, details: { value: trailer } };
        }

        const trailerAlgo = trailer.slice('x-amz-checksum-'.length);
        if (!(trailerAlgo in algorithms)) {
            return { error: ChecksumError.TrailerNotSupported, details: { value: trailer } };
        }

        const err = checkSdk(trailerAlgo);
        if (err) {
            return err;
        }

        return { algorithm: trailerAlgo, isTrailer: true, expected: undefined };
    }

    if (!checksumHeader) {
        // No x-amz-checksum- or x-amz-trailer header.
        return null;
    }

    // No x-amz-sdk-checksum-algorithm we expect one x-amz-checksum-[crc64nvme, crc32, crc32C, sha1, sha256].
    const algo = checksumHeader.slice('x-amz-checksum-'.length);
    if (!(algo in algorithms)) {
        return { error: ChecksumError.AlgoNotSupported, details: { algorithm: algo } };
    }

    const expected = headers[`x-amz-checksum-${algo}`];
    if (!algorithms[algo].isValidDigest(expected)) {
        return { error: ChecksumError.MalformedChecksum, details: { algorithm: algo, expected } };
    }

    const err = checkSdk(algo);
    if (err) {
        return err;
    }

    return { algorithm: algo, isTrailer: false, expected };
}

/**
 * validateChecksumsNoChunking - Validate the checksums of a request.
 * @param {object} headers - http headers
 * @param {Buffer} body - http request body
 * @return {object} - error
 */
async function validateChecksumsNoChunking(headers, body) {
    if (!headers) {
        return { error: ChecksumError.MissingChecksum, details: null };
    }

    let md5Present = false;
    if ('content-md5' in headers) {
        if (typeof headers['content-md5'] !== 'string') {
            return { error: ChecksumError.MD5Invalid, details: { expected: headers['content-md5'] } };
        }

        if (headers['content-md5'].length !== 24) {
            return { error: ChecksumError.MD5Invalid, details: { expected: headers['content-md5'] } };
        }

        if (!base64Regex.test(headers['content-md5'])) {
            return { error: ChecksumError.MD5Invalid, details: { expected: headers['content-md5'] } };
        }

        const md5 = crypto.createHash('md5').update(body).digest('base64');
        if (md5 !== headers['content-md5']) {
            return { error: ChecksumError.MD5Mismatch, details: { calculated: md5, expected: headers['content-md5'] } };
        }

        md5Present = true;
    }

    const err = await validateXAmzChecksums(headers, body);
    if (err && err.error === ChecksumError.MissingChecksum && md5Present) {
        // Don't return MissingChecksum if MD5 is present.
        return null;
    }

    return err;
}

function arsenalErrorFromChecksumError(err) {
    switch (err.error) {
        case ChecksumError.MissingChecksum:
            return null;
        case ChecksumError.XAmzMismatch: {
            const algoUpper = err.details.algorithm.toUpperCase();
            return errorInstances.BadDigest.customizeDescription(
                `The ${algoUpper} you specified did not match the calculated checksum.`,
            );
        }
        case ChecksumError.AlgoNotSupported:
            return errAlgoNotSupported;
        case ChecksumError.AlgoNotSupportedSDK:
            return errAlgoNotSupportedSDK;
        case ChecksumError.MissingCorresponding:
            return errMissingCorresponding;
        case ChecksumError.MultipleChecksumTypes:
            return errMultipleChecksumTypes;
        case ChecksumError.MalformedChecksum:
            return errorInstances.InvalidRequest.customizeDescription(
                `Value for x-amz-checksum-${err.details.algorithm} header is invalid.`,
            );
        case ChecksumError.MD5Invalid:
            return ArsenalErrors.InvalidDigest;
        case ChecksumError.TrailerAlgoMismatch:
            return ArsenalErrors.MalformedTrailerError;
        case ChecksumError.TrailerMissing:
            return ArsenalErrors.MalformedTrailerError;
        case ChecksumError.TrailerUnexpected:
            return ArsenalErrors.MalformedTrailerError;
        case ChecksumError.TrailerChecksumMalformed:
            return errorInstances.InvalidRequest.customizeDescription(
                `Value for x-amz-checksum-${err.details.algorithm} trailing header is invalid.`,
            );
        case ChecksumError.TrailerAndChecksum:
            return errTrailerAndChecksum;
        case ChecksumError.TrailerNotSupported:
            return errTrailerNotSupported;
        case ChecksumError.MPUAlgoNotSupported:
            return errMPUAlgoNotSupported;
        case ChecksumError.MPUTypeInvalid:
            return errMPUTypeInvalid;
        case ChecksumError.MPUTypeWithoutAlgo:
            return errMPUTypeWithoutAlgo;
        case ChecksumError.MPUInvalidCombination:
            return errorInstances.InvalidRequest.customizeDescription(
                `The ${err.details.type} checksum type cannot be used ` +
                    `with the ${err.details.algorithm.toUpperCase()} checksum algorithm.`,
            );
        default:
            return ArsenalErrors.BadDigest;
    }
}

async function defaultValidationFunc(request, body, log) {
    const err = await validateChecksumsNoChunking(request.headers, body);
    if (!err) {
        return null;
    }

    if (err.error !== ChecksumError.MissingChecksum) {
        log.debug('failed checksum validation', { method: request.apiMethod }, err);
    }

    return arsenalErrorFromChecksumError(err);
}

/**
 * validateMethodChecksumsNoChunking - Validate the checksums of a request.
 * @param {object} request - http request
 * @param {Buffer} body - http request body
 * @param {object} log - logger
 * @return {object} - error
 */
async function validateMethodChecksumNoChunking(request, body, log) {
    if (config.integrityChecks[request.apiMethod] === false) {
        return null;
    }

    if (request.apiMethod in checksumedMethods) {
        return await defaultValidationFunc(request, body, log);
    }

    return null;
}

const validMPUTypes = new Set(['COMPOSITE', 'FULL_OBJECT']);
const fullObjectAlgorithms = new Set(['crc32', 'crc32c', 'crc64nvme']);
const compositeAlgorithms = new Set(['crc32', 'crc32c', 'sha1', 'sha256']);

const defaultChecksumType = {
    crc32: 'COMPOSITE',
    crc32c: 'COMPOSITE',
    crc64nvme: 'FULL_OBJECT',
    sha1: 'COMPOSITE',
    sha256: 'COMPOSITE',
};

/**
 * Validate x-amz-checksum-algorithm and x-amz-checksum-type headers
 * for CreateMultipartUpload.
 *
 * Validation order mirrors AWS: algorithm first, then type.
 *
 * @param {object} headers - request headers
 * @returns {object} { algorithm, type, isDefault } on success, { error } on failure.
 *   Defaults to crc64nvme/FULL_OBJECT with isDefault=true when no headers sent.
 */
function getChecksumDataFromMPUHeaders(headers) {
    const algorithmHeader = headers['x-amz-checksum-algorithm'];
    const typeHeader = headers['x-amz-checksum-type'];

    // No checksum headers — use implicit default
    if (!algorithmHeader && !typeHeader) {
        // isDefault to true means that the checksum won't be returned in listMPUS
        return { algorithm: 'crc64nvme', type: defaultChecksumType['crc64nvme'], isDefault: true };
    }

    // Algorithm first
    if (algorithmHeader) {
        const algo = algorithmHeader.toLowerCase();
        if (!(algo in algorithms)) {
            return { error: ChecksumError.MPUAlgoNotSupported, details: { algorithm: algorithmHeader } };
        }
    }

    // Then type
    if (typeHeader && !algorithmHeader) {
        return { error: ChecksumError.MPUTypeWithoutAlgo, details: { type: typeHeader } };
    }

    const algo = algorithmHeader.toLowerCase();

    if (typeHeader) {
        const type = typeHeader.toUpperCase();
        if (!validMPUTypes.has(type)) {
            return { error: ChecksumError.MPUTypeInvalid, details: { type: typeHeader } };
        }

        // Validate algorithm + type combination
        if (
            (type === 'FULL_OBJECT' && !fullObjectAlgorithms.has(algo)) ||
            (type === 'COMPOSITE' && !compositeAlgorithms.has(algo))
        ) {
            return { error: ChecksumError.MPUInvalidCombination, details: { algorithm: algo, type } };
        }

        return { algorithm: algo, type, isDefault: false };
    }

    // Only algorithm sent, apply default type
    return { algorithm: algo, type: defaultChecksumType[algo], isDefault: false };
}

// =============================================================================
// MPU final-object checksum computation
// =============================================================================
//
// CompleteMultipartUpload composes a final-object checksum from the per-part
// checksums recorded at UploadPart time. AWS defines two modes:
//
//   COMPOSITE   : finalChecksum = base64(algo(decode(c1) || ... || decode(cN)))
//                 + "-N" suffix, where N is the number of parts.
//                 Supported on CRC32, CRC32C, SHA1, SHA256.
//
//   FULL_OBJECT : finalChecksum is the CRC of the entire object's bytes,
//                 reconstructed by combining the per-part CRCs via CRC
//                 linearization. CRC-only: CRC32, CRC32C,
//                 CRC64NVME.

// Bit-reversed polynomials used by the right-shift CRC implementations that
// the @aws-crypto/* and @aws-sdk/crc64-nvme-crt packages produce.
const FULL_OBJECT_POLYS = Object.freeze({
    crc32: { polyReversed: 0xedb88320n, dim: 32 },
    crc32c: { polyReversed: 0x82f63b78n, dim: 32 },
    crc64nvme: { polyReversed: 0x9a6c9329ac4bc9b5n, dim: 64 },
});

// Algorithms whose digest is synchronous, which is the full set AWS allows
// for COMPOSITE MPUs. crc64nvme is excluded because (a) AWS does not allow
// COMPOSITE for CRC64NVME and (b) its CRT-backed digest is async.
const COMPOSITE_ALGOS = new Set(['crc32', 'crc32c', 'sha1', 'sha256']);

/**
 * Compute the COMPOSITE final-object checksum for a CompleteMultipartUpload.
 *
 *   final = base64(algo(decode(c1) || decode(c2) || ... || decode(cN))) + "-N"
 *
 * Supported algorithms: crc32, crc32c, sha1, sha256. (crc64nvme is excluded —
 * AWS does not allow COMPOSITE for CRC64NVME.)
 *
 * @param {string} algorithm - lowercase algorithm name
 * @param {string[]} partChecksumsBase64 - per-part checksums in part order,
 *   each base64-encoded (the format stored on MPU part metadata)
 * @returns {{ checksum: string, error: null }
 *   | { checksum: null, error: { code: string, details: object } }}
 */
function computeCompositeMPUChecksum(algorithm, partChecksumsBase64) {
    if (!COMPOSITE_ALGOS.has(algorithm)) {
        return { checksum: null, error: { code: ChecksumError.MPUAlgoNotSupported, details: { algorithm } } };
    }

    const concat = Buffer.concat(partChecksumsBase64.map(c => Buffer.from(c, 'base64')));
    const digest = algorithms[algorithm].digest(concat);
    return {
        checksum: `${digest}-${partChecksumsBase64.length}`,
        error: null,
    };
}

/**
 * Compute the FULL_OBJECT final-object checksum for a CompleteMultipartUpload.
 *
 * Returns the CRC of the assembled object's bytes, derived purely from the
 * per-part CRCs and part lengths via CRC linearization.
 *
 * Supported algorithms: crc32, crc32c, crc64nvme.
 *
 * @param {string} algorithm - lowercase algorithm name
 * @param {Array<{value: string, length: number}>} parts - per-part data in
 *   part order; `value` is the base64-encoded per-part CRC, `length` is the
 *   byte length of that part
 * @returns {{ checksum: string, error: null }
 *   | { checksum: null, error: { code: string, details: object } }}
 */
function computeFullObjectMPUChecksum(algorithm, parts) {
    const params = FULL_OBJECT_POLYS[algorithm];
    if (!params) {
        return { checksum: null, error: { code: ChecksumError.MPUAlgoNotSupported, details: { algorithm } } };
    }
    return { checksum: combinePartCrcs(parts, params.polyReversed, params.dim), error: null };
}

module.exports = {
    ChecksumError,
    defaultChecksumData,
    validateChecksumsNoChunking,
    validateMethodChecksumNoChunking,
    getChecksumDataFromHeaders,
    arsenalErrorFromChecksumError,
    algorithms,
    checksumedMethods,
    getChecksumDataFromMPUHeaders,
    computeCompositeMPUChecksum,
    computeFullObjectMPUChecksum,
};
