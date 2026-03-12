const crypto = require('crypto');
const { Crc32 } = require('@aws-crypto/crc32');
const { Crc32c } = require('@aws-crypto/crc32c');
const { CrtCrc64Nvme } = require('@aws-sdk/crc64-nvme-crt');
const { errors: ArsenalErrors } = require('arsenal');
const { config } = require('../../../Config');

const checksumedMethods = Object.freeze({
    'completeMultipartUpload': true,
    'multiObjectDelete': true,
    'bucketPutACL': true,
    'bucketPutCors': true,
    'bucketPutEncryption': true,
    'bucketPutLifecycle': true,
    'bucketPutLogging': true,
    'bucketPutNotification': true,
    'bucketPutPolicy': true,
    'bucketPutReplication': true,
    'bucketPutTagging': true,
    'bucketPutVersioning': true,
    'bucketPutWebsite': true,
    'objectPutACL': true,
    'objectPutLegalHold': true,
    'bucketPutObjectLock': true, // PutObjectLockConfiguration
    'objectPutRetention': true,
    'objectPutTagging': true,
    'objectRestore': true,
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
});

const base64Regex = /^[A-Za-z0-9+/]*={0,2}$/;

function uint32ToBase64(num) {
    const buf = Buffer.alloc(4);
    buf.writeUInt32BE(num, 0);
    return buf.toString('base64');
}

const algorithms = Object.freeze({
    crc64nvme: {
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
        createHash: () => new CrtCrc64Nvme()
    },
    crc32: {
        digest: data => {
            const input = Buffer.isBuffer(data) ? data : Buffer.from(data);
            return uint32ToBase64(new Crc32().update(input).digest() >>> 0); // >>> 0 coerce number to uint32
        },
        digestFromHash: hash => {
            const result = hash.digest();
            return uint32ToBase64(result >>> 0);
        },
        isValidDigest: expected => typeof expected === 'string' && expected.length === 8 && base64Regex.test(expected),
        createHash: () => new Crc32()
    },
    crc32c: {
        digest: data => {
            const input = Buffer.isBuffer(data) ? data : Buffer.from(data);
            return uint32ToBase64(new Crc32c().update(input).digest() >>> 0); // >>> 0 coerce number to uint32
        },
        digestFromHash: hash => uint32ToBase64(hash.digest() >>> 0),
        isValidDigest: expected => typeof expected === 'string' && expected.length === 8 && base64Regex.test(expected),
        createHash: () => new Crc32c()
    },
    sha1: {
        digest: data => {
            const input = Buffer.isBuffer(data) ? data : Buffer.from(data);
            return crypto.createHash('sha1').update(input).digest('base64');
        },
        digestFromHash: hash => hash.digest('base64'),
        isValidDigest: expected => typeof expected === 'string' && expected.length === 28 && base64Regex.test(expected),
        createHash: () => crypto.createHash('sha1')
    },
    sha256: {
        digest: data => {
            const input = Buffer.isBuffer(data) ? data : Buffer.from(data);
            return crypto.createHash('sha256').update(input).digest('base64');
        },
        digestFromHash: hash => hash.digest('base64'),
        isValidDigest: expected => typeof expected === 'string' && expected.length === 44 && base64Regex.test(expected),
        createHash: () => crypto.createHash('sha256')
    }
});

async function validateXAmzChecksums(headers, body) {
    const checksumHeaders = Object.keys(headers).filter(header => header.startsWith('x-amz-checksum-'));
    const xAmzChecksumCnt = checksumHeaders.length;
    if (xAmzChecksumCnt > 1) {
        return { error: ChecksumError.MultipleChecksumTypes, details: { algorithms: checksumHeaders } };
    }

    if (xAmzChecksumCnt === 0 && 'x-amz-sdk-checksum-algorithm' in headers) {
        return {
            error: ChecksumError.MissingCorresponding,
            details: { expected: headers['x-amz-sdk-checksum-algorithm'] }
        };
    } else if (xAmzChecksumCnt === 0) {
        return { error: ChecksumError.MissingChecksum, details: null };
    }

    // No x-amz-sdk-checksum-algorithm we expect one x-amz-checksum-[crc64nvme, crc32, crc32C, sha1, sha256].
    const algo = checksumHeaders[0].slice('x-amz-checksum-'.length);
    if (!(algo in algorithms)) {
        return { error: ChecksumError.AlgoNotSupported, details: { algorithm: algo } };;
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

        // If AWS there is a mismatch, AWS returns the same error as if the algo was invalid.
        if (sdkLowerAlgo !== algo) {
            return { error: ChecksumError.AlgoNotSupportedSDK, details: { algorithm: sdkAlgo } };
        }
    }

    return null;
}

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

        // If AWS there is a mismatch, AWS returns the same error as if the algo was invalid.
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

    if (xAmzChecksumCnt === 0 && !('x-amz-trailer' in headers) && 'x-amz-sdk-checksum-algorithm' in headers) {
        return {
            error: ChecksumError.MissingCorresponding,
            details: { expected: headers['x-amz-sdk-checksum-algorithm'] }
        };
    }

    if ('x-amz-trailer' in headers) {
        if (xAmzChecksumCnt !== 0) {
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

    if (xAmzChecksumCnt === 0) {
        // There was no x-amz-checksum- or x-amz-trailer return crc64nvme
        return { algorithm: 'crc64nvme', isTrailer: false, expected: undefined };
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
            return ArsenalErrors.BadDigest.customizeDescription(
                `The ${algoUpper} you specified did not match the calculated checksum.`
            );
        }
        case ChecksumError.AlgoNotSupported:
            return ArsenalErrors.InvalidRequest.customizeDescription(
                'The algorithm type you specified in x-amz-checksum- header is invalid.'
            );
        case ChecksumError.AlgoNotSupportedSDK:
            return ArsenalErrors.InvalidRequest.customizeDescription(
                'Value for x-amz-sdk-checksum-algorithm header is invalid.'
            );
        case ChecksumError.MissingCorresponding:
            return ArsenalErrors.InvalidRequest.customizeDescription(
                'x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum-* ' +
                'or x-amz-trailer headers were found.'
            );
        case ChecksumError.MultipleChecksumTypes:
            return ArsenalErrors.InvalidRequest.customizeDescription(
                'Expecting a single x-amz-checksum- header. Multiple checksum Types are not allowed.'
            );
        case ChecksumError.MalformedChecksum:
            return ArsenalErrors.InvalidRequest.customizeDescription(
                `Value for x-amz-checksum-${err.details.algorithm} header is invalid.`
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
            return ArsenalErrors.InvalidRequest.customizeDescription(
                `Value for x-amz-checksum-${err.details.algorithm} trailing header is invalid.`
            );
        case ChecksumError.TrailerAndChecksum:
            return ArsenalErrors.InvalidRequest.customizeDescription('Expecting a single x-amz-checksum- header');
        case ChecksumError.TrailerNotSupported:
            return ArsenalErrors.InvalidRequest.customizeDescription(
                'The value specified in the x-amz-trailer header is not supported'
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

module.exports = {
    ChecksumError,
    validateChecksumsNoChunking,
    validateMethodChecksumNoChunking,
    getChecksumDataFromHeaders,
    arsenalErrorFromChecksumError,
    algorithms,
    checksumedMethods,
};
