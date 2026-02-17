const crypto = require('crypto');
const { Crc32 } = require('@aws-crypto/crc32');
const { Crc32c } = require('@aws-crypto/crc32c');
const { CrtCrc64Nvme } = require('@aws-sdk/crc64-nvme-crt');
const { errors: ArsenalErrors } = require('arsenal');
const { config } = require('../../../Config');

const ChecksumError = Object.freeze({
    MD5Mismatch: 'MD5Mismatch',
    XAmzMismatch: 'XAmzMismatch',
    MissingChecksum: 'MissingChecksum',
    AlgoNotSupported: 'AlgoNotSupported',
    AlgoNotSupportedSDK: 'AlgoNotSupportedSDK',
    MultipleChecksumTypes: 'MultipleChecksumTypes',
    MissingCorresponding: 'MissingCorresponding',
    MalformedChecksum: 'MalformedChecksum',
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
        isValidDigest: expected => typeof expected === 'string' && expected.length === 12 && base64Regex.test(expected),
    },
    crc32: {
        digest: data => {
            const input = Buffer.isBuffer(data) ? data : Buffer.from(data);
            return uint32ToBase64(new Crc32().update(input).digest() >>> 0); // >>> 0 coerce number to uint32
        },
        isValidDigest: expected => typeof expected === 'string' && expected.length === 8 && base64Regex.test(expected),
    },
    crc32c: {
        digest: data => {
            const input = Buffer.isBuffer(data) ? data : Buffer.from(data);
            return uint32ToBase64(new Crc32c().update(input).digest() >>> 0); // >>> 0 coerce number to uint32
        },
        isValidDigest: expected => typeof expected === 'string' && expected.length === 8 && base64Regex.test(expected),
    },
    sha1: {
        digest: data => {
            const input = Buffer.isBuffer(data) ? data : Buffer.from(data);
            return crypto.createHash('sha1').update(input).digest('base64');
        },
        isValidDigest: expected => typeof expected === 'string' && expected.length === 28 && base64Regex.test(expected),
    },
    sha256: {
        digest: data => {
            const input = Buffer.isBuffer(data) ? data : Buffer.from(data);
            return crypto.createHash('sha256').update(input).digest('base64');
        },
        isValidDigest: expected => typeof expected === 'string' && expected.length === 44 && base64Regex.test(expected),
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
        // TODO: check if the content-md5 is valid base64
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

async function defaultValidationFunc(request, body, log) {
    const err = await validateChecksumsNoChunking(request.headers, body);
    if (!err) {
        return null;
    }

    log.debug('failed checksum validation', { method: request.apiMethod }, err);

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
        default:
            return ArsenalErrors.BadDigest;
    }
}

const methodValidationFunc = Object.freeze({
    'completeMultipartUpload': defaultValidationFunc,
    'bucketPutACL': defaultValidationFunc,
    'bucketPutCors': defaultValidationFunc,
    'bucketPutEncryption': defaultValidationFunc,
    'bucketPutLifecycle': defaultValidationFunc,
    'bucketPutNotification': defaultValidationFunc,
    'bucketPutObjectLock': defaultValidationFunc,
    'bucketPutPolicy': defaultValidationFunc,
    'bucketPutReplication': defaultValidationFunc,
    'bucketPutVersioning': defaultValidationFunc,
    'bucketPutWebsite': defaultValidationFunc,
    'bucketPutLogging': defaultValidationFunc,
    'bucketPutTagging': defaultValidationFunc,
    // TODO: DeleteObjects requires a checksum. Should return an error if ChecksumError.MissingChecksum.
    'multiObjectDelete': defaultValidationFunc,
    'objectPutACL': defaultValidationFunc,
    'objectPutLegalHold': defaultValidationFunc,
    'objectPutTagging': defaultValidationFunc,
    'objectPutRetention': defaultValidationFunc,
    'objectRestore': defaultValidationFunc,
});

/**
 * validateMethodChecksumsNoChunking - Validate the checksums of a request.
 * @param {object} request - http request
 * @param {Buffer} body - http request body
 * @param {object} log - logger
 * @return {object} - error
 */
async function validateMethodChecksumNoChunking(request, body, log) {
    if (config.integrityChecks[request.apiMethod]) {
        const validationFunc = methodValidationFunc[request.apiMethod];
        if (!validationFunc) {
            return null; //await defaultValidationFunc2(request, body, log);
        }
        return await validationFunc(request, body, log);
    }

    return null;
}

module.exports = {
    ChecksumError,
    validateChecksumsNoChunking,
    validateMethodChecksumNoChunking,
};
