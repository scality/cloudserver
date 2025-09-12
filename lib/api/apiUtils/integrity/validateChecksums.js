const crypto = require('crypto');
const { errors: ArsenalErrors } = require('arsenal');
const { config } = require('../../../Config');

const ChecksumError = Object.freeze({
    MD5Mismatch: 'MD5Mismatch',
    MissingChecksum: 'MissingChecksum',
});

/**
 * validateChecksumsNoChunking - Validate the checksums of a request.
 * @param {object} headers - http headers
 * @param {Buffer} body - http request body
 * @return {object} - error
 */
function validateChecksumsNoChunking(headers, body) {
    if (headers && 'content-md5' in headers) {
        const md5 = crypto.createHash('md5').update(body).digest('base64');
        if (md5 !== headers['content-md5']) {
            return { error: ChecksumError.MD5Mismatch, details: { calculated: md5, expected: headers['content-md5'] } };
        }

        return null;
    }

    return { error: ChecksumError.MissingChecksum, details: null };
}

function defaultValidationFunc(request, body, log) {
    const err = validateChecksumsNoChunking(request.headers, body);
    if (err && err.error !== ChecksumError.MissingChecksum) {
        log.debug('failed checksum validation', { method: request.apiMethod }, err);
        return ArsenalErrors.BadDigest;
    }

    return null;
}

const methodValidationFunc = Object.freeze({
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
    // TODO: DeleteObjects requires a checksum. Should return an error if ChecksumError.MissingChecksum.
    'multiObjectDelete': defaultValidationFunc,
    'objectPutACL': defaultValidationFunc,
    'objectPutLegalHold': defaultValidationFunc,
    'objectPutTagging': defaultValidationFunc,
    'objectPutRetention': defaultValidationFunc,
});

/**
 * validateMethodChecksumsNoChunking - Validate the checksums of a request.
 * @param {object} request - http request
 * @param {Buffer} body - http request body
 * @param {object} log - logger
 * @return {object} - error
 */
function validateMethodChecksumNoChunking(request, body, log) {
    if (config.integrityChecks[request.apiMethod]) {
        const validationFunc = methodValidationFunc[request.apiMethod];
        if (!validationFunc) {
            return null;
        }

        return validationFunc(request, body, log);
    }

    return null;
}

module.exports = {
    ChecksumError,
    validateChecksumsNoChunking,
    validateMethodChecksumNoChunking,
};
