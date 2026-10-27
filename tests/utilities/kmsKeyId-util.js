const kms = require('../../lib/kms/wrapper');
const { config } = require('../../lib/Config');
const {
    getKeyIdFromArn,
    isScalityKmsArn,
    makeScalityArnPrefix,
} = require('@scality/arsenal/build/lib/network/KMSInterface');

/**
 * Key ID returned in responses for a stored key: with kmsHideScalityArn, only keys
 * of the default KMS lose their arn prefix, and a key stored without prefix belongs
 * to the SSE-migration KMS when it is set
 * @param {string} key - KeyId or KeyArn as stored in metadata
 * @returns {string} expected KeyId or KeyArn
 */
function getExpectedKeyId(key) {
    if (!config.kmsHideScalityArn) {
        return key;
    }
    if (isScalityKmsArn(key)) {
        return key.startsWith(kms.arnPrefix) ? getKeyIdFromArn(key) : key;
    }
    if (config.sseMigration) {
        const { previousKeyType, previousKeyProtocol, previousKeyProvider } = config.sseMigration;
        const previousPrefix = makeScalityArnPrefix(previousKeyType, previousKeyProtocol, previousKeyProvider);
        return previousPrefix === kms.arnPrefix ? key : `${previousPrefix}${key}`;
    }
    return key;
}

module.exports = {
    getExpectedKeyId,
};
