const { errors } = require('@scality/arsenal');

const constants = require('../../../../constants');
const { config } = require('../../../Config');

const { awsArchiveStorageClasses } = constants;

const awsArchiveLocations = new Map([...awsArchiveStorageClasses].map(([location, name]) => [name, location]));

/**
 * Map an AWS archive storage class to the cold location named after it
 * @param {string} storageClass - value of the x-amz-storage-class header
 * @returns {string} the storage class as used internally
 */
function normalizeStorageClass(storageClass) {
    const location = awsArchiveLocations.get(storageClass);
    if (location && isColdStorageClass(location)) {
        return location;
    }
    return storageClass;
}

/**
 * Map a stored storage class to the value reported to clients: a cold location named after
 * an AWS archive storage class is reported with the AWS name.
 * @param {string} storageClass - storage class from the object metadata
 * @returns {string} the storage class as reported in responses
 */
function formatStorageClass(storageClass) {
    if (!isColdStorageClass(storageClass)) {
        return storageClass;
    }
    return awsArchiveStorageClasses.get(storageClass) || storageClass;
}

/**
 * Whether the given storage class names a cold location of this deployment
 * @param {string} storageClass - value of the x-amz-storage-class header
 * @returns {boolean|undefined} true if the storage class is a cold location
 */
function isColdStorageClass(storageClass) {
    return config.locationConstraints[storageClass]?.isCold;
}

/**
 * Whether the Veeam on-prem archive tier can be used with this deployment
 *
 * Veeam writes to the archive tier with the GLACIER storage class, which needs direct-to-cold
 * writes and a cold location of that name
 * @returns {boolean} true if the archive tier is supported
 */
function supportsOnPremArchiveTier() {
    return !!(config.enableDirectToCold && isColdStorageClass('glacier'));
}

/**
 * Validate the x-amz-storage-class header of an object write request.
 *
 * Accepted values are the regular S3 storage classes, and, with
 * `enableDirectToCold`, the name of a cold location of this deployment. An AWS
 * archive storage class names the cold location named after it (see
 * `awsArchiveStorageClasses`): the header is rewritten accordingly, so that the API
 * only ever sees the location name.
 *
 * Restricting which identity may use a given storage class is done through the
 * `s3:x-amz-storage-class` policy condition key, and so is not handled here. IAM
 * policies are evaluated before this (by Vault, on the signed header), so they see
 * the storage class as sent by the client; bucket policies are evaluated afterwards,
 * so they see the location name.
 *
 * @param {object} headers - request headers, rewritten in place
 * @returns {ArsenalError|null} InvalidStorageClass if the value is not supported
 */
function validateStorageClass(headers) {
    const storageClass = headers['x-amz-storage-class'];
    if (!storageClass) {
        return null;
    }
    if (constants.validStorageClasses.includes(storageClass)) {
        return null;
    }
    // A restore writes back an object which is already in a cold location: naming a cold
    // storage class would send it straight back, so it is rejected rather than ignored.
    const putVersionId = headers['x-scal-s3-version-id'];
    if (putVersionId || putVersionId === '') {
        return errors.InvalidStorageClass;
    }
    const location = normalizeStorageClass(storageClass);
    if (config.enableDirectToCold && isColdStorageClass(location)) {
        // eslint-disable-next-line no-param-reassign
        headers['x-amz-storage-class'] = location;
        return null;
    }
    return errors.InvalidStorageClass;
}

module.exports = {
    normalizeStorageClass,
    formatStorageClass,
    isColdStorageClass,
    supportsOnPremArchiveTier,
    validateStorageClass,
};
