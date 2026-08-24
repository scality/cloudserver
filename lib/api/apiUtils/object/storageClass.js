const { errors } = require('arsenal');

const constants = require('../../../../constants');
const { config } = require('../../../Config');

/**
 * Whether the given storage class names a cold location of this deployment
 * @param {string} storageClass - value of the x-amz-storage-class header
 * @returns {boolean|undefined} true if the storage class is a cold location
 */
function isColdStorageClass(storageClass) {
    return config.locationConstraints[storageClass]?.isCold;
}

/**
 * Validate the x-amz-storage-class header of an object write request.
 *
 * Accepted values are the regular S3 storage classes, and, with
 * `enableDirectToCold`, the name of a cold location of this deployment.
 *
 * Restricting which identity may use a given storage class is done through the
 * `s3:x-amz-storage-class` IAM policy condition key, and so is not handled here.
 *
 * @param {object} headers - request headers
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
    if (config.enableDirectToCold && isColdStorageClass(storageClass)) {
        return null;
    }
    return errors.InvalidStorageClass;
}

module.exports = {
    isColdStorageClass,
    validateStorageClass,
};
