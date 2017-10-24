const constants = require('../../../constants');
const { config } = require('../../../lib/Config');

const utils = {
    logHelper(log, level, description, error, dataStoreName) {
        log[level](description, { error: error.message, stack: error.stack,
            dataStoreName });
    },
    // take off the 'x-amz-meta-'
    trimXMetaPrefix(obj) {
        const newObj = {};
        Object.keys(obj).forEach(key => {
            const newKey = key.substring(11);
            newObj[newKey] = obj[key];
        });
        return newObj;
    },
    removeQuotes(word) {
        return word.slice(1, -1);
    },
    skipMpuPartProcessing(completeObjData) {
        const backendType = completeObjData.dataStoreType;
        if (constants.mpuMDStoredExternallyBackend[backendType]) {
            return true;
        }
        return false;
    },
    /**
     * checkAzureBackendMatch - checks that the external backend location for
     * two data objects is the same and is Azure
     * @param {array} objectDataOne - data of first object to compare
     * @param {object} objectDataTwo - data of second object to compare
     * @return {boolean} - true if both data backends are Azure, false if not
     */
    checkAzureBackendMatch(objectDataOne, objectDataTwo) {
        if (objectDataOne.dataStoreType === 'azure' &&
        objectDataTwo.dataStoreType === 'azure') {
            return true;
        }
        return false;
    },

    /**
     * externalBackendCopy - using external copyObject only if copying object
     * from one externalbackend to the same external backend and for Azure if
     * it is the same account since Azure copy outside of an account is async
     * @param {string} locationConstraintSrc - location constraint of the source
     * @param {string} locationConstraintDest - location constraint of the
     * destination
     * @return {boolean} - true if copying object from one
     * externalbackend to the same external backend and for Azure if it is the
     * same account since Azure copy outside of an account is async
     */
    externalBackendCopy(locationConstraintSrc, locationConstraintDest) {
        const sourceLocationConstraintType =
        config.getLocationConstraintType(locationConstraintSrc);
        const locationTypeMatch =
        config.getLocationConstraintType(locationConstraintSrc) ===
        config.getLocationConstraintType(locationConstraintDest);
        return locationTypeMatch &&
              (sourceLocationConstraintType === 'aws_s3' ||
              (sourceLocationConstraintType === 'azure' &&
              config.isSameAzureAccount(locationConstraintSrc,
              locationConstraintDest)));
    },
};

module.exports = utils;
