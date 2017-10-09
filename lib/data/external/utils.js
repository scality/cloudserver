const constants = require('../../../constants');

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
};

module.exports = utils;
