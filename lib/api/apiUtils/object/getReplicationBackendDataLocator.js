const { errors } = require('arsenal');

/**
 * getReplicationBackendDataLocator - compares given location constraint to
 * replication backends
 * @param {object} locationObj - object containing location information
 * @param {string} locationObj.location - name of location constraint
 * @param {string} locationObj.key - keyname of object in location constraint
 * @param {string} locationObj.locationType - type of location constraint
 * @param {object} replicationInfo - information about object replication
 * @param {array} replicationInfo.backends - array containing information about
 * each replication location
 * @param {string} replicationInfo.backends[].site - name of replication
 * location
 * @param {string} replicationInfo.backends[].status - status of replication
 * @param {string} replicationInfo.backends[].dataStoreVersionId - version id
 * of object at replication location
 * @return {object} res - response object
 *     {array} [res.dataLocator] - if COMPLETED status: array
 *                                 containing the cloud location,
 *                                 undefined otherwise
 *     {string} [res.status] - replication status if no error
 *     {string} [res.reason] - reason message if PENDING/FAILED
 *     {Error} [res.error] - defined if object is not replicated to
 *                           location passed in locationObj
 */
function getReplicationBackendDataLocator(locationObj, replicationInfo) {
    const repBackendResult = {};
    const locMatch = replicationInfo.backends.find(
        backend => backend.site === locationObj.location);
    if (!locMatch) {
        repBackendResult.error = errors.InvalidLocationConstraint.
            customizeDescription('Object is not replicated to location ' +
            'passed in location header');
        return repBackendResult;
    }
    repBackendResult.status = locMatch.status;
    if (['PENDING', 'FAILED'].includes(locMatch.status)) {
        repBackendResult.reason =
            `Object replication to specified backend is ${locMatch.status}`;
        return repBackendResult;
    }
    repBackendResult.dataLocator = [{
        key: locationObj.key,
        dataStoreName: locationObj.location,
        dataStoreType: locationObj.locationType,
        dataStoreVersionId: locMatch.dataStoreVersionId }];
    return repBackendResult;
}

module.exports = getReplicationBackendDataLocator;
