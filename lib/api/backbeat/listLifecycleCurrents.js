const { errors } = require('arsenal');
const constants = require('../../../constants');
const services = require('../../services');
const { standardMetadataValidateBucket } = require('../../metadata/metadataUtils');
const { pushMetric } = require('../../utapi/utilities');
const monitoring = require('../../utilities/monitoringHandler');
const { getLocationConstraintErrorMessage, processCurrents,
    validateMaxScannedEntries } = require('../apiUtils/object/lifecycle');
const { config } = require('../../Config');

function handleResult(listParams, requestMaxKeys, authInfo,
    bucketName, list, isBucketVersioned, log, callback) {
    // eslint-disable-next-line no-param-reassign
    listParams.maxKeys = requestMaxKeys;
    const res = processCurrents(bucketName, listParams, isBucketVersioned, list);

    pushMetric('listLifecycleCurrents', log, { authInfo, bucket: bucketName });
    monitoring.promMetrics('GET', bucketName, '200', 'listLifecycleCurrents');
    return callback(null, res);
}

/**
 * listLifecycleCurrents - Return list of current versions/masters in bucket
 * @param {AuthInfo} authInfo            - Instance of AuthInfo class with
 *                                          requester's info
 * @param {array} locationConstraints    - array of location contraint
 * @param {object} request               - http request object
 * @param {function} log                 - Werelogs request logger
 * @param {function} callback            - callback to respond to http request
 *                                          with either error code or xml response body
 * @return {undefined}
 */
function listLifecycleCurrents(authInfo, locationConstraints, request, log, callback) {
    const params = request.query;
    const bucketName = request.bucketName;

    log.debug('processing request', { method: 'listLifecycleCurrents' });
    const requestMaxKeys = params['max-keys'] ?
        Number.parseInt(params['max-keys'], 10) : 1000;
    if (Number.isNaN(requestMaxKeys) || requestMaxKeys < 0) {
        monitoring.promMetrics(
            'GET', bucketName, 400, 'listLifecycleCurrents');
        return callback(errors.InvalidArgument);
    }
    const actualMaxKeys = Math.min(constants.listingHardLimit, requestMaxKeys);

    const minEntriesToBeScanned = 1;
    const { isValid, maxScannedLifecycleListingEntries } =
        validateMaxScannedEntries(params, config, minEntriesToBeScanned);
    if (!isValid) {
        monitoring.promMetrics('GET', bucketName, 400, 'listLifecycleCurrents');
        return callback(errors.InvalidArgument);
    }

    const excludedDataStoreName = params['excluded-data-store-name'];
    if (excludedDataStoreName && !locationConstraints[excludedDataStoreName]) {
        const errMsg = getLocationConstraintErrorMessage(excludedDataStoreName);
        log.error(`locationConstraint is invalid - ${errMsg}`, { locationConstraint: excludedDataStoreName });
        monitoring.promMetrics('GET', bucketName, 400, 'listLifecycleCurrents');

        return callback(errors.InvalidLocationConstraint.customizeDescription(errMsg));
    }

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'listLifecycleCurrents',
        request,
    };
    const listParams = {
        listingType: 'DelimiterCurrent',
        maxKeys: actualMaxKeys,
        prefix: params.prefix,
        beforeDate: params['before-date'],
        marker: params.marker,
        excludedDataStoreName,
        maxScannedLifecycleListingEntries,
    };

    return standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
        if (err) {
            log.debug('error processing request', { method: 'metadataValidateBucket', error: err });
            monitoring.promMetrics(
                'GET', bucketName, err.code, 'listLifecycleCurrents');
            return callback(err, null);
        }

        const vcfg = bucket.getVersioningConfiguration();
        const isBucketVersioned = vcfg && (vcfg.Status === 'Enabled' || vcfg.Status === 'Suspended');

        if (!requestMaxKeys) {
            const emptyList = {
                Contents: [],
                IsTruncated: false,
            };
            return handleResult(listParams, requestMaxKeys, authInfo,
                bucketName, emptyList, isBucketVersioned, log, callback);
        }

        return services.getLifecycleListing(bucketName, listParams, log,
        (err, list) => {
            if (err) {
                log.debug('error processing request', { method: 'services.getLifecycleListing', error: err });
                monitoring.promMetrics(
                    'GET', bucketName, err.code, 'listLifecycleCurrents');
                return callback(err, null);
            }

            return handleResult(listParams, requestMaxKeys, authInfo,
                bucketName, list, isBucketVersioned, log, callback);
        });
    });
}

module.exports = {
    listLifecycleCurrents,
};
