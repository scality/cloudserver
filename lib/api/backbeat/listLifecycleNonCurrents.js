const { errors, versioning } = require('arsenal');
const constants = require('../../../constants');
const services = require('../../services');
const { metadataValidateBucket } = require('../../metadata/metadataUtils');
const { pushMetric } = require('../../utapi/utilities');
const versionIdUtils = versioning.VersionID;
const monitoring = require('../../utilities/metrics');
const { getLocationConstraintErrorMessage, processNonCurrents } = require('../apiUtils/object/lifecycle');

function handleResult(listParams, requestMaxKeys, authInfo,
    bucketName, list, log, callback) {
    // eslint-disable-next-line no-param-reassign
    listParams.maxKeys = requestMaxKeys;
    const res = processNonCurrents(bucketName, listParams, list);

    pushMetric('listLifecycleNonCurrents', log, { authInfo, bucket: bucketName });
    monitoring.promMetrics('GET', bucketName, '200', 'listLifecycleNonCurrents');
    return callback(null, res);
}

/**
 * listLifecycleNonCurrents - Return list of non-current versions in bucket
 * @param {AuthInfo} authInfo           - Instance of AuthInfo class with
 *                                         requester's info
 * @param {array} locationConstraints    - array of location contraint
 * @param {object} request              - http request object
 * @param {function} log                - Werelogs request logger
 * @param {function} callback           - callback to respond to http request
 *                                          with either error code or xml response body
 * @return {undefined}
 */
function listLifecycleNonCurrents(authInfo, locationConstraints, request, log, callback) {
    const params = request.query;
    const bucketName = request.bucketName;

    log.debug('processing request', { method: 'listLifecycleNonCurrents' });
    const requestMaxKeys = params['max-keys'] ?
        Number.parseInt(params['max-keys'], 10) : 1000;
    if (Number.isNaN(requestMaxKeys) || requestMaxKeys < 0) {
        monitoring.promMetrics(
            'GET', bucketName, 400, 'listLifecycleNonCurrents');
        return callback(errors.InvalidArgument);
    }
    const actualMaxKeys = Math.min(constants.listingHardLimit, requestMaxKeys);

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
        requestType: 'listLifecycleNonCurrents',
        request,
    };
    const listParams = {
        listingType: 'DelimiterNonCurrent',
        maxKeys: actualMaxKeys,
        prefix: params.prefix,
        beforeDate: params['before-date'],
        keyMarker: params['key-marker'],
        excludedDataStoreName,
    };

    listParams.versionIdMarker = params['version-id-marker'] ?
        versionIdUtils.decode(params['version-id-marker']) : undefined;

    return metadataValidateBucket(metadataValParams, log, (err, bucket) => {
        if (err) {
            log.debug('error processing request', {  method: 'metadataValidateBucket', error: err });
            monitoring.promMetrics(
                'GET', bucketName, err.code, 'listLifecycleNonCurrents');
            return callback(err, null);
        }

        const vcfg = bucket.getVersioningConfiguration();
        const isBucketVersioned = vcfg && (vcfg.Status === 'Enabled' || vcfg.Status === 'Suspended');
        if (!isBucketVersioned) {
            log.debug('bucket is not versioned');
            return callback(errors.InvalidRequest.customizeDescription(
                'bucket is not versioned'), null);
        }

        if (!requestMaxKeys) {
            const emptyList = {
                Contents: [],
                IsTruncated: false,
            };
            return handleResult(listParams, requestMaxKeys, authInfo,
                bucketName, emptyList, log, callback);
        }

        return services.getLifecycleListing(bucketName, listParams, log,
        (err, list) => {
            if (err) {
                log.debug('error processing request', { method: 'services.getLifecycleListing', error: err });
                monitoring.promMetrics(
                    'GET', bucketName, err.code, 'listLifecycleNonCurrents');
                return callback(err, null);
            }
            return handleResult(listParams, requestMaxKeys, authInfo,
                bucketName, list, log, callback);
        });
    });
}

module.exports = {
    listLifecycleNonCurrents,
};
