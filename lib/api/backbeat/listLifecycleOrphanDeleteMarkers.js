const { errors } = require('arsenal');
const constants = require('../../../constants');
const services = require('../../services');
const { metadataValidateBucket } = require('../../metadata/metadataUtils');
const { pushMetric } = require('../../utapi/utilities');
const monitoring = require('../../utilities/monitoringHandler');
const { processOrphans } = require('../apiUtils/object/lifecycle');

function handleResult(listParams, requestMaxKeys, authInfo,
    bucketName, list, log, callback) {
    // eslint-disable-next-line no-param-reassign
    listParams.maxKeys = requestMaxKeys;
    const res = processOrphans(bucketName, listParams, list);

    pushMetric('listLifecycleOrphanDeleteMarkers', log, { authInfo, bucket: bucketName });
    monitoring.promMetrics('GET', bucketName, '200', 'listLifecycleOrphanDeleteMarkers');
    return callback(null, res);
}

/**
 * listLifecycleOrphanDeleteMarkers - Return list of expired object delete marker in bucket
 * @param {AuthInfo} authInfo            - Instance of AuthInfo class with
 *                                         requester's info
 * @param {array} locationConstraints    - array of location contraint
 * @param {object} request               - http request object
 * @param {function} log                 - Werelogs request logger
 * @param {function} callback            - callback to respond to http request
 *                                          with either error code or xml response body
 * @return {undefined}
 */
function listLifecycleOrphanDeleteMarkers(authInfo, locationConstraints, request, log, callback) {
    const params = request.query;
    const bucketName = request.bucketName;

    log.debug('processing request', { method: 'listLifecycleOrphanDeleteMarkers' });
    const requestMaxKeys = params['max-keys'] ?
        Number.parseInt(params['max-keys'], 10) : 1000;
    if (Number.isNaN(requestMaxKeys) || requestMaxKeys < 0) {
        monitoring.promMetrics(
            'GET', bucketName, 400, 'listLifecycleOrphanDeleteMarkers');
        return callback(errors.InvalidArgument);
    }
    const actualMaxKeys = Math.min(constants.listingHardLimit, requestMaxKeys);

    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'listLifecycleOrphanDeleteMarkers',
        request,
    };
    const listParams = {
        listingType: 'DelimiterOrphanDeleteMarker',
        maxKeys: actualMaxKeys,
        prefix: params.prefix,
        beforeDate: params['before-date'],
        marker: params.marker,
    };

    return metadataValidateBucket(metadataValParams, log, (err, bucket) => {
        if (err) {
            log.debug('error processing request', { method: 'metadataValidateBucket', error: err });
            monitoring.promMetrics(
                'GET', bucketName, err.code, 'listLifecycleOrphanDeleteMarkers');
            return callback(err, null);
        }

        const vcfg = bucket.getVersioningConfiguration();
        const isBucketVersioned = vcfg && (vcfg.Status === 'Enabled' || vcfg.Status === 'Suspended');
        if (!isBucketVersioned) {
            log.debug('bucket is not versioned or suspended');
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
                log.debug('error processing request', { error: err });
                monitoring.promMetrics(
                    'GET', bucketName, err.code, 'listLifecycleOrphanDeleteMarkers');
                return callback(err, null);
            }
            return handleResult(listParams, requestMaxKeys, authInfo,
                bucketName, list, log, callback);
        });
    });
}

module.exports = {
    listLifecycleOrphanDeleteMarkers,
};
