const { errors } = require('arsenal');
const constants = require('../../../constants');
const services = require('../../services');
const { metadataValidateBucket } = require('../../metadata/metadataUtils');
const { pushMetric } = require('../../utapi/utilities');
const monitoring = require('../../utilities/monitoringHandler');
const { processCurrents } = require('../apiUtils/object/lifecycle');


function handleResult(listParams, requestMaxKeys, authInfo,
    bucketName, list, log, callback) {
    // eslint-disable-next-line no-param-reassign
    listParams.maxKeys = requestMaxKeys;
    // eslint-disable-next-line no-param-reassign
    const res = processCurrents(bucketName, listParams, list);

    pushMetric('listLifecycleCurrents', log, { authInfo, bucket: bucketName });
    monitoring.promMetrics('GET', bucketName, '200', 'listLifecycleCurrents');
    return callback(null, res);
}

/**
 * listLifecycleCurrents - Return list of current versions/masters in bucket
 * @param  {AuthInfo} authInfo - Instance of AuthInfo class with
 *                               requester's info
 * @param  {object} request    - http request object
 * @param  {function} log      - Werelogs request logger
 * @param  {function} callback - callback to respond to http request
 *                               with either error code or xml response body
 * @return {undefined}
 */
function listLifecycleCurrents(authInfo, request, log, callback) {
    const params = request.query;
    const bucketName = request.bucketName;

    log.debug('processing request', { method: 'listLifecycleCurrents' });
    const requestMaxKeys = params['max-keys'] ?
        Number.parseInt(params['max-keys'], 10) : 1000;
    if (Number.isNaN(requestMaxKeys) || requestMaxKeys < 0) {
        monitoring.promMetrics(
            'GET', bucketName, 400, 'listBucket');
        return callback(errors.InvalidArgument);
    }
    const actualMaxKeys = Math.min(constants.listingHardLimit, requestMaxKeys);

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
        marker: params['key-marker'],
    };

    return metadataValidateBucket(metadataValParams, log, err => {
        if (err) {
            log.debug('error processing request', { method: 'metadataValidateBucket', error: err });
            monitoring.promMetrics(
                'GET', bucketName, err.code, 'listLifecycleCurrents');
            return callback(err, null);
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
                log.debug('error processing request', {  method: 'services.getLifecycleListing', error: err });
                monitoring.promMetrics(
                    'GET', bucketName, err.code, 'listLifecycleCurrents');
                return callback(err, null);
            }
            return handleResult(listParams, requestMaxKeys, authInfo,
                bucketName, list, log, callback);
        });
    });
}

module.exports = {
    listLifecycleCurrents,
};
