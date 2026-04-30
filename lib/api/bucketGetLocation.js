const { s3middleware } = require('arsenal');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const escapeForXml = s3middleware.escapeForXml;
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const monitoring = require('../utilities/monitoringHandler');

const REQUEST_TYPE = 'bucketGetLocation';
const METRICS_ACTION = 'getBucketLocation';

/**
 * Bucket Get Location - Get bucket locationConstraint configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketGetLocation(authInfo, request, log, callback) {
    const bucketName = request.bucketName;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethod || REQUEST_TYPE,
        request,
    };

    return standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin, request.method, bucket);
        if (err) {
            monitoring.promMetrics('GET', bucketName, err.code, METRICS_ACTION);
            return callback(err, null, corsHeaders);
        }

        let locationConstraint = bucket.getLocationConstraint();
        if (!locationConstraint || locationConstraint === 'us-east-1') {
            // AWS returns empty string if no region has been
            // provided or for us-east-1
            // Note: AWS JS SDK sends a request with locationConstraint us-east-1
            // if no locationConstraint provided.
            locationConstraint = '';
        }
        const xml = `<?xml version="1.0" encoding="UTF-8"?>
        <LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
            `${escapeForXml(locationConstraint)}</LocationConstraint>`;
        pushMetric(METRICS_ACTION, log, { authInfo, bucket: bucketName });
        monitoring.promMetrics('GET', bucketName, '200', METRICS_ACTION);
        return callback(null, xml, corsHeaders);
    });
}

module.exports = bucketGetLocation;
