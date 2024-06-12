const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { standardMetadataValidateBucket } = require('../metadata/metadataUtils');
const opentelemetry = require('@opentelemetry/api');

const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/metrics');

/**
 * Determine if bucket exists and if user has permission to access it
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to respond to http request
 *  with either error code or success
 * @return {undefined}
 */
function bucketHead(authInfo, request, log, callback, parentSpan) {
    parentSpan.addEvent('Cloudserver::bucketGet() processing Head Bucket request');
    parentSpan.setAttribute('code.function', 'bucketHead()');
    parentSpan.setAttribute('code.filepath', 'lib/api/bucketHead.js');
    parentSpan.setAttribute('code.lineno', 20);
    const ctx = opentelemetry.trace.setSpan(
        opentelemetry.context.active(),
        parentSpan,
    );
    log.debug('processing request', { method: 'bucketHead' });
    const bucketName = request.bucketName;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: request.apiMethods || 'bucketHead',
        request,
    };
    standardMetadataValidateBucket(metadataValParams, request.actionImplicitDenies, log, (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            monitoring.promMetrics(
                'HEAD', bucketName, err.code, 'headBucket');
            return callback(err, corsHeaders);
        }
        pushMetric('headBucket', log, {
            authInfo,
            bucket: bucketName,
        });
        const headers = {
            'x-amz-bucket-region': bucket.getLocationConstraint(),
        };
        return callback(null, Object.assign(corsHeaders, headers));
    }, ctx);
}

module.exports = bucketHead;
