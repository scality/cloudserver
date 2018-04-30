const { errors, s3middleware } = require('arsenal');
const validateHeaders = s3middleware.validateConditionalHeaders;

const { decodeVersionId } = require('./apiUtils/object/versioning');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const collectResponseHeaders = require('../utilities/collectResponseHeaders');
const { pushMetric } = require('../utapi/utilities');
const { getVersionIdResHeader } = require('./apiUtils/object/versioning');
const monitoring = require('../utilities/monitoringHandler');

const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');

/**
 * HEAD Object - Same as Get Object but only respond with headers
 *(no actual body)
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - normalized request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to function in route
 * @return {undefined}
 *
 */
function objectHead(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectHead' });
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;

    const decodedVidResult = decodeVersionId(request.query);
    if (decodedVidResult instanceof Error) {
        log.trace('invalid versionId query', {
            versionId: request.query.versionId,
            error: decodedVidResult,
        });
        return callback(decodedVidResult);
    }
    const versionId = decodedVidResult;

    const mdValParams = {
        authInfo,
        bucketName,
        objectKey,
        versionId,
        requestType: 'objectHead',
    };

    return metadataValidateBucketAndObj(mdValParams, log,
        (err, bucket, objMD) => {
            const corsHeaders = collectCorsHeaders(request.headers.origin,
                request.method, bucket);
            if (err) {
                log.debug('error validating request', {
                    error: err,
                    method: 'objectHead',
                });
                monitoring.promMetrics(
                        'HEAD', bucketName, err.code, 'headObject');
                return callback(err, corsHeaders);
            }
            if (!objMD) {
                const err = versionId ? errors.NoSuchVersion : errors.NoSuchKey;
                monitoring.promMetrics(
                    'HEAD', bucketName, err.code, 'headObject');
                return callback(err, corsHeaders);
            }
            const verCfg = bucket.getVersioningConfiguration();
            if (objMD.isDeleteMarker) {
                const responseHeaders = Object.assign({},
                    { 'x-amz-delete-marker': true }, corsHeaders);
                if (!versionId) {
                    monitoring.promMetrics(
                        'HEAD', bucketName, 404, 'headObject');
                    return callback(errors.NoSuchKey, responseHeaders);
                }
                // return MethodNotAllowed if requesting a specific
                // version that has a delete marker
                responseHeaders['x-amz-version-id'] =
                    getVersionIdResHeader(verCfg, objMD);
                monitoring.promMetrics(
                    'HEAD', bucketName, 405, 'headObject');
                return callback(errors.MethodNotAllowed, responseHeaders);
            }
            const headerValResult = validateHeaders(request.headers,
                objMD['last-modified'], objMD['content-md5']);
            if (headerValResult.error) {
                return callback(headerValResult.error, corsHeaders);
            }
            const responseHeaders =
                collectResponseHeaders(objMD, corsHeaders, verCfg);
            pushMetric('headObject', log, { authInfo, bucket: bucketName });
            monitoring.promMetrics('HEAD', bucketName, '200', 'headObject');
            return callback(null, responseHeaders);
        });
}

module.exports = objectHead;
