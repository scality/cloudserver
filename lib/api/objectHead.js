import { errors } from 'arsenal';

import { decryptVersionId } from './apiUtils/object/versioning';
import collectCorsHeaders from '../utilities/collectCorsHeaders';
import collectResponseHeaders from '../utilities/collectResponseHeaders';
import validateHeaders from '../utilities/validateHeaders';
import { pushMetric } from '../utapi/utilities';
import { getVersionIdResHeader } from './apiUtils/object/versioning';
import { metadataValidateBucketAndObj } from
'../metadata/metadataUtils';

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
export default function objectHead(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectHead' });
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;

    const decryptVidResult = decryptVersionId(request.query);
    if (decryptVidResult instanceof Error) {
        log.trace('invalid versionId query', {
            versionId: request.query.versionId,
            error: decryptVidResult,
        });
        return callback(decryptVidResult);
    }
    const versionId = decryptVidResult;

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
                return callback(err, corsHeaders);
            }
            if (!objMD) {
                const err = versionId ? errors.NoSuchVersion : errors.NoSuchKey;
                return callback(err, corsHeaders);
            }
            const verCfg = bucket.getVersioningConfiguration();
            if (objMD.isDeleteMarker) {
                const responseHeaders = Object.assign({},
                    { 'x-amz-delete-marker': true }, corsHeaders);
                if (!versionId) {
                    return callback(errors.NoSuchKey, responseHeaders);
                }
                // return MethodNotAllowed if requesting a specific
                // version that has a delete marker
                responseHeaders['x-amz-version-id'] =
                    getVersionIdResHeader(verCfg, objMD);
                return callback(errors.MethodNotAllowed, responseHeaders);
            }
            const headerValResult = validateHeaders(objMD, request.headers);
            if (headerValResult.error) {
                return callback(headerValResult.error, corsHeaders);
            }
            const responseHeaders =
                collectResponseHeaders(objMD, corsHeaders, verCfg);
            pushMetric('headObject', log, { authInfo, bucket: bucketName });
            return callback(null, responseHeaders);
        });
}
