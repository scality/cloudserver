import { errors } from 'arsenal';
import async from 'async';

import { parseRange } from './apiUtils/object/parseRange';
import collectCorsHeaders from '../utilities/collectCorsHeaders';
import collectResponseHeaders from '../utilities/collectResponseHeaders';
import services from '../services';
import validateHeaders from '../utilities/validateHeaders';
import { pushMetric } from '../utapi/utilities';

/**
 * GET Object - Get an object
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - normalized request object
 * @param {object} log - Werelogs instance
 * @param {function} callback - callback to function in route
 * @return {undefined}
 */
export default
function objectGet(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectGet' });
    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    let versionId = request.query ? request.query.versionId : undefined;
    versionId = versionId || undefined; // to smooth out versionId ''
    const mdValParams = {
        authInfo,
        bucketName,
        objectKey,
        versionId: versionId === 'null' ? undefined : versionId,
        requestType: 'objectGet',
        log,
    };

    async.waterfall([
        next => services.metadataValidateAuthorization(mdValParams,
        (err, bucket, objMD) => {
            const corsHeaders = collectCorsHeaders(request.headers.origin,
                    request.method, bucket);
            if (err) {
                log.debug('error processing request', { error: err });
                return next(err, null, corsHeaders);
            }
            if (!objMD) {
                return next(errors.NoSuchKey, null, corsHeaders);
            }
            if (versionId === undefined) {
                return next(null, bucket, objMD);
            }
            if (versionId !== 'null') {
                return next(null, bucket, objMD);
            }
            if (objMD.isNull) {
                return next(null, bucket, objMD);
            }
            if (objMD.nullVersionId === undefined) {
                return next(errors.NoSuchKey, null, corsHeaders);
            }
            mdValParams.versionId = objMD.nullVersionId;
            return services.metadataValidateAuthorization(mdValParams,
                (err, bucket, objMD) => {
                    if (err) {
                        return next(err, null, corsHeaders);
                    }
                    if (!objMD) {
                        return next(errors.NoSuchKey, null, corsHeaders);
                    }
                    return next(null, bucket, objMD);
                });
        }),
        (bucket, objMD, next) => {
            const corsHeaders = collectCorsHeaders(request.headers.origin,
                request.method, bucket);
            const headerValResult = validateHeaders(objMD, request.headers);
            if (headerValResult.error) {
                return next(headerValResult.error, null, corsHeaders);
            }
            const responseMetaHeaders = collectResponseHeaders(objMD,
                    corsHeaders);
            if (objMD.versionId) {
                responseMetaHeaders['x-amz-version-id'] = objMD.versionId;
            }
            if (objMD.isDeleteMarker) {
                // TODO check if versionId should also be included
                responseMetaHeaders['x-amz-delete-marker'] = true;
                if (versionId) {
                    return next(errors.MethodNotAllowed, null,
                            responseMetaHeaders);
                }
                return next(errors.NoSuchKey, null, responseMetaHeaders);
            }
            // 0 bytes file
            if (objMD.location === null) {
                if (request.headers.range) {
                    return next(errors.InvalidRange, null, corsHeaders);
                }
                pushMetric('getObject', log, {
                    authInfo,
                    bucket: bucketName,
                    newByteLength: 0,
                });
                return next(null, null, responseMetaHeaders);
            }
            let range;
            let maxContentLength;
            if (request.headers.range) {
                maxContentLength =
                  parseInt(responseMetaHeaders['Content-Length'], 10);
                responseMetaHeaders['Accept-Ranges'] = 'bytes';
                const parseRangeRes = parseRange(request.headers.range,
                  maxContentLength);
                range = parseRangeRes.range;
                const error = parseRangeRes.error;
                if (error) {
                    return next(error, null, corsHeaders);
                }
                if (range) {
                    // End of range should be included so + 1
                    responseMetaHeaders['Content-Length'] =
                        Math.min(maxContentLength - range[0],
                        range[1] - range[0] + 1);
                    responseMetaHeaders['Content-Range'] = `bytes ${range[0]}-`
                        + `${Math.min(maxContentLength - 1, range[1])}` +
                        `/${maxContentLength}`;
                }
            }
            // To provide for backwards compatibility before md-model-version 2,
            // need to handle cases where objMD.location is just a string
            const dataLocator = Array.isArray(objMD.location) ?
                objMD.location : [{ key: objMD.location }];
            // If have a data model before version 2, cannot support get range
            // for objects with multiple parts
            if (range && dataLocator.length > 1 &&
                dataLocator[0].start === undefined) {
                return next(errors.NotImplemented, null, corsHeaders);
            }
            if (objMD['x-amz-server-side-encryption']) {
                for (let i = 0; i < dataLocator.length; i++) {
                    dataLocator[i].masterKeyId =
                        objMD['x-amz-server-side-encryption-aws-kms-key-id'];
                    dataLocator[i].algorithm =
                        objMD['x-amz-server-side-encryption'];
                }
            }
            pushMetric('getObject', log, {
                authInfo,
                bucket: bucketName,
                newByteLength: responseMetaHeaders['Content-Length'],
            });
            return next(null, dataLocator, responseMetaHeaders, range);
        },
    ], callback);
}
