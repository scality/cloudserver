import { errors, versioning } from 'arsenal';
import async from 'async';

import collectCorsHeaders from '../utilities/collectCorsHeaders';
import collectResponseHeaders from '../utilities/collectResponseHeaders';
import services from '../services';
import validateHeaders from '../utilities/validateHeaders';
import { pushMetric } from '../utapi/utilities';

const VID = versioning.VersionID;

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
    let versionId = request.query ? request.query.versionId : undefined;
    versionId = versionId || undefined; // to smooth out versionId ''

    if (versionId && versionId !== 'null') {
        try {
            versionId = VID.decrypt(versionId);
        } catch (exception) { // eslint-disable-line
            return callback(errors.InvalidArgument
                .customizeDescription('Invalid version id specified'), null);
        }
    }

    const mdValParams = {
        authInfo,
        bucketName,
        objectKey,
        versionId: versionId === 'null' ? undefined : versionId,
        requestType: 'objectHead',
        log,
    };

    return async.waterfall([
        next => services.metadataValidateAuthorization(mdValParams,
        (err, bucket, objMD) => {
            const corsHeaders = collectCorsHeaders(request.headers.origin,
                request.method, bucket);
            if (err) {
                log.debug('error processing request', {
                    error: err,
                    method: 'metadataValidateAuthorization',
                });
                return next(err, corsHeaders);
            }
            if (!objMD) {
                return next(errors.NoSuchKey, corsHeaders);
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
                return next(errors.NoSuchKey, corsHeaders);
            }
            mdValParams.versionId = objMD.nullVersionId;
            return services.metadataValidateAuthorization(mdValParams,
                (err, bucket, objMD) => {
                    if (err) {
                        return next(err, corsHeaders);
                    }
                    if (!objMD) {
                        return next(errors.NoSuchKey, corsHeaders);
                    }
                    return next(null, bucket, objMD);
                });
        }),
        (bucket, objMD, next) => {
            const corsHeaders = collectCorsHeaders(request.headers.origin,
                request.method, bucket);
            const headerValResult = validateHeaders(objMD, request.headers);
            if (headerValResult.error) {
                return next(headerValResult.error, corsHeaders);
            }
            const responseHeaders = collectResponseHeaders(objMD, corsHeaders);
            if (versionId) {
                responseHeaders['x-amz-version-id'] = VID.encrypt(versionId);
            }
            pushMetric('headObject', log, { authInfo, bucket: bucketName });
            return next(null, responseHeaders);
        },
    ], callback);
}
