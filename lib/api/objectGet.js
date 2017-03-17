import { errors } from 'arsenal';
import { parseRange } from 'arsenal/lib/network/http/utils';

import { decodeVersionId } from './apiUtils/object/versioning';
import collectCorsHeaders from '../utilities/collectCorsHeaders';
import collectResponseHeaders from '../utilities/collectResponseHeaders';
import validateHeaders from '../utilities/validateHeaders';
import { pushMetric } from '../utapi/utilities';
import { getVersionIdResHeader } from './apiUtils/object/versioning';
import { metadataValidateBucketAndObj } from
'../metadata/metadataUtils';

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
        requestType: 'objectGet',
    };

    return metadataValidateBucketAndObj(mdValParams, log,
    (err, bucket, objMD) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.debug('error processing request', {
                error: err,
                method: 'metadataValidateBucketAndObj',
            });
            return callback(err, null, corsHeaders);
        }
        if (!objMD) {
            const err = versionId ? errors.NoSuchVersion : errors.NoSuchKey;
            return callback(err, null, corsHeaders);
        }
        const verCfg = bucket.getVersioningConfiguration();
        if (objMD.isDeleteMarker) {
            const responseMetaHeaders = Object.assign({},
                { 'x-amz-delete-marker': true }, corsHeaders);
            if (!versionId) {
                return callback(errors.NoSuchKey, null, responseMetaHeaders);
            }
            // return MethodNotAllowed if requesting a specific
            // version that has a delete marker
            responseMetaHeaders['x-amz-version-id'] =
                getVersionIdResHeader(verCfg, objMD);
            return callback(errors.MethodNotAllowed, null,
                responseMetaHeaders);
        }
        const headerValResult = validateHeaders(objMD, request.headers);
        if (headerValResult.error) {
            return callback(headerValResult.error, null, corsHeaders);
        }
        const responseMetaHeaders = collectResponseHeaders(objMD,
            corsHeaders, verCfg);

        const objLength = (objMD.location === null ?
                           0 : parseInt(objMD['content-length'], 10));
        let byteRange;
        if (request.headers.range) {
            const { range, error } = parseRange(request.headers.range,
                                                objLength);
            if (error) {
                return callback(error, null, corsHeaders);
            }
            responseMetaHeaders['Accept-Ranges'] = 'bytes';
            if (range) {
                byteRange = range;
                // End of range should be included so + 1
                responseMetaHeaders['Content-Length'] =
                    range[1] - range[0] + 1;
                responseMetaHeaders['Content-Range'] =
                    `bytes ${range[0]}-${range[1]}/${objLength}`;
            }
        }
        let dataLocator = null;
        if (objMD.location !== null) {
            // To provide for backwards compatibility before
            // md-model-version 2, need to handle cases where
            // objMD.location is just a string
            dataLocator = Array.isArray(objMD.location) ?
                objMD.location : [{ key: objMD.location }];
            // If have a data model before version 2, cannot support
            // get range for objects with multiple parts
            if (byteRange && dataLocator.length > 1 &&
                dataLocator[0].start === undefined) {
                return callback(errors.NotImplemented, null, corsHeaders);
            }
            if (objMD['x-amz-server-side-encryption']) {
                for (let i = 0; i < dataLocator.length; i++) {
                    dataLocator[i].masterKeyId =
                        objMD['x-amz-server-side-encryption-aws-kms-key-id'];
                    dataLocator[i].algorithm =
                        objMD['x-amz-server-side-encryption'];
                }
            }
        }
        pushMetric('getObject', log, {
            authInfo,
            bucket: bucketName,
            newByteLength: responseMetaHeaders['Content-Length'],
        });
        return callback(null, dataLocator, responseMetaHeaders, byteRange);
    });
}
