const { errors, s3middleware } = require('arsenal');
const validateHeaders = s3middleware.validateConditionalHeaders;
const { parseRange } = require('arsenal/lib/network/http/utils');

const { decodeVersionId } = require('./apiUtils/object/versioning');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const collectResponseHeaders = require('../utilities/collectResponseHeaders');
const { pushMetric } = require('../utapi/utilities');
const { getVersionIdResHeader } = require('./apiUtils/object/versioning');
const { getPartNumber, getPartSize, getPartCountFromMd5 } =
    require('./apiUtils/object/partInfo');

const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { maximumAllowedPartCount } = require('../../constants');

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
        request,
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
            const headerValResult = validateHeaders(request.headers,
                objMD['last-modified'], objMD['content-md5']);
            if (headerValResult.error) {
                return callback(headerValResult.error, corsHeaders);
            }
            const responseHeaders = collectResponseHeaders(objMD, corsHeaders,
                verCfg);

            const objLength = (objMD.location === null ?
                0 : parseInt(objMD['content-length'], 10));

            let byteRange;
            if (request.headers.range) {
                const { range, error }
                    = parseRange(request.headers.range, objLength);
                if (error) {
                    return callback(error, corsHeaders);
                }
                responseHeaders['accept-ranges'] = 'bytes';
                if (range) {
                    byteRange = range;
                    responseHeaders['content-length'] =
                        range[1] - range[0] + 1;
                    responseHeaders['content-range'] =
                        `bytes ${range[0]}-${range[1]}/${objLength}`;
                }
            }
            const partNumber = getPartNumber(request.query);
            if (partNumber !== undefined) {
                if (byteRange) {
                    const error = errors.InvalidRequest
                        .customizeDescription('Cannot specify both Range ' +
                            'header and partNumber query parameter.');
                    return callback(error, corsHeaders);
                }
                if (Number.isNaN(partNumber)) {
                    const error = errors.InvalidArgument
                        .customizeDescription('Part number must be a number.');
                    return callback(error, corsHeaders);
                }
                if (partNumber < 1 || partNumber > maximumAllowedPartCount) {
                    return callback(errors.BadRequest, corsHeaders);
                }
                const partSize = getPartSize(objMD, partNumber);
                if (!partSize) {
                    return callback(errors.InvalidRange, corsHeaders);
                }
                responseHeaders['content-length'] = partSize;
                const partsCount = getPartCountFromMd5(objMD);
                if (partsCount) {
                    responseHeaders['x-amz-mp-parts-count'] = partsCount;
                }
            }
            pushMetric('headObject', log, {
                authInfo,
                bucket: bucketName,
                keys: [objectKey],
                versionId: objMD ? objMD.versionId : undefined,
                location: objMD ? objMD.dataStoreName : undefined,
            });
            return callback(null, responseHeaders);
        });
}

module.exports = objectHead;
