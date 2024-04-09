const async = require('async');
const { errors, s3middleware } = require('arsenal');

const { allowedRestoreObjectRequestTierValues } = require('../../../../constants');
const coldStorage = require('./coldStorage');
const monitoring = require('../../../utilities/monitoringHandler');
const { pushMetric } = require('../../../utapi/utilities');
const { decodeVersionId } = require('./versioning');
const collectCorsHeaders = require('../../../utilities/collectCorsHeaders');
const { parseRestoreRequestXml } = s3middleware.objectRestore;


/**
 * Check if tier is supported
 * @param {object} restoreInfo - restore information
 * @returns {ArsenalError|undefined} return NotImplemented error if tier not support
 */
function checkTierSupported(restoreInfo) {
    if (!allowedRestoreObjectRequestTierValues.includes(restoreInfo.tier)) {
        return errors.NotImplemented;
    }
    return undefined;
}

/**
 * POST Object restore process
 *
 * @param {MetadataWrapper} metadata - metadata wrapper
 * @param {object} mdUtils - utility object to treat metadata
 * @param {AuthInfo} userInfo - Instance of AuthInfo class with requester's info
 * @param {IncomingMessage} request - request info
 * @param {object} log - Werelogs logger
 * @param {function} callback callback function
 * @return {undefined}
 */
function objectRestore(metadata, mdUtils, userInfo, request, log, callback) {
    const METHOD = 'objectRestore';

    const { bucketName, objectKey } = request;

    log.debug('processing request', { method: METHOD });

    const decodedVidResult = decodeVersionId(request.query);
    if (decodedVidResult instanceof Error) {
        log.trace('invalid versionId query',
            {
                method: METHOD,
                versionId: request.query.versionId,
                error: decodedVidResult,
            });
        return process.nextTick(() => callback(decodedVidResult));
    }

    let isObjectRestored = false;

    const mdValueParams = {
        authInfo: userInfo,
        bucketName,
        objectKey,
        versionId: decodedVidResult,
        requestType: request.apiMethods || 'restoreObject',
        request,
    };

    return async.waterfall([
            // get metadata of bucket and object
            function validateBucketAndObject(next) {
                return mdUtils.standardMetadataValidateBucketAndObj(mdValueParams, request.actionImplicitDenies,
                log, (err, bucketMD, objectMD) => {
                    if (err) {
                        log.trace('request authorization failed', { method: METHOD, error: err });
                        return next(err);
                    }
                    // Call back error if object metadata could not be obtained
                    if (!objectMD) {
                        const err = decodedVidResult ? errors.NoSuchVersion : errors.NoSuchKey;
                        log.trace('error no object metadata found', { method: METHOD, error: err });
                        return next(err, bucketMD);
                    }
                    // If object metadata is delete marker,
                    // call back NoSuchKey or MethodNotAllowed depending on specifying versionId
                    if (objectMD.isDeleteMarker) {
                        let err = errors.NoSuchKey;
                        if (decodedVidResult) {
                            err = errors.MethodNotAllowed;
                        }
                        log.trace('version is a delete marker', { method: METHOD, error: err });
                        return next(err, bucketMD, objectMD);
                    }
                    log.info('it acquired the object metadata.', {
                        'method': METHOD,
                    });
                    return next(null, bucketMD, objectMD);
                });
            },

            // generate restore param obj from xml of request body and check tier validity
            function parseRequestXmlAndCheckTier(bucketMD, objectMD, next) {
                log.trace('parsing object restore information');
                return parseRestoreRequestXml(request.post, log, (err, restoreInfo) => {
                    if (err) {
                        return next(err, bucketMD, objectMD, restoreInfo);
                    }
                    log.info('it parsed xml of the request body.', { method: METHOD, value: restoreInfo });
                    const checkTierResult = checkTierSupported(restoreInfo);
                    if (checkTierResult instanceof Error) {
                        return next(checkTierResult);
                    }
                    return next(null, bucketMD, objectMD, restoreInfo);
                });
            },
            // start restore process
            function startRestore(bucketMD, objectMD, restoreInfo, next) {
                return coldStorage.startRestore(objectMD, restoreInfo, log,
                    (err, _isObjectRestored) => {
                        isObjectRestored = _isObjectRestored;
                        return next(err, bucketMD, objectMD);
                    });
            },
            function updateObjectMD(bucketMD, objectMD, next) {
                const params = objectMD.versionId ? { versionId: objectMD.versionId } : {};
                metadata.putObjectMD(bucketMD.getName(), objectKey, objectMD, params,
                    log, err => next(err, bucketMD, objectMD));
            },
        ],
        (err, bucketMD) => {
            // generate CORS response header
            const responseHeaders = collectCorsHeaders(request.headers.origin, request.method, bucketMD);
            if (err) {
                log.trace('error processing request',
                    {
                        method: METHOD,
                        error: err,
                    });
                monitoring.promMetrics(
                    'POST', bucketName, err.code, 'restoreObject');
                return callback(err, err.code, responseHeaders);
            }
            pushMetric('restoreObject', log, {
                userInfo,
                bucket: bucketName,
            });
            if (isObjectRestored) {
                monitoring.promMetrics(
                    'POST', bucketName, '200', 'restoreObject');
                return callback(null, 200, responseHeaders);
            }
            monitoring.promMetrics(
                'POST', bucketName, '202', 'restoreObject');
            return callback(null, 202, responseHeaders);
        });
}


module.exports = objectRestore;
