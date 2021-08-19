const async = require('async');
const { errors, s3middleware } = require('arsenal');

const { decodeVersionId, getVersionIdResHeader }
    = require('./apiUtils/object/versioning');

const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');

const { convertToXml } = s3middleware.objectLegalHold;

/**
 * Returns legal hold status of object
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function objectGetLegalHold(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectGetLegalHold' });

    const { bucketName, objectKey, query } = request;

    const decodedVidResult = decodeVersionId(query);
    if (decodedVidResult instanceof Error) {
        log.trace('invalid versionId query', {
            versionId: query.versionId,
            error: decodedVidResult,
        });
        return process.nextTick(() => callback(decodedVidResult));
    }
    const versionId = decodedVidResult;

    const metadataValParams = {
        authInfo,
        bucketName,
        objectKey,
        requestType: 'objectGetLegalHold',
        versionId,
        request,
    };

    return async.waterfall([
        next => metadataValidateBucketAndObj(metadataValParams, log,
            (err, bucket, objectMD) => {
                if (err) {
                    log.trace('request authorization failed',
                    { method: 'objectGetLegalHold', error: err });
                    return next(err);
                }
                if (!objectMD) {
                    const err = versionId ? errors.NoSuchVersion :
                        errors.NoSuchKey;
                    log.trace('error no object metadata found',
                        { method: 'objectGetLegalHold', error: err });
                    return next(err, bucket);
                }
                if (objectMD.isDeleteMarker) {
                    if (versionId) {
                        log.trace('requested version is delete marker',
                            { method: 'objectGetLegalHold' });
                        return next(errors.MethodNotAllowed);
                    }
                    log.trace('most recent version is delete marker',
                        { method: 'objectGetLegalHold' });
                    return next(errors.NoSuchKey);
                }
                if (!bucket.isObjectLockEnabled()) {
                    log.trace('object lock not enabled on bucket',
                        { method: 'objectGetRetention' });
                    return next(errors.InvalidRequest.customizeDescription(
                        'Bucket is missing Object Lock Configuration'));
                }
                return next(null, bucket, objectMD);
            }),
        (bucket, objectMD, next) => {
            const { legalHold } = objectMD;
            const xml = convertToXml(legalHold);
            if (xml === '') {
                return next(errors.NoSuchObjectLockConfiguration);
            }
            return next(null, bucket, xml, objectMD);
        },
    ], (err, bucket, xml, objectMD) => {
        const additionalResHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'objectGetLegalHold' });
        } else {
            pushMetric('getObjectLegalHold', log, {
                authInfo,
                bucket: bucketName,
                keys: [objectKey],
                versionId: objectMD ? objectMD.versionId : undefined,
                location: objectMD ? objectMD.dataStoreName : undefined,
            });
            const verCfg = bucket.getVersioningConfiguration();
            additionalResHeaders['x-amz-version-id'] =
                getVersionIdResHeader(verCfg, objectMD);
        }
        return callback(err, xml, additionalResHeaders);
    });
}

module.exports = objectGetLegalHold;
