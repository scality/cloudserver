const async = require('async');
const { errors, s3middleware } = require('arsenal');

const { decodeVersionId, getVersionIdResHeader }
    = require('./apiUtils/object/versioning');

const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');

const { convertToXml } = s3middleware.retention;

/**
 * Object Get Retention - Return retention info for object
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function objectGetRetention(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectGetRetention' });

    const { bucketName, objectKey } = request;

    const decodedVidResult = decodeVersionId(request.query);
    if (decodedVidResult instanceof Error) {
        log.trace('invalid versionId query', {
            versionId: request.query.versionId,
            error: decodedVidResult,
        });
        return process.nextTick(() => callback(decodedVidResult));
    }
    const reqVersionId = decodedVidResult;

    const metadataValParams = {
        authInfo,
        bucketName,
        objectKey,
        requestType: 'objectGetRetention',
        versionId: reqVersionId,
        request,
    };

    return async.waterfall([
        next => metadataValidateBucketAndObj(metadataValParams, log,
            (err, bucket, objectMD) => {
                if (err) {
                    log.trace('request authorization failed',
                        { method: 'objectGetRetention', error: err });
                    return next(err);
                }
                if (!objectMD) {
                    const err = reqVersionId ? errors.NoSuchVersion :
                        errors.NoSuchKey;
                    log.trace('error no object metadata found',
                        { method: 'objectGetRetention', error: err });
                    return next(err, bucket);
                }
                if (objectMD.isDeleteMarker) {
                    if (reqVersionId) {
                        log.trace('requested version is delete marker',
                            { method: 'objectGetRetention' });
                        return next(errors.MethodNotAllowed);
                    }
                    log.trace('most recent version is delete marker',
                        { method: 'objectGetRetention' });
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
            const { retentionMode, retentionDate } = objectMD;
            if (!retentionMode || !retentionDate) {
                return next(errors.NoSuchObjectLockConfiguration);
            }
            const xml = convertToXml(retentionMode, retentionDate);
            return next(null, bucket, xml, objectMD);
        },
    ], (err, bucket, xml, objectMD) => {
        const additionalResHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'objectGetRetention' });
        } else {
            pushMetric('getObjectRetention', log, {
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

module.exports = objectGetRetention;
