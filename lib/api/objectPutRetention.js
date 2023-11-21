const async = require('async');
const { errors, s3middleware } = require('arsenal');

const { decodeVersionId, getVersionIdResHeader } =
  require('./apiUtils/object/versioning');
const { ObjectLockInfo, checkUserGovernanceBypass, hasGovernanceBypassHeader } =
    require('./apiUtils/object/objectLockHelpers');
const { standardMetadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const getReplicationInfo = require('./apiUtils/object/getReplicationInfo');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');

const { parseRetentionXml } = s3middleware.retention;
const REPLICATION_ACTION = 'PUT_RETENTION';

/**
 * Object Put Retention - Adds retention information to object
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function objectPutRetention(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectPutRetention' });

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
        versionId: reqVersionId,
        requestType: request.apiMethods || 'objectPutRetention',
        request,
    };

    return async.waterfall([
        next => {
            log.trace('parsing retention information');
            parseRetentionXml(request.post, log,
                (err, retentionInfo) => {
                    if (err) {
                        log.trace('error parsing retention information',
                            { error: err });
                        return next(err);
                    }
                    const remainingDays = Math.ceil(
                        (new Date(retentionInfo.date) - Date.now()) / (1000 * 3600 * 24));
                    metadataValParams.request.objectLockRetentionDays = remainingDays;
                    return next(null, retentionInfo);
                });
        },
        (retentionInfo, next) => standardMetadataValidateBucketAndObj(metadataValParams, request.actionImplicitDenies,
            log, (err, bucket, objectMD) => {
                if (err) {
                    log.trace('request authorization failed',
                        { method: 'objectPutRetention', error: err });
                    return next(err);
                }
                if (!objectMD) {
                    const err = reqVersionId ? errors.NoSuchVersion :
                        errors.NoSuchKey;
                    log.trace('error no object metadata found',
                        { method: 'objectPutRetention', error: err });
                    return next(err, bucket);
                }
                if (objectMD.isDeleteMarker) {
                    log.trace('version is a delete marker',
                        { method: 'objectPutRetention' });
                    // FIXME we should return a `x-amz-delete-marker: true` header,
                    // see S3C-7592
                    return next(errors.MethodNotAllowed, bucket);
                }
                if (!bucket.isObjectLockEnabled()) {
                    log.trace('object lock not enabled on bucket',
                        { method: 'objectPutRetention' });
                    return next(errors.InvalidRequest.customizeDescription(
                        'Bucket is missing Object Lock Configuration'
                    ), bucket);
                }
                return next(null, bucket, retentionInfo, objectMD);
            }),
        (bucket, retentionInfo, objectMD, next) => {
            const hasGovernanceBypass = hasGovernanceBypassHeader(request.headers);
            if (hasGovernanceBypass && authInfo.isRequesterAnIAMUser()) {
                return checkUserGovernanceBypass(request, authInfo, bucket, objectKey, log, err => {
                    if (err) {
                        if (err.is.AccessDenied) {
                            log.debug('user does not have BypassGovernanceRetention and object is locked');
                        }
                        return next(err, bucket);
                    }
                    return next(null, bucket, retentionInfo, hasGovernanceBypass, objectMD);
                });
            }
            return next(null, bucket, retentionInfo, hasGovernanceBypass, objectMD);
        },
        (bucket, retentionInfo, hasGovernanceBypass, objectMD, next) => {
            const objLockInfo = new ObjectLockInfo({
                mode: objectMD.retentionMode,
                date: objectMD.retentionDate,
                legalHold: objectMD.legalHold,
            });

            if (!objLockInfo.canModifyPolicy(retentionInfo, hasGovernanceBypass)) {
                return next(errors.AccessDenied, bucket);
            }

            return next(null, bucket, retentionInfo, objectMD);
        },
        (bucket, retentionInfo, objectMD, next) => {
            /* eslint-disable no-param-reassign */
            objectMD.retentionMode = retentionInfo.mode;
            objectMD.retentionDate = retentionInfo.date;
            const params = objectMD.versionId ?
                { versionId: objectMD.versionId } : {};
            const replicationInfo = getReplicationInfo(objectKey, bucket, true,
                0, REPLICATION_ACTION, objectMD);
            if (replicationInfo) {
                objectMD.replicationInfo = Object.assign({},
                    objectMD.replicationInfo, replicationInfo);
            }
            /* eslint-enable no-param-reassign */
            metadata.putObjectMD(bucket.getName(), objectKey, objectMD, params,
                log, err => next(err, bucket, objectMD));
        },
    ], (err, bucket, objectMD) => {
        const additionalResHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request',
                { error: err, method: 'objectPutRetention' });
        } else {
            pushMetric('putObjectRetention', log, {
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
        return callback(err, additionalResHeaders);
    });
}

module.exports = objectPutRetention;
