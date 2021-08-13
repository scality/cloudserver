const async = require('async');
const { errors, s3middleware } = require('arsenal');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { decodeVersionId, getVersionIdResHeader } =
  require('./apiUtils/object/versioning');
const getReplicationInfo = require('./apiUtils/object/getReplicationInfo');
const metadata = require('../metadata/wrapper');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');

const { parseLegalHoldXml } = s3middleware.objectLegalHold;

const REPLICATION_ACTION = 'PUT_LEGAL_HOLD';

/**
 * Object Put Legal Hold - Sets legal hold status of object
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function objectPutLegalHold(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectPutLegalHold' });

    const { bucketName, objectKey } = request;

    const decodedVidResult = decodeVersionId(request.query);
    if (decodedVidResult instanceof Error) {
        log.trace('invalid versionId query', {
            versionId: request.query.versionId,
            error: decodedVidResult,
        });
        return process.nextTick(() => callback(decodedVidResult));
    }
    const versionId = decodedVidResult;

    const metadataValParams = {
        authInfo,
        bucketName,
        objectKey,
        requestType: 'objectPutLegalHold',
        versionId,
        request,
    };

    return async.waterfall([
        next => metadataValidateBucketAndObj(metadataValParams, log,
        (err, bucket, objectMD) => {
            if (err) {
                log.trace('request authorization failed',
                    { method: 'objectPutLegalHold', error: err });
                return next(err);
            }
            if (!objectMD) {
                const err = versionId ? errors.NoSuchVersion :
                    errors.NoSuchKey;
                log.trace('error no object metadata found',
                    { method: 'objectPutLegalHold', error: err });
                return next(err, bucket);
            }
            if (objectMD.isDeleteMarker) {
                log.trace('version is a delete marker',
                    { method: 'objectPutLegalHold' });
                return next(errors.MethodNotAllowed, bucket);
            }
            if (!bucket.isObjectLockEnabled()) {
                log.trace('object lock not enabled on bucket',
                    { method: 'objectPutLegalHold' });
                return next(errors.InvalidRequest.customizeDescription(
                    'Bucket is missing Object Lock Configuration'
                ), bucket);
            }
            return next(null, bucket, objectMD);
        }),
        (bucket, objectMD, next) => {
            log.trace('parsing legal hold');
            parseLegalHoldXml(request.post, log, (err, res) =>
                next(err, bucket, res, objectMD));
        },
        (bucket, legalHold, objectMD, next) => {
            // eslint-disable-next-line no-param-reassign
            objectMD.legalHold = legalHold;
            const params = objectMD.versionId ?
                { versionId: objectMD.versionId } : {};
            const replicationInfo = getReplicationInfo(objectKey, bucket, true,
                0, REPLICATION_ACTION, objectMD);
            if (replicationInfo) {
                // eslint-disable-next-line no-param-reassign
                objectMD.replicationInfo = Object.assign({},
                    objectMD.replicationInfo, replicationInfo);
            }
            metadata.putObjectMD(bucket.getName(), objectKey, objectMD, params,
                log, err => next(err, bucket, objectMD));
        },
    ], (err, bucket, objectMD) => {
        const additionalResHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request',
                { error: err, method: 'objectPutLegalHold' });
        } else {
            pushMetric('putObjectLegalHold', log, {
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

module.exports = objectPutLegalHold;
