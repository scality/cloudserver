const async = require('async');
const { errors } = require('arsenal');

const { decodeVersionId, getVersionIdResHeader, getVersionSpecificMetadataOptions }
    = require('./apiUtils/object/versioning');

const { standardMetadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/metrics');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const getReplicationInfo = require('./apiUtils/object/getReplicationInfo');
const { data } = require('../data/wrapper');
const { config } = require('../Config');
const REPLICATION_ACTION = 'DELETE_TAGGING';

/**
 * Object Delete Tagging - Delete tag set from an object
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function objectDeleteTagging(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectDeleteTagging' });

    const bucketName = request.bucketName;
    const objectKey = request.objectKey;

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
        getDeleteMarker: true,
        requestType: 'objectDeleteTagging',
        request,
    };

    return async.waterfall([
        next => standardMetadataValidateBucketAndObj(metadataValParams, request.actionImplicitDenies, log,
          (err, bucket, objectMD) => {
              if (err) {
                  log.trace('request authorization failed',
                     { method: 'objectDeleteTagging', error: err });
                  return next(err);
              }
              if (!objectMD) {
                  const err = reqVersionId ? errors.NoSuchVersion :
                      errors.NoSuchKey;
                  log.trace('error no object metadata found',
                    { method: 'objectDeleteTagging', error: err });
                  return next(err, bucket);
              }
              if (objectMD.isDeleteMarker) {
                  log.trace('version is a delete marker',
                  { method: 'objectDeleteTagging' });
                  // FIXME we should return a `x-amz-delete-marker: true` header,
                  // see S3C-7592
                  return next(errors.MethodNotAllowed, bucket);
              }
              return next(null, bucket, objectMD);
          }),
        (bucket, objectMD, next) => {
            // eslint-disable-next-line no-param-reassign
            objectMD.tags = {};
            const params = getVersionSpecificMetadataOptions(objectMD, config.nullVersionCompatMode);
            const replicationInfo = getReplicationInfo(objectKey, bucket, true,
                0, REPLICATION_ACTION, objectMD);
            if (replicationInfo) {
                // eslint-disable-next-line no-param-reassign
                objectMD.replicationInfo = Object.assign({},
                    objectMD.replicationInfo, replicationInfo);
            }
            // eslint-disable-next-line no-param-reassign
            objectMD.originOp = 's3:ObjectTagging:Delete';
            metadata.putObjectMD(bucket.getName(), objectKey, objectMD, params,
            log, err =>
                next(err, bucket, objectMD));
        },
        (bucket, objectMD, next) =>
            // if external backends handles tagging
            data.objectTagging('Delete', objectKey, bucket, objectMD,
            log, err => next(err, bucket, objectMD)),
    ], (err, bucket, objectMD) => {
        const additionalResHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'objectDeleteTagging' });
            monitoring.promMetrics(
                'DELETE', bucketName, err.code, 'deleteObjectTagging');
        } else {
            pushMetric('deleteObjectTagging', log, {
                authInfo,
                bucket: bucketName,
                keys: [objectKey],
                versionId: objectMD ? objectMD.versionId : undefined,
                location: objectMD ? objectMD.dataStoreName : undefined,
            });
            monitoring.promMetrics(
                'DELETE', bucketName, '200', 'deleteObjectTagging');
            const verCfg = bucket.getVersioningConfiguration();
            additionalResHeaders['x-amz-version-id'] =
                getVersionIdResHeader(verCfg, objectMD);
        }
        return callback(err, additionalResHeaders);
    });
}

module.exports = objectDeleteTagging;
