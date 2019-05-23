const async = require('async');
const { errors } = require('arsenal');

const { decodeVersionId, getVersionIdResHeader }
    = require('./apiUtils/object/versioning');

const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const getReplicationInfo = require('./apiUtils/object/getReplicationInfo');
const { config } = require('../Config');
const multipleBackendGateway = require('../data/multipleBackendGateway');
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
        requestType: 'bucketOwnerAction',
        versionId: reqVersionId,
    };

    return async.waterfall([
        next => metadataValidateBucketAndObj(metadataValParams, log,
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
                  return next(errors.MethodNotAllowed, bucket);
              }
              return next(null, bucket, objectMD);
          }),
        (bucket, objectMD, next) => {
            // eslint-disable-next-line no-param-reassign
            objectMD.tags = {};
            const params = objectMD.versionId ? { versionId:
              objectMD.versionId } : {};
            const replicationInfo = getReplicationInfo(objectKey, bucket, true,
                0, REPLICATION_ACTION, objectMD);
            if (replicationInfo) {
                // eslint-disable-next-line no-param-reassign
                objectMD.replicationInfo = Object.assign({},
                    objectMD.replicationInfo, replicationInfo);
            }
            metadata.putObjectMD(bucket.getName(), objectKey, objectMD, params,
            log, err =>
                next(err, bucket, objectMD));
        },
        (bucket, objectMD, next) => {
            if (config.backends.data === 'multiple') {
                return multipleBackendGateway.objectTagging('Delete', objectKey,
                bucket, objectMD, log, err => next(err, bucket, objectMD));
            }
            return next(null, bucket, objectMD);
        },
    ], (err, bucket, objectMD) => {
        const additionalResHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'objectDeleteTagging' });
        } else {
            pushMetric('deleteObjectTagging', log, {
                authInfo,
                bucket: bucketName,
                keys: [objectKey],
            });
            const verCfg = bucket.getVersioningConfiguration();
            additionalResHeaders['x-amz-version-id'] =
                getVersionIdResHeader(verCfg, objectMD);
        }
        return callback(err, additionalResHeaders);
    });
}

module.exports = objectDeleteTagging;
