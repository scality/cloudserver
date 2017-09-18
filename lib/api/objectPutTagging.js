const async = require('async');
const { errors, s3middleware } = require('arsenal');

const { decodeVersionId, getVersionIdResHeader } =
  require('./apiUtils/object/versioning');

const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const getReplicationInfo = require('./apiUtils/object/getReplicationInfo');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { config } = require('../Config');
const multipleBackendGateway = require('../data/multipleBackendGateway');
const { parseTagXml } = s3middleware.tagging;

/**
 * Object Put Tagging - Adds tag(s) to object
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function objectPutTagging(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectPutTagging' });

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
                     { method: 'objectPutTagging', error: err });
                  return next(err);
              }
              if (!objectMD) {
                  const err = reqVersionId ? errors.NoSuchVersion :
                      errors.NoSuchKey;
                  log.trace('error no object metadata found',
                    { method: 'objectPutTagging', error: err });
                  return next(err, bucket);
              }
              if (objectMD.isDeleteMarker) {
                  log.trace('version is a delete marker',
                  { method: 'objectPutTagging' });
                  return next(errors.MethodNotAllowed, bucket);
              }
              return next(null, bucket, objectMD);
          }),
        (bucket, objectMD, next) => {
            log.trace('parsing tag(s)');
            parseTagXml(request.post, log, (err, tags) =>
              next(err, bucket, tags, objectMD));
        },
        (bucket, tags, objectMD, next) => {
            // eslint-disable-next-line no-param-reassign
            objectMD.tags = tags;
            const params = objectMD.versionId ? { versionId:
              objectMD.versionId } : {};
            const replicationInfo =
                getReplicationInfo(objectKey, bucket, true);
            if (replicationInfo) {
                // eslint-disable-next-line no-param-reassign
                objectMD.replicationInfo = replicationInfo;
            }
            metadata.putObjectMD(bucket.getName(), objectKey, objectMD, params,
            log, err =>
                next(err, bucket, objectMD));
        },
        (bucket, objectMD, next) => {
            if (config.backends.data === 'multiple') {
                return multipleBackendGateway.objectTagging('Put', objectKey,
                bucket, objectMD, log, err => next(err, bucket, objectMD));
            }
            return next(null, bucket, objectMD);
        },
    ], (err, bucket, objectMD) => {
        const additionalResHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'objectPutTagging' });
        } else {
            pushMetric('putObjectTagging', log, {
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

module.exports = objectPutTagging;
