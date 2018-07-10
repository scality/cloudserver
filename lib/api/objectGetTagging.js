const async = require('async');
const { errors, s3middleware } = require('arsenal');

const { decodeVersionId, getVersionIdResHeader }
    = require('./apiUtils/object/versioning');

const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { convertToXml } = s3middleware.tagging;
const monitoring = require('../utilities/monitoringHandler');

/**
 * Object Get Tagging - Return tag for object
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function objectGetTagging(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'objectGetTagging' });

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
                  { method: 'objectGetTagging', error: err });
                  return next(err);
              }
              if (!objectMD) {
                  const err = reqVersionId ? errors.NoSuchVersion :
                      errors.NoSuchKey;
                  log.trace('error no object metadata found',
                  { method: 'objectGetTagging', error: err });
                  return next(err, bucket);
              }
              if (objectMD.isDeleteMarker) {
                  if (reqVersionId) {
                      log.trace('requested version is delete marker',
                      { method: 'objectGetTagging' });
                      return next(errors.MethodNotAllowed);
                  }
                  log.trace('most recent version is delete marker',
                  { method: 'objectGetTagging' });
                  return next(errors.NoSuchKey);
              }
              return next(null, bucket, objectMD);
          }),
        (bucket, objectMD, next) => {
            const tags = objectMD.tags;
            const xml = convertToXml(tags);
            next(null, bucket, xml, objectMD);
        },
    ], (err, bucket, xml, objectMD) => {
        const additionalResHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'objectGetTagging' });
            monitoring.promMetrics(
                    'GET', bucketName, err.code, 'getObjectTagging');
        } else {
            pushMetric('getObjectTagging', log, {
                authInfo,
                bucket: bucketName,
            });
            monitoring.promMetrics(
                'GET', bucketName, '200', 'getObjectTagging');
            const verCfg = bucket.getVersioningConfiguration();
            additionalResHeaders['x-amz-version-id'] =
                getVersionIdResHeader(verCfg, objectMD);
        }
        return callback(err, xml, additionalResHeaders);
    });
}

module.exports = objectGetTagging;
