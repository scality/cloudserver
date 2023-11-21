const async = require('async');
const { errors, s3middleware } = require('arsenal');

const { decodeVersionId, getVersionIdResHeader, getVersionSpecificMetadataOptions } =
  require('./apiUtils/object/versioning');

const { standardMetadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const monitoring = require('../utilities/metrics');
const getReplicationInfo = require('./apiUtils/object/getReplicationInfo');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { data } = require('../data/wrapper');

const { parseTagXml } = s3middleware.tagging;
const { config } = require('../Config');
const REPLICATION_ACTION = 'PUT_TAGGING';

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

    const { bucketName } = request;
    const { objectKey } = request;

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
        requestType: request.apiMethods || 'objectPutTagging',
        request,
    };

    return async.waterfall([
        next => standardMetadataValidateBucketAndObj(metadataValParams, request.actionImplicitDenies, log,
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
                  // FIXME we should return a `x-amz-delete-marker: true` header,
                  // see S3C-7592
                  return next(errors.MethodNotAllowed, bucket);
              }
              return next(null, bucket, objectMD);
          }),
        (bucket, objectMD, next) => {
            log.trace('parsing tag(s)');
            parseTagXml(request.post, log, (err, tags) => next(err, bucket, tags, objectMD));
        },
        (bucket, tags, objectMD, next) => {
            // eslint-disable-next-line no-param-reassign
            objectMD.tags = tags;
            const params = getVersionSpecificMetadataOptions(objectMD, config.nullVersionCompatMode);
            const replicationInfo = getReplicationInfo(objectKey, bucket, true,
                0, REPLICATION_ACTION, objectMD);
            if (replicationInfo) {
                // eslint-disable-next-line no-param-reassign
                objectMD.replicationInfo = Object.assign({},
                    objectMD.replicationInfo, replicationInfo);
            }
            // eslint-disable-next-line no-param-reassign
            objectMD.originOp = 's3:ObjectTagging:Put';
            metadata.putObjectMD(bucket.getName(), objectKey, objectMD, params,
                log, err => next(err, bucket, objectMD));
        },
        // if external backend handles tagging
        (bucket, objectMD, next) => data.objectTagging('Put', objectKey, bucket, objectMD,
            log, err => next(err, bucket, objectMD)),
    ], (err, bucket, objectMD) => {
        const additionalResHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'objectPutTagging' });
            monitoring.promMetrics('PUT', bucketName, err.code,
                'putObjectTagging');
        } else {
            pushMetric('putObjectTagging', log, {
                authInfo,
                bucket: bucketName,
                keys: [objectKey],
                versionId: objectMD ? objectMD.versionId : undefined,
                location: objectMD ? objectMD.dataStoreName : undefined,
            });
            monitoring.promMetrics(
                'PUT', bucketName, '200', 'putObjectTagging');
            const verCfg = bucket.getVersioningConfiguration();
            additionalResHeaders['x-amz-version-id'] = getVersionIdResHeader(verCfg, objectMD);
        }
        return callback(err, additionalResHeaders);
    });
}

module.exports = objectPutTagging;
