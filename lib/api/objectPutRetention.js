const async = require('async');
const { errors, s3middleware, auth, policies } = require('arsenal');

const vault = require('../auth/vault');
const { decodeVersionId, getVersionIdResHeader } =
  require('./apiUtils/object/versioning');
const { validateObjectLockUpdate } =
    require('./apiUtils/object/objectLockHelpers');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const getReplicationInfo = require('./apiUtils/object/getReplicationInfo');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { config } = require('../Config');

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
        requestType: 'objectPutRetention',
        versionId: reqVersionId,
        request,
    };

    return async.waterfall([
        next => metadataValidateBucketAndObj(metadataValParams, log,
        (err, bucket, objectMD) => {
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
                return next(errors.MethodNotAllowed, bucket);
            }
            if (!bucket.isObjectLockEnabled()) {
                log.trace('object lock not enabled on bucket',
                    { method: 'objectPutRetention' });
                return next(errors.InvalidRequest.customizeDescription(
                    'Bucket is missing Object Lock Configuration'
                ), bucket);
            }
            return next(null, bucket, objectMD);
        }),
        (bucket, objectMD, next) => {
            log.trace('parsing retention information');
            parseRetentionXml(request.post, log,
                (err, retentionInfo) => next(err, bucket, retentionInfo, objectMD));
        },
        (bucket, retentionInfo, objectMD, next) => {
            if (objectMD.retentionMode === 'GOVERNANCE' && authInfo.isRequesterAnIAMUser()) {
                log.trace('object in GOVERNANCE mode and is user, checking for attached policies',
                    { method: 'objectPutRetention' });
                const authParams = auth.server.extractParams(request, log, 's3',
                    request.query);
                const ip = policies.requestUtils.getClientIp(request, config);
                const requestContextParams = {
                    constantParams: {
                        headers: request.headers,
                        query: request.query,
                        generalResource: bucketName,
                        specificResource: { key: objectKey },
                        requesterIp: ip,
                        sslEnabled: request.connection.encrypted,
                        apiMethod: 'bypassGovernanceRetention',
                        awsService: 's3',
                        locationConstraint: bucket.getLocationConstraint(),
                        requesterInfo: authInfo,
                        signatureVersion: authParams.params.data.signatureVersion,
                        authType: authParams.params.data.authType,
                        signatureAge: authParams.params.data.signatureAge,
                    },
                };
                return vault.checkPolicies(requestContextParams,
                    authInfo.getArn(), log, (err, authorizationResults) => {
                        if (err) {
                            return next(err);
                        }
                        if (authorizationResults[0].isAllowed !== true) {
                            log.trace('authorization check failed for user',
                                {
                                    'method': 'objectPutRetention',
                                    's3:BypassGovernanceRetention': false,
                                });
                            return next(errors.AccessDenied);
                        }
                        return next(null, bucket, retentionInfo, objectMD);
                    });
            }
            return next(null, bucket, retentionInfo, objectMD);
        },
        (bucket, retentionInfo, objectMD, next) => {
            const bypassHeader = request.headers['x-amz-bypass-governance-retention'] || '';
            const bypassGovernance = bypassHeader.toLowerCase() === 'true';
            const validationError = validateObjectLockUpdate(objectMD, retentionInfo, bypassGovernance);
            if (validationError) {
                return next(validationError, bucket, objectMD);
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
