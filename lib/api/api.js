const { auth, errors, policies } = require('arsenal');
const async = require('async');

const bucketDelete = require('./bucketDelete');
const bucketDeleteCors = require('./bucketDeleteCors');
const bucketDeleteWebsite = require('./bucketDeleteWebsite');
const bucketDeleteLifecycle = require('./bucketDeleteLifecycle');
const bucketDeletePolicy = require('./bucketDeletePolicy');
const bucketGet = require('./bucketGet');
const bucketGetACL = require('./bucketGetACL');
const bucketGetCors = require('./bucketGetCors');
const bucketGetVersioning = require('./bucketGetVersioning');
const bucketGetWebsite = require('./bucketGetWebsite');
const bucketGetLocation = require('./bucketGetLocation');
const bucketGetLifecycle = require('./bucketGetLifecycle');
const bucketGetNotification = require('./bucketGetNotification');
const bucketGetObjectLock = require('./bucketGetObjectLock');
const bucketGetPolicy = require('./bucketGetPolicy');
const bucketGetEncryption = require('./bucketGetEncryption');
const bucketHead = require('./bucketHead');
const { bucketPut } = require('./bucketPut');
const bucketPutACL = require('./bucketPutACL');
const bucketPutCors = require('./bucketPutCors');
const bucketPutVersioning = require('./bucketPutVersioning');
const bucketPutWebsite = require('./bucketPutWebsite');
const bucketPutReplication = require('./bucketPutReplication');
const bucketPutLifecycle = require('./bucketPutLifecycle');
const bucketPutNotification = require('./bucketPutNotification');
const bucketPutPolicy = require('./bucketPutPolicy');
const bucketPutObjectLock = require('./bucketPutObjectLock');
const bucketGetReplication = require('./bucketGetReplication');
const bucketDeleteReplication = require('./bucketDeleteReplication');
const corsPreflight = require('./corsPreflight');
const completeMultipartUpload = require('./completeMultipartUpload');
const initiateMultipartUpload = require('./initiateMultipartUpload');
const listMultipartUploads = require('./listMultipartUploads');
const listParts = require('./listParts');
const { multiObjectDelete } = require('./multiObjectDelete');
const multipartDelete = require('./multipartDelete');
const objectCopy = require('./objectCopy');
const objectDelete = require('./objectDelete');
const objectDeleteTagging = require('./objectDeleteTagging');
const objectGet = require('./objectGet');
const objectGetACL = require('./objectGetACL');
const objectGetLegalHold = require('./objectGetLegalHold');
const objectGetRetention = require('./objectGetRetention');
const objectGetTagging = require('./objectGetTagging');
const objectHead = require('./objectHead');
const objectPut = require('./objectPut');
const objectPutACL = require('./objectPutACL');
const objectPutLegalHold = require('./objectPutLegalHold');
const objectPutTagging = require('./objectPutTagging');
const objectPutPart = require('./objectPutPart');
const objectPutCopyPart = require('./objectPutCopyPart');
const objectPutRetention = require('./objectPutRetention');
const prepareRequestContexts
    = require('./apiUtils/authorization/prepareRequestContexts');
const serviceGet = require('./serviceGet');
const vault = require('../auth/vault');
const websiteGet = require('./websiteGet');
const websiteHead = require('./websiteHead');
const writeContinue = require('../utilities/writeContinue');
const validateQueryAndHeaders = require('../utilities/validateQueryAndHeaders');
const parseCopySource = require('./apiUtils/object/parseCopySource');
const { tagConditionKeyAuth } = require('./apiUtils/authorization/tagConditionKeys');

const monitoringMap = policies.actionMaps.actionMonitoringMapS3;

auth.setHandler(vault);

/* eslint-disable no-param-reassign */
const api = {
    callApiMethod(apiMethod, request, response, log, callback) {
        const actionLog = monitoringMap[apiMethod];
        if (!actionLog &&
            apiMethod !== 'websiteGet' &&
            apiMethod !== 'websiteHead' &&
            apiMethod !== 'corsPreflight') {
            log.error('callApiMethod(): No actionLog for this api method', {
                apiMethod,
            });
        }
        log.addDefaultFields({
            service: 's3',
            action: actionLog,
            bucketName: request.bucketName,
        });
        if (request.objectKey) {
            log.addDefaultFields({
                objectKey: request.objectKey,
            });
        }
        let returnTagCount = true;

        const validationRes = validateQueryAndHeaders(request, log);
        if (validationRes.error) {
            log.debug('request query / header validation failed', {
                error: validationRes.error,
                method: 'api.callApiMethod',
            });
            return process.nextTick(callback, validationRes.error);
        }

        // no need to check auth on website or cors preflight requests
        if (apiMethod === 'websiteGet' || apiMethod === 'websiteHead' ||
        apiMethod === 'corsPreflight') {
            return this[apiMethod](request, log, callback);
        }

        const { sourceBucket, sourceObject, sourceVersionId, parsingError } =
            parseCopySource(apiMethod, request.headers['x-amz-copy-source']);
        if (parsingError) {
            log.debug('error parsing copy source', {
                error: parsingError,
            });
            return process.nextTick(callback, parsingError);
        }

        const requestContexts = prepareRequestContexts(apiMethod, request,
            sourceBucket, sourceObject, sourceVersionId);

        function checkAuthResults(authResults) {
            let returnTagCount = true;
            if (apiMethod === 'objectGet') {
                // first item checks s3:GetObject(Version) action
                if (!authResults[0].isAllowed) {
                    log.trace('get object authorization denial from Vault');
                    return errors.AccessDenied;
                }
                // second item checks s3:GetObject(Version)Tagging action
                if (!authResults[1].isAllowed) {
                    log.trace('get tagging authorization denial ' +
                    'from Vault');
                    returnTagCount = false;
                }
            } else {
                for (let i = 0; i < authResults.length; i++) {
                    if (!authResults[i].isAllowed) {
                        log.trace('authorization denial from Vault');
                        return errors.AccessDenied;
                    }
                }
            }
            return returnTagCount;
        }

        return async.waterfall([
            next => auth.server.doAuth(request, log, (err, userInfo, authorizationResults, streamingV4Params) =>
                next(err, userInfo, authorizationResults, streamingV4Params), 's3', requestContexts),
            (userInfo, authorizationResults, streamingV4Params, next) => {
                if (authorizationResults) {
                    const checkedResults = checkAuthResults(authorizationResults);
                    if (checkedResults instanceof Error) {
                        return next(checkedResults);
                    }
                    returnTagCount = checkedResults;
                }
                return tagConditionKeyAuth(authorizationResults, request, requestContexts, apiMethod, log,
                (err, tagAuthResults, updatedContexts) =>
                    next(err, tagAuthResults, authorizationResults, userInfo, streamingV4Params, updatedContexts));
            },
        ], (err, tagAuthResults, authorizationResults, userInfo, streamingV4Params, updatedContexts) => {
            if (err) {
                log.trace('authentication error', { error: err });
                return callback(err);
            }
            const authNames = { accountName: userInfo.getAccountDisplayName() };
            if (userInfo.isRequesterAnIAMUser()) {
                authNames.userName = userInfo.getIAMdisplayName();
            }
            log.addDefaultFields(authNames);
            if (tagAuthResults) {
                const checkedResults = checkAuthResults(tagAuthResults);
                if (checkedResults instanceof Error) {
                    return callback(checkedResults);
                }
                returnTagCount = checkedResults;
            }
            if (apiMethod === 'objectPut' || apiMethod === 'objectPutPart') {
                request._response = response;
                return this[apiMethod](userInfo, request, streamingV4Params,
                    log, callback, authorizationResults);
            }
            // issue 100 Continue to the client
            writeContinue(request, response);
            const MAX_POST_LENGTH = request.method.toUpperCase() === 'POST' ?
                1024 * 1024 : 1024 * 1024 / 2; // 1 MB or 512 KB
            const post = [];
            let postLength = 0;
            request.on('data', chunk => {
                postLength += chunk.length;
                // Sanity check on post length
                if (postLength <= MAX_POST_LENGTH) {
                    post.push(chunk);
                }
                return undefined;
            });

            request.on('error', err => {
                log.trace('error receiving request', {
                    error: err,
                });
                return callback(errors.InternalError);
            });

            request.on('end', () => {
                if (postLength > MAX_POST_LENGTH) {
                    log.error('body length is too long for request type',
                        { postLength });
                    return callback(errors.InvalidRequest);
                }
                // Convert array of post buffers into one string
                request.post = Buffer.concat(post, postLength).toString();

                // IAM policy -Tag condition keys require information from CloudServer for evaluation
                return tagConditionKeyAuth(authorizationResults, request, (updatedContexts || requestContexts),
                apiMethod, log, (err, tagAuthResults) => {
                    if (err) {
                        log.trace('tag authentication error', { error: err });
                        return callback(err);
                    }
                    if (tagAuthResults) {
                        const checkedResults = checkAuthResults(tagAuthResults);
                        if (checkedResults instanceof Error) {
                            return callback(checkedResults);
                        }
                        returnTagCount = checkedResults;
                    }
                    if (apiMethod === 'objectCopy' ||
                    apiMethod === 'objectPutCopyPart') {
                        return this[apiMethod](userInfo, request, sourceBucket,
                            sourceObject, sourceVersionId, log, callback);
                    }
                    if (apiMethod === 'objectGet') {
                        return this[apiMethod](userInfo, request,
                        returnTagCount, log, callback);
                    }
                    return this[apiMethod](userInfo, request, log, callback);
                });
            });
            return undefined;
        });
    },
    bucketDelete,
    bucketDeleteCors,
    bucketDeleteWebsite,
    bucketGet,
    bucketGetACL,
    bucketGetCors,
    bucketGetObjectLock,
    bucketGetVersioning,
    bucketGetWebsite,
    bucketGetLocation,
    bucketGetEncryption,
    bucketHead,
    bucketPut,
    bucketPutACL,
    bucketPutCors,
    bucketPutVersioning,
    bucketPutWebsite,
    bucketPutReplication,
    bucketGetReplication,
    bucketDeleteReplication,
    bucketPutLifecycle,
    bucketGetLifecycle,
    bucketDeleteLifecycle,
    bucketPutPolicy,
    bucketGetPolicy,
    bucketDeletePolicy,
    bucketPutObjectLock,
    bucketPutNotification,
    bucketGetNotification,
    corsPreflight,
    completeMultipartUpload,
    initiateMultipartUpload,
    listMultipartUploads,
    listParts,
    multiObjectDelete,
    multipartDelete,
    objectDelete,
    objectDeleteTagging,
    objectGet,
    objectGetACL,
    objectGetLegalHold,
    objectGetRetention,
    objectGetTagging,
    objectCopy,
    objectHead,
    objectPut,
    objectPutACL,
    objectPutLegalHold,
    objectPutTagging,
    objectPutPart,
    objectPutCopyPart,
    objectPutRetention,
    serviceGet,
    websiteGet,
    websiteHead,
};

module.exports = api;
