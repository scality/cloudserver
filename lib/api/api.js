const { auth, errors, policies } = require('arsenal');
const async = require('async');

const bucketDelete = require('./bucketDelete');
const bucketDeleteCors = require('./bucketDeleteCors');
const bucketDeleteEncryption = require('./bucketDeleteEncryption');
const bucketDeleteWebsite = require('./bucketDeleteWebsite');
const bucketDeleteLifecycle = require('./bucketDeleteLifecycle');
const bucketDeletePolicy = require('./bucketDeletePolicy');
const bucketDeleteQuota = require('./bucketDeleteQuota');
const { bucketGet } = require('./bucketGet');
const bucketGetACL = require('./bucketGetACL');
const bucketGetCors = require('./bucketGetCors');
const bucketGetVersioning = require('./bucketGetVersioning');
const bucketGetWebsite = require('./bucketGetWebsite');
const bucketGetLocation = require('./bucketGetLocation');
const bucketGetLifecycle = require('./bucketGetLifecycle');
const bucketGetNotification = require('./bucketGetNotification');
const bucketGetObjectLock = require('./bucketGetObjectLock');
const bucketGetPolicy = require('./bucketGetPolicy');
const bucketGetQuota = require('./bucketGetQuota');
const bucketGetEncryption = require('./bucketGetEncryption');
const bucketHead = require('./bucketHead');
const { bucketPut } = require('./bucketPut');
const bucketPutACL = require('./bucketPutACL');
const bucketPutCors = require('./bucketPutCors');
const bucketPutVersioning = require('./bucketPutVersioning');
const bucketPutTagging = require('./bucketPutTagging');
const bucketDeleteTagging = require('./bucketDeleteTagging');
const bucketGetTagging = require('./bucketGetTagging');
const bucketPutWebsite = require('./bucketPutWebsite');
const bucketPutReplication = require('./bucketPutReplication');
const bucketPutLifecycle = require('./bucketPutLifecycle');
const bucketPutNotification = require('./bucketPutNotification');
const bucketPutEncryption = require('./bucketPutEncryption');
const bucketPutPolicy = require('./bucketPutPolicy');
const bucketPutObjectLock = require('./bucketPutObjectLock');
const bucketUpdateQuota = require('./bucketUpdateQuota');
const bucketGetReplication = require('./bucketGetReplication');
const bucketDeleteReplication = require('./bucketDeleteReplication');
const corsPreflight = require('./corsPreflight');
const completeMultipartUpload = require('./completeMultipartUpload');
const initiateMultipartUpload = require('./initiateMultipartUpload');
const listMultipartUploads = require('./listMultipartUploads');
const listParts = require('./listParts');
const metadataSearch = require('./metadataSearch');
const { multiObjectDelete } = require('./multiObjectDelete');
const multipartDelete = require('./multipartDelete');
const objectCopy = require('./objectCopy');
const { objectDelete } = require('./objectDelete');
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
const objectRestore = require('./objectRestore');
const prepareRequestContexts
    = require('./apiUtils/authorization/prepareRequestContexts');
const serviceGet = require('./serviceGet');
const vault = require('../auth/vault');
const website = require('./website');
const writeContinue = require('../utilities/writeContinue');
const validateQueryAndHeaders = require('../utilities/validateQueryAndHeaders');
const parseCopySource = require('./apiUtils/object/parseCopySource');
const { tagConditionKeyAuth } = require('./apiUtils/authorization/tagConditionKeys');
const { isRequesterASessionUser } = require('./apiUtils/authorization/permissionChecks');
const checkHttpHeadersSize = require('./apiUtils/object/checkHttpHeadersSize');

// Import instrumentation utilities
const { instrumentApiMethod } = require('../instrumentation/simple');
const { trace } = require('@opentelemetry/api');
const { monitorLatency } = require('../utilities/monitoringHandler');

const monitoringMap = policies.actionMaps.actionMonitoringMapS3;

auth.setHandler(vault);

/* eslint-disable no-param-reassign */
const api = {
    callApiMethod(apiMethod, request, response, log, callback) {
        // Create a detailed span for the overall API method call
        const tracer = trace.getTracer('cloudserver-api-request', '1.0.0');
        const span = tracer.startSpan('api.request_processing', {
            attributes: {
                'cloudserver.component': 'api',
                'cloudserver.api_method': apiMethod,
                'http.method': request.method,
                'cloudserver.operation_type': getApiOperationType(apiMethod)
            }
        });

        // Attach the apiMethod method to the request, so it can used by monitoring in the server
        request.apiMethod = apiMethod;
        // Array of end of API callbacks, used to perform some logic
        // at the end of an API.
        request.finalizerHooks = [];

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

        // Wrap the original callback to end the span
        const wrappedCallback = function (err, ...results) {
            if (err) {
                span.recordException(err);
                span.setStatus({ code: 2 }); // ERROR
                if (err.code) {
                    span.setAttributes({ 'cloudserver.error_code': err.code });
                }
            } else {
                span.setStatus({ code: 1 }); // OK
            }
            span.end();
            return callback(err, ...results);
        };

        const validationRes = validateQueryAndHeaders(request, log);
        if (validationRes.error) {
            log.debug('request query / header validation failed', {
                error: validationRes.error,
                method: 'api.callApiMethod',
            });
            return process.nextTick(wrappedCallback, validationRes.error);
        }

        // no need to check auth on website or cors preflight requests
        if (apiMethod === 'websiteGet' || apiMethod === 'websiteHead' ||
        apiMethod === 'corsPreflight') {
            request.actionImplicitDenies = false;
            return this[apiMethod](request, log, wrappedCallback);
        }

        const { sourceBucket, sourceObject, sourceVersionId, parsingError } =
            parseCopySource(apiMethod, request.headers['x-amz-copy-source']);
        if (parsingError) {
            log.debug('error parsing copy source', {
                error: parsingError,
            });
            return process.nextTick(wrappedCallback, parsingError);
        }

        const { httpHeadersSizeError } = checkHttpHeadersSize(request.headers);
        if (httpHeadersSizeError) {
            log.debug('http header size limit exceeded', {
                error: httpHeadersSizeError,
            });
            return process.nextTick(wrappedCallback, httpHeadersSizeError);
        }

        const requestContexts = prepareRequestContexts(apiMethod, request,
            sourceBucket, sourceObject, sourceVersionId);

        // Extract all the _apiMethods and store them in an array
        const apiMethods = requestContexts ? requestContexts.map(context => context._apiMethod) : [];
        // Attach the names to the current request
        request.apiMethods = apiMethods;

        let authzResultsLatency;

        function checkAuthResults(authResults) {
            let returnTagCount = true;
            const isImplicitDeny = {};
            let isOnlyImplicitDeny = true;
            if (apiMethod === 'objectGet') {
                // first item checks s3:GetObject(Version) action
                if (!authResults[0].isAllowed && !authResults[0].isImplicit) {
                    log.trace('get object authorization denial from Vault');
                    return errors.AccessDenied;
                }
                // TODO add support for returnTagCount in the bucket policy
                // checks
                isImplicitDeny[authResults[0].action] = authResults[0].isImplicit;
                // second item checks s3:GetObject(Version)Tagging action
                if (!authResults[1].isAllowed) {
                    log.trace('get tagging authorization denial ' +
                    'from Vault');
                    returnTagCount = false;
                }
            } else {
                for (let i = 0; i < authResults.length; i++) {
                    isImplicitDeny[authResults[i].action] = true;
                    if (!authResults[i].isAllowed && !authResults[i].isImplicit) {
                        // Any explicit deny rejects the current API call
                        log.trace('authorization denial from Vault');
                        return errors.AccessDenied;
                    }
                    if (authResults[i].isAllowed) {
                        // If the action is allowed, the result is not implicit
                        // Deny.
                        isImplicitDeny[authResults[i].action] = false;
                        isOnlyImplicitDeny = false;
                    }
                }
            }
            // These two APIs cannot use ACLs or Bucket Policies, hence, any
            // implicit deny from vault must be treated as an explicit deny.
            if ((apiMethod === 'bucketPut' || apiMethod === 'serviceGet') && isOnlyImplicitDeny) {
                log.trace('authorization denial for serviceGet or bucketPut');
                return errors.AccessDenied;
            }

            return { returnTagCount, isImplicitDeny };
        }
        // use precise metric with performance.now()
        const startTime = performance.now();
        return async.waterfall([
            next => auth.server.doAuth(
                request, log, (err, userInfo, authorizationResults, streamingV4Params, infos) => {
                    monitorLatency('1-doAuth', performance.now() - startTime);
                    if (err) {
                        // VaultClient returns standard errors, but the route requires
                        // Arsenal errors
                        const arsenalError = err.metadata ? err : errors[err.code] || errors.InternalError;
                        log.trace('authentication error', { error: err });
                        return next(arsenalError);
                    }
                    return next(null, userInfo, authorizationResults, streamingV4Params, infos);
                }, 's3', requestContexts),
            (userInfo, authorizationResults, streamingV4Params, infos, next) => {
                authzResultsLatency = performance.now();
                const authNames = { accountName: userInfo.getAccountDisplayName() };
                if (userInfo.isRequesterAnIAMUser()) {
                    authNames.userName = userInfo.getIAMdisplayName();
                }
                if (isRequesterASessionUser(userInfo)) {
                    authNames.sessionName = userInfo.getShortid().split(':')[1];
                }
                log.addDefaultFields(authNames);
                if (apiMethod === 'objectPut' || apiMethod === 'objectPutPart') {
                    return next(null, userInfo, authorizationResults, streamingV4Params, infos);
                }
                // issue 100 Continue to the client
                writeContinue(request, response);
                const MAX_POST_LENGTH = request.method === 'POST' ?
                      1024 * 1024 : 1024 * 1024 / 2; // 1 MB or 512 KB
                const post = [];
                let postLength = 0;
                request.on('data', chunk => {
                    postLength += chunk.length;
                    // Sanity check on post length
                    if (postLength <= MAX_POST_LENGTH) {
                        post.push(chunk);
                    }
                });

                request.on('error', err => {
                    log.trace('error receiving request', {
                        error: err,
                    });
                    return next(errors.InternalError);
                });

                request.on('end', () => {
                    if (postLength > MAX_POST_LENGTH) {
                        log.error('body length is too long for request type',
                                  { postLength });
                        return next(errors.InvalidRequest);
                    }
                    // Convert array of post buffers into one string
                    request.post = Buffer.concat(post, postLength).toString();
                    return next(null, userInfo, authorizationResults, streamingV4Params, infos);
                });
                return undefined;
            },
            // Tag condition keys require information from CloudServer for evaluation
            (userInfo, authorizationResults, streamingV4Params, infos, next) => tagConditionKeyAuth(
                authorizationResults,
                request,
                requestContexts,
                apiMethod,
                log,
                (err, authResultsWithTags) => {
                    monitorLatency('2-tagConditionKeyAuth', performance.now() - authzResultsLatency);
                    if (err) {
                        log.trace('tag authentication error', { error: err });
                        return next(err);
                    }
                    return next(null, userInfo, authResultsWithTags, streamingV4Params, infos);
                },
            ),
        ],
        (err, userInfo, authorizationResults, streamingV4Params, infos) => {
            if (err) {
                return wrappedCallback(err);
            }
            request.accountQuotas = infos?.accountQuota;
            if (authorizationResults) {
                const checkedResults = checkAuthResults(authorizationResults);
                if (checkedResults instanceof Error) {
                    return wrappedCallback(checkedResults);
                }
                returnTagCount = checkedResults.returnTagCount;
                request.actionImplicitDenies = checkedResults.isImplicitDeny;
            } else {
                // create an object of keys apiMethods with all values to false:
                // for backward compatibility, all apiMethods are allowed by default
                // thus it is explicitly allowed, so implicit deny is false
                request.actionImplicitDenies = apiMethods.reduce((acc, curr) => {
                    acc[curr] = false;
                    return acc;
                }, {});
            }
            const methodCallback = (err, ...results) => async.forEachLimit(request.finalizerHooks, 5,
                    (hook, done) => hook(err, done),
                    () => wrappedCallback(err, ...results));

            if (apiMethod === 'objectPut' || apiMethod === 'objectPutPart') {
                request._response = response;
                return this[apiMethod](userInfo, request, streamingV4Params,
                    log, methodCallback, authorizationResults);
            }
            if (apiMethod === 'objectCopy' || apiMethod === 'objectPutCopyPart') {
                return this[apiMethod](userInfo, request, sourceBucket,
                    sourceObject, sourceVersionId, log, methodCallback);
            }
            if (apiMethod === 'objectGet') {
                return this[apiMethod](userInfo, request, returnTagCount, log, callback);
            }
            return this[apiMethod](userInfo, request, log, methodCallback);
        });
    },
    bucketDelete,
    bucketDeleteCors,
    bucketDeleteEncryption,
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
    bucketPutTagging,
    bucketDeleteTagging,
    bucketGetTagging,
    bucketPutWebsite,
    bucketPutReplication,
    bucketGetReplication,
    bucketDeleteReplication,
    bucketDeleteQuota,
    bucketPutLifecycle,
    bucketUpdateQuota,
    bucketGetLifecycle,
    bucketDeleteLifecycle,
    bucketPutPolicy,
    bucketGetPolicy,
    bucketGetQuota,
    bucketDeletePolicy,
    bucketPutObjectLock,
    bucketPutNotification,
    bucketGetNotification,
    bucketPutEncryption,
    corsPreflight,
    completeMultipartUpload,
    initiateMultipartUpload,
    listMultipartUploads,
    listParts,
    metadataSearch,
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
    objectRestore,
    serviceGet,
    websiteGet: website,
    websiteHead: website,
};

// Helper function to get operation type for API methods
function getApiOperationType(apiMethod) {
    if (apiMethod.includes('object')) {
        if (apiMethod.includes('Put')) {return 'object_put';}
        if (apiMethod.includes('Get')) {return 'object_get';}
        if (apiMethod.includes('Head')) {return 'object_head';}
        if (apiMethod.includes('Delete')) {return 'object_delete';}
        if (apiMethod.includes('Copy')) {return 'object_copy';}
        return 'object_op';
    }
    if (apiMethod.includes('bucket')) {
        if (apiMethod.includes('Put')) {return 'bucket_put';}
        if (apiMethod.includes('Get')) {return 'bucket_get';}
        if (apiMethod.includes('Head')) {return 'bucket_head';}
        if (apiMethod.includes('Delete')) {return 'bucket_delete';}
        return 'bucket_op';
    }
    if (apiMethod.includes('multipart') || apiMethod.includes('Multipart')) {
        return 'multipart';
    }
    if (apiMethod.includes('service') || apiMethod.includes('Service')) {
        return 'service';
    }
    return 'other';
}

// Add all the API methods to the api object with instrumentation
api.bucketDelete = instrumentApiMethod(bucketDelete, 'bucketDelete');
api.bucketDeleteCors = instrumentApiMethod(bucketDeleteCors, 'bucketDeleteCors');
api.bucketDeleteEncryption = instrumentApiMethod(bucketDeleteEncryption, 'bucketDeleteEncryption');
api.bucketDeleteWebsite = instrumentApiMethod(bucketDeleteWebsite, 'bucketDeleteWebsite');
api.bucketDeleteLifecycle = instrumentApiMethod(bucketDeleteLifecycle, 'bucketDeleteLifecycle');
api.bucketDeletePolicy = instrumentApiMethod(bucketDeletePolicy, 'bucketDeletePolicy');
api.bucketDeleteQuota = instrumentApiMethod(bucketDeleteQuota, 'bucketDeleteQuota');
api.bucketGet = instrumentApiMethod(bucketGet, 'bucketGet');
api.bucketGetACL = instrumentApiMethod(bucketGetACL, 'bucketGetACL');
api.bucketGetCors = instrumentApiMethod(bucketGetCors, 'bucketGetCors');
api.bucketGetVersioning = instrumentApiMethod(bucketGetVersioning, 'bucketGetVersioning');
api.bucketGetWebsite = instrumentApiMethod(bucketGetWebsite, 'bucketGetWebsite');
api.bucketGetLocation = instrumentApiMethod(bucketGetLocation, 'bucketGetLocation');
api.bucketGetLifecycle = instrumentApiMethod(bucketGetLifecycle, 'bucketGetLifecycle');
api.bucketGetNotification = instrumentApiMethod(bucketGetNotification, 'bucketGetNotification');
api.bucketGetObjectLock = instrumentApiMethod(bucketGetObjectLock, 'bucketGetObjectLock');
api.bucketGetPolicy = instrumentApiMethod(bucketGetPolicy, 'bucketGetPolicy');
api.bucketGetQuota = instrumentApiMethod(bucketGetQuota, 'bucketGetQuota');
api.bucketGetEncryption = instrumentApiMethod(bucketGetEncryption, 'bucketGetEncryption');
api.bucketHead = instrumentApiMethod(bucketHead, 'bucketHead');
api.bucketPut = instrumentApiMethod(bucketPut, 'bucketPut');
api.bucketPutACL = instrumentApiMethod(bucketPutACL, 'bucketPutACL');
api.bucketPutCors = instrumentApiMethod(bucketPutCors, 'bucketPutCors');
api.bucketPutVersioning = instrumentApiMethod(bucketPutVersioning, 'bucketPutVersioning');
api.bucketPutTagging = instrumentApiMethod(bucketPutTagging, 'bucketPutTagging');
api.bucketDeleteTagging = instrumentApiMethod(bucketDeleteTagging, 'bucketDeleteTagging');
api.bucketGetTagging = instrumentApiMethod(bucketGetTagging, 'bucketGetTagging');
api.bucketPutWebsite = instrumentApiMethod(bucketPutWebsite, 'bucketPutWebsite');
api.bucketPutReplication = instrumentApiMethod(bucketPutReplication, 'bucketPutReplication');
api.bucketPutLifecycle = instrumentApiMethod(bucketPutLifecycle, 'bucketPutLifecycle');
api.bucketPutNotification = instrumentApiMethod(bucketPutNotification, 'bucketPutNotification');
api.bucketPutEncryption = instrumentApiMethod(bucketPutEncryption, 'bucketPutEncryption');
api.bucketPutPolicy = instrumentApiMethod(bucketPutPolicy, 'bucketPutPolicy');
api.bucketPutObjectLock = instrumentApiMethod(bucketPutObjectLock, 'bucketPutObjectLock');
api.bucketUpdateQuota = instrumentApiMethod(bucketUpdateQuota, 'bucketUpdateQuota');
api.bucketGetReplication = instrumentApiMethod(bucketGetReplication, 'bucketGetReplication');
api.bucketDeleteReplication = instrumentApiMethod(bucketDeleteReplication, 'bucketDeleteReplication');
api.corsPreflight = instrumentApiMethod(corsPreflight, 'corsPreflight');
api.completeMultipartUpload = instrumentApiMethod(completeMultipartUpload, 'completeMultipartUpload');
api.initiateMultipartUpload = instrumentApiMethod(initiateMultipartUpload, 'initiateMultipartUpload');
api.listMultipartUploads = instrumentApiMethod(listMultipartUploads, 'listMultipartUploads');
api.listParts = instrumentApiMethod(listParts, 'listParts');
api.metadataSearch = instrumentApiMethod(metadataSearch, 'metadataSearch');
api.multiObjectDelete = instrumentApiMethod(multiObjectDelete, 'multiObjectDelete');
api.multipartDelete = instrumentApiMethod(multipartDelete, 'multipartDelete');
api.objectCopy = instrumentApiMethod(objectCopy, 'objectCopy');
api.objectDelete = instrumentApiMethod(objectDelete, 'objectDelete');
api.objectDeleteTagging = instrumentApiMethod(objectDeleteTagging, 'objectDeleteTagging');
api.objectGet = instrumentApiMethod(objectGet, 'objectGet');
api.objectGetACL = instrumentApiMethod(objectGetACL, 'objectGetACL');
api.objectGetLegalHold = instrumentApiMethod(objectGetLegalHold, 'objectGetLegalHold');
api.objectGetRetention = instrumentApiMethod(objectGetRetention, 'objectGetRetention');
api.objectGetTagging = instrumentApiMethod(objectGetTagging, 'objectGetTagging');
api.objectHead = instrumentApiMethod(objectHead, 'objectHead');
api.objectPut = instrumentApiMethod(objectPut, 'objectPut');
api.objectPutACL = instrumentApiMethod(objectPutACL, 'objectPutACL');
api.objectPutLegalHold = instrumentApiMethod(objectPutLegalHold, 'objectPutLegalHold');
api.objectPutTagging = instrumentApiMethod(objectPutTagging, 'objectPutTagging');
api.objectPutPart = instrumentApiMethod(objectPutPart, 'objectPutPart');
api.objectPutCopyPart = instrumentApiMethod(objectPutCopyPart, 'objectPutCopyPart');
api.objectPutRetention = instrumentApiMethod(objectPutRetention, 'objectPutRetention');
api.objectRestore = instrumentApiMethod(objectRestore, 'objectRestore');
api.serviceGet = instrumentApiMethod(serviceGet, 'serviceGet');
api.websiteGet = instrumentApiMethod(website.website, 'websiteGet');
api.websiteHead = instrumentApiMethod(website.website, 'websiteHead');

module.exports = api;
