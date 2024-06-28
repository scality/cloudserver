const { auth, errors, policies } = require('arsenal');
const async = require('async');
const opentelemetry = require('@opentelemetry/api');

const bucketDelete = require('./bucketDelete');
const bucketDeleteCors = require('./bucketDeleteCors');
const bucketDeleteEncryption = require('./bucketDeleteEncryption');
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
const website = require('./website');
const writeContinue = require('../utilities/writeContinue');
const validateQueryAndHeaders = require('../utilities/validateQueryAndHeaders');
const parseCopySource = require('./apiUtils/object/parseCopySource');
const { tagConditionKeyAuth } = require('./apiUtils/authorization/tagConditionKeys');
const checkHttpHeadersSize = require('./apiUtils/object/checkHttpHeadersSize');

const monitoringMap = policies.actionMaps.actionMonitoringMapS3;

auth.setHandler(vault);

/* eslint-disable no-param-reassign */
const api = {
    callApiMethod(apiMethod, request, response, log, callback, oTel) {
        const action = monitoringMap[apiMethod];
        const {
            cloudserverApiSpan,
            activeSpan,
            activeTracerContext,
            tracer,
        } = oTel;
        activeSpan.addEvent(`Initiating Cloudserver API Method ${apiMethod}`);
        // cloudserverApiSpan.updateName(`Performing operations to satisfy ${action} API request. Cloudserver API method: ${apiMethod}()`);
        cloudserverApiSpan.updateName(`Executing ${action} API Method`);
        // TODO: Try out activeTracerContext instead of opentelemetry.context.active()
        const cloudserverApiSpanContext = opentelemetry.trace.setSpan(
            activeTracerContext,
            cloudserverApiSpan,
        );
        return tracer.startActiveSpan(' Setting Up Request Headers and Context for Authentication and Authorization', undefined, cloudserverApiSpanContext, callApiMethod => {
            callApiMethod.setAttributes({
                'code.function': 'callApiMethod()',
                'code.filename': 'lib/api/api.js',
                'code.lineno': 79,
                'rpc.method': action,
            });
            // Attach the apiMethod method to the request, so it can used by monitoring in the server
            // eslint-disable-next-line no-param-reassign
            request.apiMethod = apiMethod;
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
                activeSpan.recordException(validationRes.error);
                callApiMethod.end();
                log.debug('request query / header validation failed', {
                    error: validationRes.error,
                    method: 'api.callApiMethod',
                });
                return process.nextTick(callback, validationRes.error);
            }

            // no need to check auth on website or cors preflight requests
            if (apiMethod === 'websiteGet' || apiMethod === 'websiteHead' ||
            apiMethod === 'corsPreflight') {
                request.actionImplicitDenies = false;
                callApiMethod.end();
                activeSpan.addEvent(`Forwarding Request to ${apiMethod} Handler`);
                // TODO: inject tracing context into the request object
                return this[apiMethod](request, log, callback);
            }

            const { sourceBucket, sourceObject, sourceVersionId, parsingError } =
                parseCopySource(apiMethod, request.headers['x-amz-copy-source']);
            if (parsingError) {
                activeSpan.recordException(parsingError);
                callApiMethod.end();
                log.debug('error parsing copy source', {
                    error: parsingError,
                });
                return process.nextTick(callback, parsingError);
            }

            const { httpHeadersSizeError } = checkHttpHeadersSize(request.headers);
            if (httpHeadersSizeError) {
                activeSpan.recordException(httpHeadersSizeError);
                callApiMethod.end();
                log.debug('http header size limit exceeded', {
                    error: httpHeadersSizeError,
                });
                return process.nextTick(callback, httpHeadersSizeError);
            }
            activeSpan.addEvent('Initializing Authentication and Authorization Request Contexts');
            const requestContexts = prepareRequestContexts(apiMethod, request,
                sourceBucket, sourceObject, sourceVersionId);
            activeSpan.addEvent('Authentication and Authorization Request Contexts Initialized');
            // Extract all the _apiMethods and store them in an array
            const apiMethods = requestContexts ? requestContexts.map(context => context._apiMethod) : [];
            // Attach the names to the current request
            // eslint-disable-next-line no-param-reassign
            request.apiMethods = apiMethods;

            function checkAuthResults(authResults) {
                let returnTagCount = true;
                const isImplicitDeny = {};
                let isOnlyImplicitDeny = true;
                if (apiMethod === 'objectGet') {
                    // first item checks s3:GetObject(Version) action
                    if (!authResults[0].isAllowed && !authResults[0].isImplicit) {
                        activeSpan.recordException(errors.AccessDenied);
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
                            activeSpan.recordException(errors.AccessDenied);
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
                    activeSpan.recordException(errors.AccessDenied);
                    return errors.AccessDenied;
                }
                return { returnTagCount, isImplicitDeny };
            }
            callApiMethod.end();
            activeSpan.addEvent('Starting User Authentication and Authorization');
            return async.waterfall([
                next => tracer.startActiveSpan('Authenticating and Authorizing the Request with Vault', undefined, cloudserverApiSpanContext, iamAuthSpan => {
                    const iamAuthSpanContext = opentelemetry.trace.setSpan(
                        cloudserverApiSpanContext,
                        iamAuthSpan,
                    );
                    activeSpan.addEvent('Starting Vault Authentication and Authorization');
                    iamAuthSpan.setAttributes({
                        'code.function': 'callApiMethod()',
                        'code.filename': 'lib/api/api.js',
                        'code.lineno': 220,
                        'rpc.method': 'AuthV4',
                    });
                    return next(null, iamAuthSpan, iamAuthSpanContext);
                }),
                (iamAuthSpan, iamAuthSpanContext, next) => auth.server.doAuth(
                    request, log, (err, userInfo, authorizationResults, streamingV4Params) => {
                        activeSpan.addEvent('Received Response from Vault');
                        if (err) {
                            activeSpan.recordException(err);
                            iamAuthSpan.end();
                            log.trace('authentication error', { error: err });
                            return next(err);
                        }
                        return next(null, userInfo, authorizationResults, streamingV4Params, iamAuthSpan);
                    }, 's3', requestContexts, { activeSpan, iamAuthSpanContext, tracer }),
                (userInfo, authorizationResults, streamingV4Params, iamAuthSpan, next) => {
                    activeSpan.addEvent('Processing Vault Response');
                    const authNames = { accountName: userInfo.getAccountDisplayName() };
                    if (userInfo.isRequesterAnIAMUser()) {
                        authNames.userName = userInfo.getIAMdisplayName();
                    }
                    log.addDefaultFields(authNames);
                    if (apiMethod === 'objectPut' || apiMethod === 'objectPutPart') {
                        return next(null, userInfo, authorizationResults, streamingV4Params, iamAuthSpan);
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
                        activeSpan.recordException(err);
                        iamAuthSpan.end();
                        log.trace('error receiving request', {
                            error: err,
                        });
                        return next(errors.InternalError);
                    });

                    request.on('end', () => {
                        if (postLength > MAX_POST_LENGTH) {
                            activeSpan.recordException(errors.InvalidRequest);
                            iamAuthSpan.end();
                            log.error('body length is too long for request type',
                                      { postLength });
                            return next(errors.InvalidRequest);
                        }
                        // Convert array of post buffers into one string
                        request.post = Buffer.concat(post, postLength).toString();
                        return next(null, userInfo, authorizationResults, streamingV4Params, iamAuthSpan);
                    });
                    return undefined;
                },
                (userInfo, authorizationResults, streamingV4Params, iamAuthSpan, next) => {
                    activeSpan.addEvent('Vault Response Processed');
                    activeSpan.addEvent('Validating Tag Conditions in Authorization');
                    return next(null, userInfo, authorizationResults, streamingV4Params, iamAuthSpan);
                },
                // Tag condition keys require information from CloudServer for evaluation
                (userInfo, authorizationResults, streamingV4Params, iamAuthSpan, next) => tagConditionKeyAuth(
                    authorizationResults,
                    request,
                    requestContexts,
                    apiMethod,
                    log,
                    (err, authResultsWithTags) => {
                        if (err) {
                            activeSpan.recordException(err);
                            iamAuthSpan.end();
                            log.trace('tag authentication error', { error: err });
                            return next(err);
                        }
                        return next(null, userInfo, authResultsWithTags, streamingV4Params, iamAuthSpan);
                    },
                ),
            ], (err, userInfo, authorizationResults, streamingV4Params, iamAuthSpan) => {
                if (iamAuthSpan) {
                    iamAuthSpan.end();
                }
                activeSpan.addEvent('Vault Authentication and Authorization Completed');
                if (err) {
                    activeSpan.recordException(err);
                    return callback(err);
                }
                if (authorizationResults) {
                    activeSpan.addEvent('Verifying Authorization Results');
                    const checkedResults = checkAuthResults(authorizationResults);
                    if (checkedResults instanceof Error) {
                        activeSpan.recordException(checkedResults);
                        return callback(checkedResults);
                    }
                    activeSpan.addEvent('Authorization Results Verified');
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
                activeSpan.addEvent(`Forwarding Request to ${apiMethod} Handler`);
                if (apiMethod === 'objectPut' || apiMethod === 'objectPutPart') {
                    request._response = response;
                    return this[apiMethod](userInfo, request, streamingV4Params,
                        log, callback, authorizationResults, oTel);
                }
                if (apiMethod === 'objectCopy' || apiMethod === 'objectPutCopyPart') {
                    return this[apiMethod](userInfo, request, sourceBucket,
                        sourceObject, sourceVersionId, log, callback, oTel);
                }
                if (apiMethod === 'objectGet') {
                    return this[apiMethod](userInfo, request, returnTagCount, log, callback, oTel);
                }
                return this[apiMethod](userInfo, request, log, callback, oTel);
            });
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
    bucketGetTagging,
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
    bucketDeleteTagging,
    bucketPutObjectLock,
    bucketPutNotification,
    bucketGetNotification,
    bucketPutEncryption,
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
    websiteGet: website,
    websiteHead: website,
};

module.exports = api;
