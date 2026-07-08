const { auth, errors, policies } = require('arsenal');
const async = require('async');

const bucketDelete = require('./bucketDelete');
const bucketDeleteCors = require('./bucketDeleteCors');
const bucketDeleteEncryption = require('./bucketDeleteEncryption');
const bucketDeleteWebsite = require('./bucketDeleteWebsite');
const bucketDeleteLifecycle = require('./bucketDeleteLifecycle');
const bucketDeletePolicy = require('./bucketDeletePolicy');
const bucketDeleteQuota = require('./bucketDeleteQuota');
const bucketDeleteRateLimit = require('./bucketDeleteRateLimit.js');
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
const bucketGetRateLimit = require('./bucketGetRateLimit.js');
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
const bucketPutRateLimit = require('./bucketPutRateLimit.js');
const bucketGetReplication = require('./bucketGetReplication');
const bucketDeleteReplication = require('./bucketDeleteReplication');
const bucketGetLogging = require('./bucketGetLogging');
const bucketPutLogging = require('./bucketPutLogging');
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
const objectGetAttributes = require('./objectGetAttributes.js');
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
const constants = require('../../constants');
const { config } = require('../Config.js');
const metadata = require('../metadata/wrapper');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { validateMethodChecksumNoChunking } = require('./apiUtils/integrity/validateChecksums');
const {
    requestNeedsRateCheck,
    getCachedRateLimitConfig,
    buildRateChecksFromConfig,
    checkRateLimitsForRequest,
} = require('./apiUtils/rateLimit/helpers');
const rateLimitCache = require('./apiUtils/rateLimit/cache');

const monitoringMap = policies.actionMaps.actionMonitoringMapS3;

auth.setHandler(vault);

// Detect CORS headers the handler already attached to its callback args.
// Callback arity varies per route ((err, corsHeaders), (err, xml,
// corsHeaders), (err, dataGetInfo, resMetaHeaders, range), ...) so we
// scan the args instead of relying on a fixed position.
function hasCorsHeaders(callbackArgs) {
    for (const arg of callbackArgs) {
        if (!arg || typeof arg !== 'object' || Array.isArray(arg)
            || Buffer.isBuffer(arg)) {
            continue;
        }
        if ('access-control-allow-origin' in arg) {
            return true;
        }
    }
    return false;
}

// Wrap the API callback so that, on error, we look up the bucket's CORS
// configuration and set the matching Access-Control-* headers directly on
// the HTTP response. This is needed because auth / pre-handler errors
// return before the API handler retrieves the bucket (where the existing
// collectCorsHeaders call lives), so the route-level error response path
// otherwise sends no CORS headers - breaking cross-origin browser clients.
// We only pay the extra metadata lookup when an Origin header is present,
// matching AWS behavior and the existing contract of collectCorsHeaders.
//
// On the error path of a single request, this wrapper produces at most one
// extra metadata.getBucket call - the fallback below. It only fires when:
//   1. The handler ran and called metadata.getBucket itself (count = 1), AND
//   2. It returned without surfacing CORS headers in its callback args
//      (e.g. handler dropped the loaded bucket between waterfall steps,
//      or the request did not match any CORS rule and the result was {}).
// Combined with the handler's own call, total getBucket calls per failing
// request stay capped at 2. Any future optimization must preserve or
// improve that ceiling; the unit test in tests/unit/api/corsErrorHeaders.js
// asserts it.
function wrapCallbackWithErrorCorsHeaders(callback, request, response, log) {
    function applyHeadersFromBucket(bucket) {
        if (!bucket || response.headersSent) {
            return;
        }
        const headers = collectCorsHeaders(
            request.headers.origin, request.method, bucket);
        Object.keys(headers).forEach(key => {
            try {
                response.setHeader(key, headers[key]);
            } catch (e) {
                log.debug('could not set CORS header on error', {
                    header: key, error: e.message,
                });
            }
        });
    }

    return (err, ...callbackArgs) => {
        if (!err || !request.headers || !request.headers.origin
            || !request.bucketName) {
            return callback(err, ...callbackArgs);
        }
        // Fast path: most post-auth failures come back with corsHeaders
        // already computed by the handler. The route will forward them
        // to responseXMLBody/responseNoBody, so no extra lookup is needed.
        if (hasCorsHeaders(callbackArgs)) {
            return callback(err, ...callbackArgs);
        }
        // Fallback: either the bucket was never loaded (pre-handler
        // failure like auth.server.doAuth denial, header-size check,
        // copy-source parse error) or the handler returned without
        // surfacing CORS headers. Re-fetch the bucket using the
        // request's bucketName (the destination on copy operations) so
        // we evaluate CORS against the right bucket.
        return metadata.getBucket(request.bucketName, log, (mdErr, bucket) => {
            if (mdErr) {
                log.warn('could not fetch bucket CORS config for error response', {
                    bucketName: request.bucketName,
                    error: mdErr.code || mdErr.message,
                });
                return callback(err, ...callbackArgs);
            }
            applyHeadersFromBucket(bucket);
            return callback(err, ...callbackArgs);
        });
    };
}

function checkAuthResults(authResults, apiMethod, log) {
    let returnTagCount = true;
    const isImplicitDeny = {};
    let isOnlyImplicitDeny = true;
    if (apiMethod === 'objectGet') {
        // first item checks s3:GetObject(Version) action
        if (!authResults[0].isAllowed && !authResults[0].isImplicit) {
            log.trace('get object authorization denial from Vault');
            return errors.AccessDenied;
        }
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
        return errors.AccessDenied;
    }
    return { returnTagCount, isImplicitDeny };
}

/* eslint-disable no-param-reassign */
function handleAuthorizationResults(request, authorizationResults, apiMethod, returnTagCount, log, callback) {
    if (authorizationResults) {
        const checkedResults = checkAuthResults(authorizationResults, apiMethod, log);
        if (checkedResults instanceof Error) {
            return callback(checkedResults);
        }
        returnTagCount = checkedResults.returnTagCount;
        request.actionImplicitDenies = checkedResults.isImplicitDeny;
    } else {
        // create an object of keys apiMethods with all values to false:
        // for backward compatibility, all apiMethods are allowed by default
        // thus it is explicitly allowed, so implicit deny is false
        request.actionImplicitDenies = request.apiMethods.reduce((acc, curr) => {
            acc[curr] = false;
            return acc;
        }, {});
    }
    return callback(null, { returnTagCount });
}

function callApiHandler(apiMethod, apiHandler, request, response, log, callback) {
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
        request.actionImplicitDenies = false;
        return apiHandler(request, log, callback);
    }

    const { sourceBucket, sourceObject, sourceVersionId, parsingError } =
        parseCopySource(apiMethod, request.headers['x-amz-copy-source']);
    if (parsingError) {
        log.debug('error parsing copy source', {
            error: parsingError,
        });
        return process.nextTick(callback, parsingError);
    }

    const { httpHeadersSizeError } = checkHttpHeadersSize(request.headers);
    if (httpHeadersSizeError) {
        log.debug('http header size limit exceeded', {
            error: httpHeadersSizeError,
        });
        return process.nextTick(callback, httpHeadersSizeError);
    }

    const requestContexts = prepareRequestContexts(apiMethod, request,
        sourceBucket, sourceObject, sourceVersionId);

    // Extract all the _apiMethods and store them in an array
    const apiMethods = requestContexts ? requestContexts.map(context => context._apiMethod) : [];
    // Attach the names to the current request
    request.apiMethods = apiMethods;

    // If we have a cached bucket owner for the targeted bucket, pass it to
    // Vault as a hint so the returned rate limit config is the target account.
    // Only included when the cache has a hit.
    const authOptions = {};
    if (request.bucketName) {
        const cachedOwner = rateLimitCache.getCachedBucketOwner(request.bucketName);
        if (cachedOwner) {
            request.rateLimitTargetAccount = cachedOwner;
            authOptions.targetAccount = cachedOwner;
        }
    }

    return async.waterfall([
        next => auth.server.doAuth(
            request, log, (err, userInfo, authorizationResults, streamingV4Params, infos) => {
                if (request.serverAccessLog) {
                    request.serverAccessLog.authInfo = userInfo;
                }
                if (err) {
                    // VaultClient returns standard errors, but the route requires
                    // Arsenal errors
                    const arsenalError = err.metadata ? err : errors[err.code] || errors.InternalError;
                    log.trace('authentication error', { error: err });
                    return next(arsenalError);
                }
                return next(null, userInfo, authorizationResults, streamingV4Params, infos);
            }, 's3', requestContexts, authOptions),
        (userInfo, authorizationResults, streamingV4Params, infos, next) => {
            const authNames = { accountName: userInfo.getAccountDisplayName() };
            if (userInfo.isRequesterAnIAMUser()) {
                authNames.userName = userInfo.getIAMdisplayName();
            }
            if (isRequesterASessionUser(userInfo)) {
                authNames.sessionName = userInfo.getShortid().split(':')[1];
            }
            log.addDefaultFields(authNames);
            if (request.serverAccessLog) {
                request.serverAccessLog.analyticsAccountName = authNames.accountName;
                request.serverAccessLog.analyticsUserName    = authNames.userName;
            }
            if (apiMethod === 'objectPut' || apiMethod === 'objectPutPart') {
                const setStartTurnAroundTime = () => {
                    if (request.serverAccessLog) {
                        request.serverAccessLog.startTurnAroundTime = process.hrtime.bigint();
                    }
                };
                // For 0-byte uploads, downstream handlers do not consume
                // the request stream, so 'end' never fires. Set
                // startTurnAroundTime synchronously in that case.
                if (request.headers['content-length'] === '0') {
                    setStartTurnAroundTime();
                } else {
                    request.on('end', setStartTurnAroundTime);
                }
                return next(null, userInfo, authorizationResults, streamingV4Params, infos);
            }
            // issue 100 Continue to the client
            writeContinue(request, response);

            const defaultMaxBodyLength = request.method === 'POST' ?
                    constants.oneMegaBytes : constants.halfMegaBytes;
            const MAX_BODY_LENGTH = config.apiBodySizeLimits[apiMethod] || defaultMaxBodyLength;
            const post = [];
            let bodyLength = 0;
            request.on('data', chunk => {
                bodyLength += chunk.length;
                // Sanity check on post length
                if (bodyLength <= MAX_BODY_LENGTH) {
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
                if (request.serverAccessLog) {
                    request.serverAccessLog.startTurnAroundTime = process.hrtime.bigint();
                }

                if (bodyLength > MAX_BODY_LENGTH) {
                    log.error('body length is too long for request type',
                                { bodyLength });
                    return next(errors.InvalidRequest);
                }

                const buff = Buffer.concat(post, bodyLength);

                const err = validateMethodChecksumNoChunking(request, buff, log);
                if (err) {
                    return next(err);
                }

                // Convert array of post buffers into one string
                request.post = buff.toString();
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
                if (err) {
                    log.trace('tag authentication error', { error: err });
                    return next(err);
                }
                return next(null, userInfo, authResultsWithTags, streamingV4Params, infos);
            },
        ),
        (userInfo, authorizationResults, streamingV4Params, infos, next) => handleAuthorizationResults(
            request, authorizationResults, apiMethod, returnTagCount, log, (err, res) => {
                request.accountQuotas = infos?.accountQuota;
                request.accountLimits = infos?.limits;
                if (err) {
                    return next(err);
                }
                returnTagCount = res.returnTagCount;
                return next(null, userInfo, authorizationResults, streamingV4Params);
            }),
    ], (err, userInfo, authorizationResults, streamingV4Params) => {
        if (err) {
            return callback(err);
        }
        const methodCallback = (err, ...results) => async.forEachLimit(request.finalizerHooks, 5,
                (hook, done) => hook(err, done),
                () => callback(err, ...results));

        if (apiMethod === 'objectPut' || apiMethod === 'objectPutPart') {
            request._response = response;
            return apiHandler(userInfo, request, streamingV4Params,
                log, methodCallback, authorizationResults);
        }
        if (apiMethod === 'objectCopy' || apiMethod === 'objectPutCopyPart') {
            return apiHandler(userInfo, request, sourceBucket,
                sourceObject, sourceVersionId, log, methodCallback);
        }
        if (apiMethod === 'objectGet') {
            // remove objectGetTagging/objectGetTaggingVersion from apiMethods, these were added by
            // prepareRequestContexts to determine the value of returnTagCount.
            request.apiMethods = request.apiMethods.filter(methodName => !methodName.includes('Tagging'));
            return apiHandler(userInfo, request, returnTagCount, log, callback);
        }
        return apiHandler(userInfo, request, log, methodCallback);
    });
}

const api = {
    callApiMethod(apiMethod, request, response, log, callback) {
        // Attach the apiMethod method to the request, so it can used by monitoring in the server
        request.apiMethod = apiMethod;
        callback = wrapCallbackWithErrorCorsHeaders(
            callback, request, response, log);
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
        if (request.serverAccessLog) {
            request.serverAccessLog.bucketName = request.bucketName;
            request.serverAccessLog.objectKey = request.objectKey;
            request.serverAccessLog.analyticsAction = actionLog;
        }

        // Initialize rate limit tracker flag
        request.rateLimitBucketAlreadyChecked = false;
        request.rateLimitAccountAlreadyChecked = false;

        const apiHandler = this[apiMethod];

        // Process the request with validation, authentication, and execution

        if (request.bucketName === undefined || !requestNeedsRateCheck(request)) {
            return process.nextTick(callApiHandler, apiMethod, apiHandler, request, response, log, callback);
        }

        const checks = [];
        const rateLimitConfig = getCachedRateLimitConfig(request);

        if (rateLimitConfig.bucket !== undefined) {
            request.rateLimitBucketAlreadyChecked = true;
            checks.push(...buildRateChecksFromConfig('bucket', request.bucketName, rateLimitConfig.bucket));
        }

        if (rateLimitConfig.account !== undefined) {
            request.rateLimitAccountAlreadyChecked = true;
            checks.push(...buildRateChecksFromConfig('account', rateLimitConfig.bucketOwner, rateLimitConfig.account));
        }

        if (checks.length > 0) {
            const { allowed, rateLimitSource } = checkRateLimitsForRequest(checks, log);
            if (!allowed) {
                log.addDefaultFields({
                    rateLimited: true,
                    rateLimitSource,
                });
                // Add to server access log
                if (request.serverAccessLog) {
                    /* eslint-disable no-param-reassign */
                    request.serverAccessLog.rateLimited = true;
                    request.serverAccessLog.rateLimitSource = rateLimitSource;
                    /* eslint-enable no-param-reassign */
                }

                return process.nextTick(callback, config.rateLimiting.error);
            }
        }

        return process.nextTick(callApiHandler, apiMethod, apiHandler, request, response, log, callback);
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
    bucketGetLogging,
    bucketPutLogging,
    bucketGetRateLimit,
    bucketPutRateLimit,
    bucketDeleteRateLimit,
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
    objectGetAttributes,
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
    checkAuthResults,
    handleAuthorizationResults,
};

module.exports = api;
