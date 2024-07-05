const { auth, errors, policies } = require('arsenal');
const async = require('async');
const busboy = require('@fastify/busboy');

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
const objectPost = require('./objectPost');
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
    callApiMethod(apiMethod, request, response, log, callback) {
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
        // eslint-disable-next-line no-param-reassign
        request.apiMethods = apiMethods;

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
                return errors.AccessDenied;
            }
            return { returnTagCount, isImplicitDeny };
        }
        let bb;
        let fileEventData = null;

        if (apiMethod === 'objectPost') {
            bb = busboy({ headers: request.headers });
        }

        return async.waterfall([
            next => {
                if (apiMethod === 'objectPut' || apiMethod === 'objectPutPart') {
                    return next(null);
                }
                if (apiMethod === 'objectPost' && request.headers['content-type'].includes('multipart/form-data')) {
                    writeContinue(request, response);

                    let algoOK = false;
                    let credOK = false;
                    let dateOK = false;
                    let sigOK = false;
                    let policyOK = false;
                    request.formData = {};
                    let totalFieldSize = 0;
                    const MAX_FIELD_SIZE = 20 * 1024; // 20KB
                    bb.on('field', (fieldname, val) => {
                        totalFieldSize += Buffer.byteLength(val, 'utf8');
                        if (totalFieldSize > MAX_FIELD_SIZE) {
                            const err = errors.MaxPostPreDataLengthExceeded;
                            bb.emit('error', err);
                        }
                        // Convert fieldname to lowercase for case-insensitive comparison
                        const lowerFieldname = fieldname.toLowerCase();
                        request.formData[lowerFieldname] = val;
                        if (lowerFieldname === 'x-amz-algorithm') {
                            algoOK = true;
                        }
                        if (lowerFieldname === 'x-amz-credential') {
                            credOK = true;
                        }
                        if (lowerFieldname === 'x-amz-date') {
                            dateOK = true;
                        }
                        if (lowerFieldname === 'x-amz-signature') {
                            sigOK = true;
                        }
                        if (lowerFieldname === 'policy') {
                            policyOK = true;
                        }
                    });

                    bb.on('file', (fieldname, file, filename, encoding, mimetype) => {
                        fileEventData = { fieldname, file, filename, encoding, mimetype };
                        if (algoOK && credOK && dateOK && sigOK && policyOK) {
                            return next(null);
                        }
                        return undefined;
                    });

                    bb.on('finish', () => {
                        // if fields are found but no file, continue
                        if ((algoOK && credOK && dateOK && sigOK && policyOK) && !fileEventData) {
                            return next(null);
                        }
                        return undefined;
                    });

                    bb.on('error', (err) => {
                        log.trace('Error processing form data:', {
                            error: err,
                        });
                        request.unpipe(bb);
                        return next(err);
                    });

                    request.pipe(bb);
                } else {
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
                        request.post = Buffer.concat(post, postLength).toString();
                        return next(null);
                    });
                }
                return undefined;
            },
            next => auth.server.doAuth(
                request, log, (err, userInfo, authorizationResults, streamingV4Params) => {
                    if (err) {
                        log.trace('authentication error', { error: err });
                        return next(err);
                    }
                    return next(null, userInfo, authorizationResults, streamingV4Params);
                }, 's3', requestContexts),
            (userInfo, authorizationResults, streamingV4Params, next) => {
                const authNames = { accountName: userInfo.getAccountDisplayName() };
                if (userInfo.isRequesterAnIAMUser()) {
                    authNames.userName = userInfo.getIAMdisplayName();
                }
                log.addDefaultFields(authNames);
                return next(null, userInfo, authorizationResults, streamingV4Params);
            },
            // Tag condition keys require information from CloudServer for evaluation
            (userInfo, authorizationResults, streamingV4Params, next) => tagConditionKeyAuth(
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
                    return next(null, userInfo, authResultsWithTags, streamingV4Params);
                },
            ),
        ], (err, userInfo, authorizationResults, streamingV4Params) => {
            if (err) {
                return callback(err);
            }
            if (authorizationResults) {
                const checkedResults = checkAuthResults(authorizationResults);
                if (checkedResults instanceof Error) {
                    return callback(checkedResults);
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
            if (apiMethod === 'objectPost') {
                request._response = response;
                if (fileEventData) {
                    request.fileEventData = fileEventData;
                    request.headers['content-type'] = fileEventData.mimetype;
                }
                return this[apiMethod](userInfo, request, streamingV4Params,
                    log, callback, authorizationResults);
            }
            if (apiMethod === 'objectPut' || apiMethod === 'objectPutPart') {
                request._response = response;
                return this[apiMethod](userInfo, request, streamingV4Params,
                    log, callback, authorizationResults);
            }
            if (apiMethod === 'objectCopy' || apiMethod === 'objectPutCopyPart') {
                return this[apiMethod](userInfo, request, sourceBucket,
                    sourceObject, sourceVersionId, log, callback);
            }
            if (apiMethod === 'objectGet') {
                return this[apiMethod](userInfo, request, returnTagCount, log, callback);
            }
            return this[apiMethod](userInfo, request, log, callback);
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
    objectPost,
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
