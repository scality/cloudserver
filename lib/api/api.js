const { auth, errors } = require('arsenal');

const bucketDelete = require('./bucketDelete');
const bucketDeleteCors = require('./bucketDeleteCors');
const bucketDeleteWebsite = require('./bucketDeleteWebsite');
const bucketGet = require('./bucketGet');
const bucketGetACL = require('./bucketGetACL');
const bucketGetCors = require('./bucketGetCors');
const bucketGetVersioning = require('./bucketGetVersioning');
const bucketGetWebsite = require('./bucketGetWebsite');
const bucketGetLocation = require('./bucketGetLocation');
const bucketGetLifecycle = require('./bucketGetLifecycle');
const bucketHead = require('./bucketHead');
const { bucketPut } = require('./bucketPut');
const bucketPutACL = require('./bucketPutACL');
const bucketPutCors = require('./bucketPutCors');
const bucketPutVersioning = require('./bucketPutVersioning');
const bucketPutWebsite = require('./bucketPutWebsite');
const bucketPutReplication = require('./bucketPutReplication');
const bucketPutLifecycle = require('./bucketPutLifecycle');
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
const objectGetTagging = require('./objectGetTagging');
const objectHead = require('./objectHead');
const objectPut = require('./objectPut');
const objectPutACL = require('./objectPutACL');
const objectPutTagging = require('./objectPutTagging');
const objectPutPart = require('./objectPutPart');
const objectPutCopyPart = require('./objectPutCopyPart');
const prepareRequestContexts
    = require('./apiUtils/authorization/prepareRequestContexts');
const serviceGet = require('./serviceGet');
const vault = require('../auth/vault');
const websiteGet = require('./websiteGet');
const websiteHead = require('./websiteHead');
const writeContinue = require('../utilities/writeContinue');
const validateQueryAndHeaders = require('../utilities/validateQueryAndHeaders');
const parseCopySource = require('./apiUtils/object/parseCopySource');

auth.setHandler(vault);

/* eslint-disable no-param-reassign */
const api = {
    callApiMethod(apiMethod, request, response, log, callback) {
        let returnTagCount = true;

        const validationRes =
            validateQueryAndHeaders(request.method, request.query,
                request.headers, log);
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
        return auth.server.doAuth(request, log, (err, userInfo,
            authorizationResults, streamingV4Params) => {
            if (err) {
                log.trace('authentication error', { error: err });
                return callback(err);
            }
            if (authorizationResults) {
                if (apiMethod === 'objectGet') {
                    // first item checks s3:GetObject(Version) action
                    if (!authorizationResults[0].isAllowed) {
                        log.trace('get object authorization denial from Vault');
                        return callback(errors.AccessDenied);
                    }
                    // second item checks s3:GetObject(Version)Tagging action
                    if (!authorizationResults[1].isAllowed) {
                        log.trace('get tagging authorization denial ' +
                        'from Vault');
                        returnTagCount = false;
                    }
                } else {
                    for (let i = 0; i < authorizationResults.length; i++) {
                        if (!authorizationResults[i].isAllowed) {
                            log.trace('authorization denial from Vault');
                            return callback(errors.AccessDenied);
                        }
                    }
                }
            }
            // issue 100 Continue to the client
            writeContinue(request, response);
            if (apiMethod === 'objectPut' || apiMethod === 'objectPutPart') {
                return this[apiMethod](userInfo, request, streamingV4Params,
                    log, callback);
            }
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
            return undefined;
        }, 's3', requestContexts);
    },
    bucketDelete,
    bucketDeleteCors,
    bucketDeleteWebsite,
    bucketGet,
    bucketGetACL,
    bucketGetCors,
    bucketGetVersioning,
    bucketGetWebsite,
    bucketGetLocation,
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
    objectGetTagging,
    objectCopy,
    objectHead,
    objectPut,
    objectPutACL,
    objectPutTagging,
    objectPutPart,
    objectPutCopyPart,
    serviceGet,
    websiteGet,
    websiteHead,
};

module.exports = api;
