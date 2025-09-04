const { policies } = require('arsenal');
const { hasGovernanceBypassHeader } = require('../object/objectLockHelpers');

const { RequestContext } = policies;

// GC Optimization: RequestContext pool to avoid repeated allocations
class RequestContextPool {
    constructor(maxSize = 20) {
        this.pool = [];
        this.maxSize = maxSize;
    }

    get(headers, query, bucketName, objectKey, ip, isSecure, apiMethod, service) {
        let context;
        if (this.pool.length > 0) {
            context = this.pool.pop();
            // Reset/reinitialize the pooled context
            context._headers = headers;
            context._query = query;
            context._bucketName = bucketName;
            context._objectKey = objectKey;
            context._ip = ip;
            context._isSecure = isSecure;
            context._apiMethod = apiMethod;
            context._service = service;
            // Reset other properties that might have been set
            context._needQuota = undefined;
        } else {
            context = new RequestContext(headers, query, bucketName, objectKey, ip, isSecure, apiMethod, service);
        }
        return context;
    }

    release(context) {
        if (this.pool.length < this.maxSize && context) {
            // Clear references to potentially large objects
            context._needQuota = undefined;
            this.pool.push(context);
        }
    }

    releaseMultiple(contexts) {
        if (Array.isArray(contexts)) {
            contexts.forEach(ctx => this.release(ctx));
        }
    }
}

// Global pool instance
const contextPool = new RequestContextPool();

// GC Optimization: Cache common API method strings to avoid concatenation
const versionedApiMethodCache = new Map();
function getVersionedApiMethod(apiMethod) {
    let versionedMethod = versionedApiMethodCache.get(apiMethod);
    if (!versionedMethod) {
        versionedMethod = `${apiMethod}Version`;
        versionedApiMethodCache.set(apiMethod, versionedMethod);
    }
    return versionedMethod;
}

let apiMethodAfterVersionCheck;
const apiMethodWithVersion = {
    objectGetACL: true,
    objectPutACL: true,
    objectGet: true,
    objectDelete: true,
    objectPutTagging: true,
    objectGetTagging: true,
    objectDeleteTagging: true,
    objectGetLegalHold: true,
    objectPutLegalHold: true,
    objectPutRetention: true,
};

function isHeaderAcl(headers) {
    return headers['x-amz-grant-read'] || headers['x-amz-grant-read-acp'] ||
    headers['x-amz-grant-write-acp'] || headers['x-amz-grant-full-control'] ||
    headers['x-amz-acl'];
}

/**
 * Prepares the requestContexts array to send to Vault for authorization
 * @param {string} apiMethod - api being called
 * @param {object} request - request object
 * @param {string} sourceBucket - name of sourceBucket if copy request
 * @param {string} sourceObject - name of sourceObject if copy request
 * @param {string} sourceVersionId - value of sourceVersionId if copy request
 * @return {RequestContext []} array of requestContexts
 */
function prepareRequestContexts(apiMethod, request, sourceBucket,
    sourceObject, sourceVersionId) {
    // if multiObjectDelete request, we want to authenticate
    // before parsing the post body and creating multiple requestContexts
    // so send null as requestContexts to Vault to avoid authorization
    // checks at this point
    //
    // If bucketPut request, we want to do the authorization check in the API
    // itself (once we parse the locationConstraint from the xml body) so send
    // null as the requestContext to Vault so it will only do an authentication
    // check.

    const ip = request.clientIP; //requestUtils.getClientIp(request, config);
    const isSecure = request.isSecure; //requestUtils.getHttpProtocolSecurity(request, config);

    // GC Optimization: Use pooled RequestContext instead of always creating new ones
    function generateRequestContext(apiMethod) {
        return contextPool.get(request.headers, request.query, request.bucketName, 
            request.objectKey, ip, isSecure, apiMethod, 's3');
    }

    if (apiMethod === 'bucketPut') {
        return null;
    }

    // GC Optimization: Use cached string concatenation
    if (apiMethodWithVersion[apiMethod] && request.query &&
        request.query.versionId) {
        apiMethodAfterVersionCheck = getVersionedApiMethod(apiMethod);
    } else {
        apiMethodAfterVersionCheck = apiMethod;
    }

    const requestContexts = [];

    if (apiMethod === 'multiObjectDelete') {
        // MultiObjectDelete does not require any authorization when evaluating
        // the API. Instead, we authorize each object passed.
        // But in order to get any relevant information from the authorization service
        // for example, the account quota, we must send a request context object
        // with no `specificResource`. We expect the result to be an implicit deny.
        // In the API, we then ignore these authorization results, and we can use
        // any information returned, e.g., the quota.
        const requestContextMultiObjectDelete = generateRequestContext('objectDelete');
          requestContexts.push(requestContextMultiObjectDelete);
    } else if (apiMethodAfterVersionCheck === 'objectCopy'
        || apiMethodAfterVersionCheck === 'objectPutCopyPart') {
        const objectGetAction = sourceVersionId ? 'objectGetVersion' :
          'objectGet';
        // GC Optimization: Avoid Object.assign, create minimal query object
        const reqQuery = sourceVersionId ? 
            { ...request.query, versionId: sourceVersionId } : request.query;
        const getRequestContext = contextPool.get(request.headers,
            reqQuery, sourceBucket, sourceObject,
            ip, isSecure, objectGetAction, 's3');
        const putRequestContext = generateRequestContext('objectPut');
        requestContexts.push(getRequestContext, putRequestContext);
        if (apiMethodAfterVersionCheck === 'objectCopy') {
            // if tagging directive is COPY, "s3:PutObjectTagging" don't need
            // to be included in the list of permitted actions in IAM policy
            if (request.headers['x-amz-tagging'] &&
                request.headers['x-amz-tagging-directive'] === 'REPLACE') {
                const putTaggingRequestContext =
                    generateRequestContext('objectPutTagging');
                requestContexts.push(putTaggingRequestContext);
            }
            if (isHeaderAcl(request.headers)) {
                const putAclRequestContext =
                  generateRequestContext('objectPutACL');
                requestContexts.push(putAclRequestContext);
            }
        }
    } else if (apiMethodAfterVersionCheck === 'objectGet'
               || apiMethodAfterVersionCheck === 'objectGetVersion') {
        const objectGetTaggingAction = (request.query &&
          request.query.versionId) ? 'objectGetTaggingVersion' :
          'objectGetTagging';
        if (request.headers['x-amz-version-id']) {
            const objectGetVersionAction = 'objectGetVersion';
            const getVersionResourceVersion =
                generateRequestContext(objectGetVersionAction);
            requestContexts.push(getVersionResourceVersion);
        }
        const getRequestContext =
          generateRequestContext(apiMethodAfterVersionCheck);
        const getTaggingRequestContext =
          generateRequestContext(objectGetTaggingAction);
        requestContexts.push(getRequestContext, getTaggingRequestContext);
    } else if (apiMethodAfterVersionCheck === 'objectGetTagging') {
        const objectGetTaggingAction = 'objectGetTagging';
        const getTaggingResourceVersion =
            generateRequestContext(objectGetTaggingAction);
        requestContexts.push(getTaggingResourceVersion);
        if (request.headers['x-amz-version-id']) {
            const objectGetTaggingVersionAction = 'objectGetTaggingVersion';
            const getTaggingVersionResourceVersion =
                generateRequestContext(objectGetTaggingVersionAction);
            requestContexts.push(getTaggingVersionResourceVersion);
        }
    } else if (apiMethodAfterVersionCheck === 'objectHead') {
        const objectHeadAction = 'objectHead';
        const headObjectAction =
            generateRequestContext(objectHeadAction);
        requestContexts.push(headObjectAction);
        if (request.headers['x-amz-version-id']) {
            const objectHeadVersionAction = 'objectGetVersion';
            const headObjectVersion =
                generateRequestContext(objectHeadVersionAction);
            requestContexts.push(headObjectVersion);
        }
        if (request.headers['x-amz-scal-archive-info']) {
            const coldStatus =
                generateRequestContext('objectGetArchiveInfo');
            requestContexts.push(coldStatus);
        }
    } else if (apiMethodAfterVersionCheck === 'objectPutTagging') {
        const putObjectTaggingRequestContext =
            generateRequestContext('objectPutTagging');
        requestContexts.push(putObjectTaggingRequestContext);
        if (request.headers['x-amz-version-id']) {
            const putObjectVersionRequestContext =
                generateRequestContext('objectPutTaggingVersion');
            requestContexts.push(putObjectVersionRequestContext);
        }
    } else if (apiMethodAfterVersionCheck === 'objectPut') {
        // if put object with version
        if (request.headers['x-scal-s3-version-id'] ||
        request.headers['x-scal-s3-version-id'] === '') {
            const putVersionRequestContext =
              generateRequestContext('objectPutVersion');
            requestContexts.push(putVersionRequestContext);
        } else {
            const putRequestContext =
              generateRequestContext(apiMethodAfterVersionCheck);
            requestContexts.push(putRequestContext);
            
            // GC Optimization: Batch header checks to avoid multiple property lookups
            const headers = request.headers;
            const hasTagging = headers['x-amz-tagging'];
            const hasLegalHold = ['ON', 'OFF'].includes(headers['x-amz-object-lock-legal-hold-status']);
            const hasAcl = isHeaderAcl(headers);
            const hasObjectLock = headers['x-amz-object-lock-mode'];
            const hasVersionId = headers['x-amz-version-id'];
            const hasGovernanceBypass = hasObjectLock && hasGovernanceBypassHeader(headers);
            
            // Create contexts only if needed
            if (hasTagging) {
                const putTaggingRequestContext =
                  generateRequestContext('objectPutTagging');
                requestContexts.push(putTaggingRequestContext);
            }
            if (hasLegalHold) {
                const putLegalHoldStatusAction =
                    generateRequestContext('objectPutLegalHold');
                requestContexts.push(putLegalHoldStatusAction);
            }
            if (hasAcl) {
                const putAclRequestContext =
                  generateRequestContext('objectPutACL');
                requestContexts.push(putAclRequestContext);
            }
            if (hasObjectLock) {
                const putObjectLockRequestContext =
                  generateRequestContext('objectPutRetention');
                requestContexts.push(putObjectLockRequestContext);
                if (hasGovernanceBypass) {
                    const checkUserGovernanceBypassRequestContext =
                        generateRequestContext('bypassGovernanceRetention');
                    requestContexts.push(checkUserGovernanceBypassRequestContext);
                }
            }
            if (hasVersionId) {
                const putObjectVersionRequestContext =
                    generateRequestContext('objectPutTaggingVersion');
                requestContexts.push(putObjectVersionRequestContext);
            }
        }
    } else if (apiMethodAfterVersionCheck === 'objectPutRetention' ||
        apiMethodAfterVersionCheck === 'objectPutRetentionVersion') {
        const putRetentionRequestContext =
            generateRequestContext(apiMethodAfterVersionCheck);
        requestContexts.push(putRetentionRequestContext);
        if (hasGovernanceBypassHeader(request.headers)) {
            const checkUserGovernanceBypassRequestContext =
                generateRequestContext('bypassGovernanceRetention');
            requestContexts.push(checkUserGovernanceBypassRequestContext);
        }
    } else if (apiMethodAfterVersionCheck === 'initiateMultipartUpload' ||
      apiMethodAfterVersionCheck === 'objectPutPart' ||
      apiMethodAfterVersionCheck === 'completeMultipartUpload'
      ) {
        if (request.headers['x-scal-s3-version-id'] ||
        request.headers['x-scal-s3-version-id'] === '') {
            const putVersionRequestContext =
              generateRequestContext('objectPutVersion');
            requestContexts.push(putVersionRequestContext);
        } else {
            const putRequestContext =
              generateRequestContext(apiMethodAfterVersionCheck);
            requestContexts.push(putRequestContext);
        }

        // if put object (versioning) with ACL
        if (isHeaderAcl(request.headers)) {
            const putAclRequestContext =
              generateRequestContext('objectPutACL');
            requestContexts.push(putAclRequestContext);
        }

        if (request.headers['x-amz-object-lock-mode']) {
            const putObjectLockRequestContext =
              generateRequestContext('objectPutRetention');
            requestContexts.push(putObjectLockRequestContext);
        }
        if (request.headers['x-amz-version-id']) {
            const putObjectVersionRequestContext =
                generateRequestContext('objectPutTaggingVersion');
            requestContexts.push(putObjectVersionRequestContext);
        }
    // AWS only returns an object lock error if a version id
    // is specified, else continue to create a delete marker
    } else if (sourceVersionId && apiMethodAfterVersionCheck === 'objectDeleteVersion') {
        const deleteRequestContext =
          generateRequestContext(apiMethodAfterVersionCheck);
        requestContexts.push(deleteRequestContext);
        if (hasGovernanceBypassHeader(request.headers)) {
            const checkUserGovernanceBypassRequestContext =
                generateRequestContext('bypassGovernanceRetention');
            requestContexts.push(checkUserGovernanceBypassRequestContext);
        }
    } else {
        const requestContext =
          generateRequestContext(apiMethodAfterVersionCheck);
        requestContexts.push(requestContext);
    }

    if (apiMethod === 'completeMultipartUpload' || apiMethod === 'multipartDelete') {
        // Request account quotas explicitly for MPU requests, to consider parts cleanup
        // NOTE: we need quota for these, but it will be evaluated at the end of the API,
        // once the parts have actually been deleted (not via standardMetadataValidateBucketAndObj)
        requestContexts.forEach(context => {
            context._needQuota = true; // eslint-disable-line no-param-reassign
        });
    }

    return requestContexts;
}

// Export the cleanup function for releasing pooled contexts
function releaseRequestContexts(requestContexts) {
    contextPool.releaseMultiple(requestContexts);
}

module.exports = prepareRequestContexts;
module.exports.releaseRequestContexts = releaseRequestContexts;
