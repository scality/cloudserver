const { policies } = require('arsenal');
const { config } = require('../../../Config');

const { RequestContext, requestUtils } = policies;
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

    const ip = requestUtils.getClientIp(request, config);

    function generateRequestContext(apiMethod) {
        return new RequestContext(request.headers,
            request.query, request.bucketName, request.objectKey,
            ip, request.connection.encrypted,
            apiMethod, 's3');
    }

    if (apiMethod === 'bucketPut') {
        return null;
    }

    if (apiMethodWithVersion[apiMethod] && request.query &&
        request.query.versionId) {
        apiMethodAfterVersionCheck = `${apiMethod}Version`;
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
        const reqQuery = Object.assign({}, request.query,
            { versionId: sourceVersionId });
        const getRequestContext = new RequestContext(request.headers,
            reqQuery, sourceBucket, sourceObject,
            ip, request.connection.encrypted,
            objectGetAction, 's3');
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
    } else if (apiMethodAfterVersionCheck === 'objectPutCopyPart') {
        const putObjectRequestContext =
            generateRequestContext('objectPut');
        requestContexts.push(putObjectRequestContext);
        const getObjectRequestContext =
            generateRequestContext('objectGet');
        requestContexts.push(getObjectRequestContext);
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
            // if put object (versioning) with tag set
            if (request.headers['x-amz-tagging']) {
                const putTaggingRequestContext =
                  generateRequestContext('objectPutTagging');
                requestContexts.push(putTaggingRequestContext);
            }
            if (['ON', 'OFF'].includes(request.headers['x-amz-object-lock-legal-hold-status'])) {
                const putLegalHoldStatusAction =
                    generateRequestContext('objectPutLegalHold');
                requestContexts.push(putLegalHoldStatusAction);
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
    } else {
        const requestContext =
          generateRequestContext(apiMethodAfterVersionCheck);
        requestContexts.push(requestContext);
    }
    return requestContexts;
}

module.exports = prepareRequestContexts;
