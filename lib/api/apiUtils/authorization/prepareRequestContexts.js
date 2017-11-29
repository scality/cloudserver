const { policies } = require('arsenal');

const RequestContext = policies.RequestContext;
let apiMethodAfterVersionCheck;
const apiMethodWithVersion = { objectGetACL: true, objectPutACL: true,
objectGet: true, objectDelete: true, objectPutTagging: true,
objectGetTagging: true, objectDeleteTagging: true };

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

    const ip = request.headers['x-forwarded-for'] ||
    request.socket.remoteAddress;

    function generateRequestContext(apiMethod) {
        return new RequestContext(request.headers,
            request.query, request.bucketName, request.objectKey,
            ip, request.connection.encrypted,
            apiMethod, 's3');
    }

    if (apiMethod === 'multiObjectDelete' || apiMethod === 'bucketPut') {
        return null;
    }

    if (apiMethodWithVersion[apiMethod] && request.query &&
        request.query.versionId) {
        apiMethodAfterVersionCheck = `${apiMethod}Version`;
    } else {
        apiMethodAfterVersionCheck = apiMethod;
    }

    const requestContexts = [];

    if (apiMethodAfterVersionCheck === 'objectCopy'
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
        const getRequestContext =
          generateRequestContext(apiMethodAfterVersionCheck);
        const getTaggingRequestContext =
          generateRequestContext(objectGetTaggingAction);
        requestContexts.push(getRequestContext, getTaggingRequestContext);
    } else if (apiMethodAfterVersionCheck === 'objectPut') {
        const putRequestContext =
          generateRequestContext(apiMethodAfterVersionCheck);
        requestContexts.push(putRequestContext);
        // if put object (versioning) with tag set
        if (request.headers['x-amz-tagging']) {
            const putTaggingRequestContext =
              generateRequestContext('objectPutTagging');
            requestContexts.push(putTaggingRequestContext);
        }
        // if put object (versioning) with ACL
        if (isHeaderAcl(request.headers)) {
            const putAclRequestContext =
              generateRequestContext('objectPutACL');
            requestContexts.push(putAclRequestContext);
        }
    } else {
        const requestContext =
          generateRequestContext(apiMethodAfterVersionCheck);
        requestContexts.push(requestContext);
    }
    return requestContexts;
}

module.exports = prepareRequestContexts;
