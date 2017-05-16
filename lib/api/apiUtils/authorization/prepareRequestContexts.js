import { policies } from 'arsenal';

const RequestContext = policies.RequestContext;
let apiMethodAfterVersionCheck;
const apiMethodWithVersion = { objectGetACL: true, objectPutACL: true,
objectGet: true, objectDelete: true, objectPutTagging: true,
objectGetTagging: true, objectDeleteTagging: true };

/**
 * Prepares the requestContexts array to send to Vault for authorization
 * @param {string} apiMethod - api being called
 * @param {object} request - request object
 * @param {string} sourceBucket - name of sourceBucket if copy request
 * @param {string} sourceObject - name of sourceObject if copy request
 * @param {string} sourceVersionId - value of sourceVersionId if copy request
 * @return {RequestContext []} array of requestContexts
 */
export default function prepareRequestContexts(apiMethod, request, sourceBucket,
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
    if (apiMethodAfterVersionCheck === 'objectCopy' ||
    apiMethodAfterVersionCheck === 'objectPutCopyPart') {
        const objectGetAction = sourceVersionId ? 'objectGetVersion' :
          'objectGet';
        const reqQuery = Object.assign({}, request.query,
            { versionId: sourceVersionId });
        const getRequestContext = new RequestContext(request.headers,
            reqQuery, sourceBucket, sourceObject,
            request.socket.remoteAddress, request.connection.encrypted,
            objectGetAction, 's3');
        const putRequestContext = new RequestContext(request.headers,
            request.query, request.bucketName, request.objectKey,
            request.socket.remoteAddress, request.connection.encrypted,
            'objectPut', 's3');
        requestContexts.push(getRequestContext, putRequestContext);
    } else if (apiMethodAfterVersionCheck === 'objectGet' ||
      apiMethodAfterVersionCheck === 'objectGetVersion') {
        const objectGetTaggingAction = (request.query &&
          request.query.versionId) ? 'objectGetTaggingVersion' :
          'objectGetTagging';
        const getRequestContext = new RequestContext(request.headers,
            request.query, request.bucketName, request.objectKey,
            request.socket.remoteAddress, request.connection.encrypted,
            apiMethodAfterVersionCheck, 's3');
        const getTaggingRequestContext = new RequestContext(request.headers,
            request.query, request.bucketName, request.objectKey,
            request.socket.remoteAddress, request.connection.encrypted,
            objectGetTaggingAction, 's3');
        requestContexts.push(getRequestContext, getTaggingRequestContext);
    } else {
        const requestContext = new RequestContext(request.headers,
            request.query, request.bucketName, request.objectKey,
            request.socket.remoteAddress, request.connection.encrypted,
            apiMethodAfterVersionCheck, 's3');
        requestContexts.push(requestContext);
    }
    return requestContexts;
}
