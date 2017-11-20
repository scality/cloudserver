import { policies } from 'arsenal';
const RequestContext = policies.RequestContext;


/**
 * Prepares the requestContexts array to send to Vault for authorization
 * @param {string} apiMethod - api being called
 * @param {object} request - request object
 * @param {string} locationConstraint - locationConstraint if bucket put
 * operation
 * @param {string} sourceBucket - name of sourceBucket if copy request
 * @param {string} sourceObject - name of sourceObject if copy request
 * @return {RequestContext []} array of requestContexts
 */
export default function prepareRequestContexts(apiMethod, request,
    locationConstraint, sourceBucket, sourceObject) {
    // if multiObjectDelete request, we want to authenticate
    // before parsing the post body and creating multiple requestContexts
    // so send null as requestContexts to Vault to avoid authorization
    // checks at this point
    if (apiMethod === 'multiObjectDelete') {
        return null;
    }
    const requestContexts = [];
    const ip = request.headers['x-forwarded-for'] ||
    request.socket.remoteAddress;
    if (apiMethod === 'objectCopy' || apiMethod === 'objectPutCopyPart') {
        const getRequestContext = new RequestContext(request.headers,
            request.query, sourceBucket, sourceObject,
            ip, request.connection.encrypted,
            'objectGet', 's3', locationConstraint);
        const putRequestContext = new RequestContext(request.headers,
            request.query, request.bucketName, request.objectKey,
            ip, request.connection.encrypted,
            'objectPut', 's3', locationConstraint);
        requestContexts.push(getRequestContext, putRequestContext);
    } else {
        const requestContext = new RequestContext(request.headers,
            request.query, request.bucketName, request.objectKey,
            ip, request.connection.encrypted,
            apiMethod, 's3', locationConstraint);
        requestContexts.push(requestContext);
    }
    return requestContexts;
}
