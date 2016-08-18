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
    const requestContexts = [];
    if (apiMethod === 'objectCopy' || apiMethod === 'objectPutCopyPart') {
        const getRequestContext = new RequestContext(request.headers,
            request.query, sourceBucket, sourceObject,
            request.socket.remoteAddress, request.connection.encrypted,
            'objectGet', 's3', locationConstraint);
        const putRequestContext = new RequestContext(request.headers,
            request.query, request.bucketName, request.objectKey,
            request.socket.remoteAddress, request.connection.encrypted,
            'objectPut', 's3', locationConstraint);
        requestContexts.push(getRequestContext, putRequestContext);
    } else {
        const requestContext = new RequestContext(request.headers,
            request.query, request.bucketName, request.objectKey,
            request.socket.remoteAddress, request.connection.encrypted,
            apiMethod, 's3', locationConstraint);
        requestContexts.push(requestContext);
    }
    return requestContexts;
}
