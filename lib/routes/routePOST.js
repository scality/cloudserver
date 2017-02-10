import { errors } from 'arsenal';

import api from '../api/api';
import routesUtils from './routesUtils';

const MAX_POST_LENGTH = 1024 * 1024; // 1 MB

/* eslint-disable no-param-reassign */
export default function routePOST(request, response, log) {
    log.debug('routing request', { method: 'routePOST' });

    const invalidMultiObjectDelReq = request.query.delete !== undefined
        && request.bucketName === undefined;
    if (invalidMultiObjectDelReq) {
        return routesUtils.responseNoBody(errors.MethodNotAllowed, null,
            response, null, log);
    }

    request.post = '';

    const invalidInitiateMpuReq = request.query.uploads !== undefined
        && request.objectKey === undefined;
    const invalidCompleteMpuReq = request.query.uploadId !== undefined
        && request.objectKey === undefined;
    if (invalidInitiateMpuReq || invalidCompleteMpuReq) {
        return routesUtils.responseNoBody(errors.InvalidURI, null,
            response, null, log);
    }

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

    request.on('end', () => {
        if (postLength > MAX_POST_LENGTH) {
            log.error('body length is too long for request type',
                { postLength });
            return routesUtils.responseXMLBody(errors.InvalidRequest, null,
                response, log);
        }
        // Convert array of post buffers into one string
        request.post = Buffer.concat(post, postLength).toString();

        // POST initiate multipart upload
        if (request.query.uploads !== undefined) {
            return api.callApiMethod('initiateMultipartUpload', request, log,
                (err, result, corsHeaders) =>
                routesUtils.responseXMLBody(err, result, response, log,
                    corsHeaders));
        }

        // POST complete multipart upload
        if (request.query.uploadId !== undefined) {
            console.log('Received request in routePOST', Date.now(), new Date().toUTCString())
            return api.callApiMethod('completeMultipartUpload', request, log,
                (err, result, corsHeaders) => {
                console.log('===========================');
                console.log('We reached callback in routePOST for completeMpu.', Date.now(), new Date().toUTCString());
                console.log(`callback param: err ${JSON.stringify(err)}`)
                console.log(`callback param: result ${JSON.stringify(result)}`)
                console.log(`callback param: corsHeaders ${JSON.stringify(corsHeaders)}`)
                console.log('===========================')
                routesUtils.responseXMLBody(err, result, response, log,
                    corsHeaders)
                });
        }

        // POST multiObjectDelete
        if (request.query.delete !== undefined) {
            return api.callApiMethod('multiObjectDelete', request, log,
                (err, xml, corsHeaders) =>
                routesUtils.responseXMLBody(err, xml, response, log,
                    corsHeaders));
        }

        return routesUtils.responseNoBody(errors.NotImplemented, null, response,
                200, log);
    });
    return undefined;
}
/* eslint-enable no-param-reassign */
