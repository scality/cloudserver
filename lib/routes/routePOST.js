import { errors } from 'arsenal';

import api from '../api/api';
import routesUtils from './routesUtils';
import pushMetrics from '../utilities/pushMetrics';

const MAX_POST_LENGTH = 1024 * 1024 / 2; // 512 KB

/* eslint-disable no-param-reassign */
export default function routePOST(request, response, log, utapi) {
    log.debug('routing request', { method: 'routePOST' });

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
        request.post = Buffer.concat(post, postLength)
            .toString();
        if (request.query.uploads !== undefined) {
            // POST multipart upload
            api.callApiMethod('initiateMultipartUpload', request, log,
                (err, result) => {
                    pushMetrics(err, log, utapi, 'initiateMultipartUpload',
                        request.bucketName);
                    return routesUtils.responseXMLBody(err, result, response,
                        log);
                });
        } else if (request.query.uploadId !== undefined) {
            // POST complete multipart upload
            api.callApiMethod('completeMultipartUpload', request, log,
                (err, result) => {
                    pushMetrics(err, log, utapi, 'completeMultipartUpload',
                        request.bucketName);
                    return routesUtils.responseXMLBody(err, result, response,
                        log);
                });
        } else if (request.query.delete !== undefined) {
            // POST multiObjectDelete
            api.callApiMethod('multiObjectDelete', request, log,
                (err, xml, totalDeletedContentLength, numOfObjects) => {
                    pushMetrics(err, log, utapi, 'multiObjectDelete',
                        request.bucketName, totalDeletedContentLength,
                        numOfObjects);
                    return routesUtils.responseXMLBody(err, xml, response,
                        log);
                });
        } else {
            routesUtils.responseNoBody(errors.NotImplemented, null, response,
                200, log);
        }
        return undefined;
    });
}
/* eslint-enable no-param-reassign */
