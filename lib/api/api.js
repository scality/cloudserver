import { auth, errors } from 'arsenal';

import bucketDelete from './bucketDelete';
import bucketDeleteCors from './bucketDeleteCors';
import bucketDeleteWebsite from './bucketDeleteWebsite';
import bucketGet from './bucketGet';
import bucketGetACL from './bucketGetACL';
import bucketGetCors from './bucketGetCors';
import bucketGetVersioning from './bucketGetVersioning';
import bucketGetWebsite from './bucketGetWebsite';
import bucketGetLocation from './bucketGetLocation';
import bucketHead from './bucketHead';
import bucketPut from './bucketPut';
import bucketPutACL from './bucketPutACL';
import bucketPutCors from './bucketPutCors';
import bucketPutVersioning from './bucketPutVersioning';
import bucketPutWebsite from './bucketPutWebsite';
import corsPreflight from './corsPreflight';
import completeMultipartUpload from './completeMultipartUpload';
import initiateMultipartUpload from './initiateMultipartUpload';
import listMultipartUploads from './listMultipartUploads';
import listParts from './listParts';
import multiObjectDelete from './multiObjectDelete';
import multipartDelete from './multipartDelete';
import objectCopy from './objectCopy';
import objectDelete from './objectDelete';
import objectGet from './objectGet';
import objectGetACL from './objectGetACL';
import objectHead from './objectHead';
import objectPut from './objectPut';
import objectPutACL from './objectPutACL';
import objectPutTagging from './objectPutTagging';
import objectPutPart from './objectPutPart';
import objectPutCopyPart from './objectPutCopyPart';
import prepareRequestContexts from
    './apiUtils/authorization/prepareRequestContexts';
import serviceGet from './serviceGet';
import vault from '../auth/vault';
import websiteGet from './websiteGet';
import websiteHead from './websiteHead';
import writeContinue from '../utilities/writeContinue';
import parseCopySource from './apiUtils/object/parseCopySource';

auth.setHandler(vault);

/* eslint-disable no-param-reassign */
const api = {
    callApiMethod(apiMethod, request, response, log, callback) {
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
            return callback(parsingError);
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
                for (let i = 0; i < authorizationResults.length; i++) {
                    if (!authorizationResults[i].isAllowed) {
                        log.trace('authorization denial from Vault');
                        return callback(errors.AccessDenied);
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
    corsPreflight,
    completeMultipartUpload,
    initiateMultipartUpload,
    listMultipartUploads,
    listParts,
    multiObjectDelete,
    multipartDelete,
    objectDelete,
    objectGet,
    objectGetACL,
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

export default api;
