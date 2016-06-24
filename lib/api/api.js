import { auth } from 'arsenal';

import bucketDelete from './bucketDelete';
import bucketGet from './bucketGet';
import bucketGetACL from './bucketGetACL';
import bucketHead from './bucketHead';
import bucketPut from './bucketPut';
import bucketPutACL from './bucketPutACL';
import completeMultipartUpload from './completeMultipartUpload';
import initiateMultipartUpload from './initiateMultipartUpload';
import listMultipartUploads from './listMultipartUploads';
import listParts from './listParts';
import multipartDelete from './multipartDelete';
import objectDelete from './objectDelete';
import objectGet from './objectGet';
import objectGetACL from './objectGetACL';
import objectHead from './objectHead';
import objectPut from './objectPut';
import objectPutACL from './objectPutACL';
import objectPutPart from './objectPutPart';
import serviceGet from './serviceGet';
import vault from '../auth/vault';

auth.setAuthHandler(vault);

const api = {
    callApiMethod(apiMethod, request, log, callback) {
        auth.doAuth(request, log, (err, authInfo) => {
            if (err) {
                log.trace('authentication error', { error: err });
                return callback(err);
            }
            return this[apiMethod](authInfo, request, log, callback);
        }, 's3', request.query);
    },
    bucketDelete,
    bucketGet,
    bucketGetACL,
    bucketHead,
    bucketPut,
    bucketPutACL,
    completeMultipartUpload,
    initiateMultipartUpload,
    listMultipartUploads,
    listParts,
    multipartDelete,
    objectDelete,
    objectGet,
    objectGetACL,
    objectHead,
    objectPut,
    objectPutACL,
    objectPutPart,
    serviceGet,
};

export default api;
