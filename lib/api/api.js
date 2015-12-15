import metastore from '../metadata/in_memory/metadata.json';
import auth from '../auth/auth';

import bucketDelete from './bucketDelete';
import bucketGet from './bucketGet';
import bucketGetACL from './bucketGetACL';
import bucketHead from './bucketHead';
import bucketPut from './bucketPut';
import bucketPutACL from './bucketPutACL';
import completeMultipartUpload from './completeMultipartUpload';
import initiateMultipartUpload from './initiateMultipartUpload';
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

const api = {
    callApiMethod(apiMethod, request, callback) {
        auth(request, function authRes(err, accessKey) {
            if (err) {
                return callback(err);
            }
            try {
                this[apiMethod](accessKey, metastore, request, callback);
            } catch (e) {
                // log this error
                return callback(new Error(e.message));
            }
        }.bind(this));
    },
    bucketDelete,
    bucketGet,
    bucketGetACL,
    bucketHead,
    bucketPut,
    bucketPutACL,
    completeMultipartUpload,
    initiateMultipartUpload,
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
