import assert from 'assert';
import async from 'async';
import { parseString } from 'xml2js';

import { cleanup, DummyRequestLogger } from '../helpers';
import bucketPut from '../../../lib/api/bucketPut';
import initiateMultipartUpload
    from '../../../lib/api/initiateMultipartUpload';
import multipartDelete from '../../../lib/api/multipartDelete';
import { makeAuthInfo } from '../helpers';

const bucketName = 'multipartdeletebucket';
const log = new DummyRequestLogger();
const locationConstraint = 'us-east-1';
const authInfo = makeAuthInfo('accessKey1');

const namespace = 'default';
const bucketPutRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    post: '',
};
const objectKey = 'testObject';
const initiateRequest = {
    bucketName,
    namespace,
    objectKey,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: `/${objectKey}?uploads`,
};

function _createAndAbortMpu(fakeUploadID, callback) {
    async.waterfall([
        next => bucketPut(authInfo, bucketPutRequest, locationConstraint, log,
            next),
        (corsHeaders, next) =>
            initiateMultipartUpload(authInfo, initiateRequest, log, next),
        (result, corsHeaders, next) => parseString(result, next),
        (json, next) => {
            const testUploadId = fakeUploadID ? 'nonexistinguploadid' :
                json.InitiateMultipartUploadResult.UploadId[0];
            const deleteMpuRequest = {
                bucketName,
                namespace,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectKey}`,
                query: `uploadId=${testUploadId}`,
            };
            next(null, deleteMpuRequest);
        },
        (deleteMpuRequest, next) =>
            multipartDelete(authInfo, deleteMpuRequest, log, next),
    ], callback);
}

describe('Multipart Delete API', () => {
    beforeEach(() => {
        cleanup();
    });
    afterEach(() => {
        cleanup();
    });

    it('should not return error if uploadId exists on multipart abort call',
    done => {
        _createAndAbortMpu(false, err => {
            assert.strictEqual(err, undefined, `Expected no error, got ${err}`);
            done(err);
        });
    });

    it('should still not return error if uploadId does not exist on ' +
    'multipart abort call', done => {
        _createAndAbortMpu(true, err => {
            assert.strictEqual(err, undefined, `Expected no error, got ${err}`);
            done(err);
        });
    });
});
