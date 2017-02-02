import assert from 'assert';
import async from 'async';
import { errors } from 'arsenal';
import { parseString } from 'xml2js';

import config from '../../../lib/Config';
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
const originalUsEastValue = config.usEastBehavior;

function _createAndAbortMpu(usEastSetting, fakeUploadID, callback) {
    config.usEastBehavior = usEastSetting;
    async.waterfall([
        next => bucketPut(authInfo, bucketPutRequest, locationConstraint, log,
            next),
        next =>
            initiateMultipartUpload(authInfo, initiateRequest, log, next),
        (result, next) => parseString(result, next),
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
        config.usEastBehavior = originalUsEastValue; // set back to default
        cleanup();
    });

    it('should return 404 if uploadId does not exist and usEastBehavior' +
    'set to true', done => {
        _createAndAbortMpu(true, true, err => {
            assert.strictEqual(err, errors.NoSuchUpload,
                `Expected NoSuchUpload, got ${err}`);
            done();
        });
    });

    it('should return no error if uploadId does not exist and usEastBehavior' +
    'set to false', done => {
        _createAndAbortMpu(false, true, err => {
            assert.strictEqual(err, undefined, `Expected no error, got ${err}`);
            done();
        });
    });
});
