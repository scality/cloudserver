const assert = require('assert');
const async = require('async');
const { parseString } = require('xml2js');
const sinon = require('sinon');

const { cleanup, DummyRequestLogger } = require('../helpers');
const { config } = require('../../../lib/Config');
const services = require('../../../lib/services');
const DummyRequest = require('../DummyRequest');
const { bucketPut } = require('../../../lib/api/bucketPut');
const initiateMultipartUpload
    = require('../../../lib/api/initiateMultipartUpload');
const multipartDelete = require('../../../lib/api/multipartDelete');
const objectPutPart = require('../../../lib/api/objectPutPart');
const { makeAuthInfo } = require('../helpers');

const bucketName = 'multipartdeletebucket';
const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');

const namespace = 'default';
const bucketPutRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};
const objectKey = 'testObject';
const initiateRequest = {
    bucketName,
    namespace,
    objectKey,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: `/${objectKey}?uploads`,
};
const eastLocation = 'us-east-1';
const westLocation = 'scality-internal-file';

function _createAndAbortMpu(usEastSetting, fakeUploadID, locationConstraint,
    callback) {
    config.locationConstraints['us-east-1'].legacyAwsBehavior =
        usEastSetting;
    let uploadId;
    const post = '<?xml version="1.0" encoding="UTF-8"?>' +
        '<CreateBucketConfiguration ' +
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        `<LocationConstraint>${locationConstraint}</LocationConstraint>` +
        '</CreateBucketConfiguration>';
    const testBucketPutRequest = Object.assign({ post }, bucketPutRequest);
    async.waterfall([
        next => bucketPut(authInfo, testBucketPutRequest, log, next),
        (corsHeaders, next) =>
            initiateMultipartUpload(authInfo, initiateRequest, log, next),
        (result, corsHeaders, next) => parseString(result, next),
        (json, next) => {
            // use uploadId parsed from initiateMpu request to construct
            // uploadPart and deleteMpu requests
            uploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const partBody = Buffer.from('I am a part\n', 'utf8');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectKey}?partNumber=1&uploadId=${uploadId}`,
                query: {
                    partNumber: '1',
                    uploadId,
                },
            }, partBody);
            const testUploadId = fakeUploadID ? 'nonexistinguploadid' :
                uploadId;
            const deleteMpuRequest = {
                bucketName,
                namespace,
                objectKey,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectKey}?uploadId=${testUploadId}`,
                query: { uploadId: testUploadId },
            };
            next(null, partRequest, deleteMpuRequest);
        },
        (partRequest, deleteMpuRequest, next) =>
            objectPutPart(authInfo, partRequest, undefined, log, err => {
                if (err) {
                    return next(err);
                }
                return next(null, deleteMpuRequest);
            }),
        (deleteMpuRequest, next) =>
            multipartDelete(authInfo, deleteMpuRequest, log, next),
    ], err => callback(err, uploadId));
}
// TODO CLDSRV-431 remove skip
describe.skip('Multipart Delete API', () => {
    beforeEach(() => {
        cleanup();
    });
    afterEach(() => {
        // set back to original
        config.locationConstraints['us-east-1'].legacyAwsBehavior =
            true;
        cleanup();
    });

    it('should not return error if mpu exists with uploadId and at least ' +
    'one part', done => {
        _createAndAbortMpu(true, false, eastLocation, err => {
            assert.ifError(err);
            done(err);
        });
    });

    it('should still not return error if uploadId does not exist on ' +
    'multipart abort call, in region other than us-east-1', done => {
        _createAndAbortMpu(true, true, westLocation, err => {
            assert.ifError(err);
            done(err);
        });
    });

    it('bucket created in us-east-1: should return 404 if uploadId does not ' +
    'exist and legacyAwsBehavior set to true',
    done => {
        _createAndAbortMpu(true, true, eastLocation, err => {
            assert.strictEqual(err.is.NoSuchUpload, true);
            done();
        });
    });

    it('bucket created in us-east-1: should return no error ' +
    'if uploadId does not exist and legacyAwsBehavior set to false', done => {
        _createAndAbortMpu(false, true, eastLocation, err => {
            assert.strictEqual(err, null, `Expected no error, got ${err}`);
            done();
        });
    });

    it('should send a PUT to bucketd with `isAbort` and `replayId`', done => {
        const spy = sinon.spy(services, 'sendAbortMPUPut');
        _createAndAbortMpu(true, false, eastLocation, (err, uploadId) => {
            assert.ifError(err);
            assert.strictEqual(spy.calledOnce, true);
            assert.strictEqual(
                spy.calledOnceWith(bucketName, objectKey, uploadId), true);
            done();
        });
    });
});
