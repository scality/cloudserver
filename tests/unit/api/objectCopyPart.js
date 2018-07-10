const assert = require('assert');
const async = require('async');
const { parseString } = require('xml2js');
const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');
const objectPutCopyPart = require('../../../lib/api/objectPutCopyPart');
const initiateMultipartUpload
= require('../../../lib/api/initiateMultipartUpload');
const { metadata } = require('arsenal').storage.metadata.inMemory.metadata;
const DummyRequest = require('../DummyRequest');
const { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils }
    = require('../helpers');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const destBucketName = 'destbucketname';
const sourceBucketName = 'sourcebucketname';
const objectKey = 'objectName';

function _createBucketPutRequest(bucketName) {
    return new DummyRequest({
        bucketName,
        namespace,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
    });
}

function _createInitiateRequest(bucketName) {
    const params = {
        bucketName,
        namespace,
        objectKey,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: `/${objectKey}?uploads`,
    };
    return new DummyRequest(params);
}

function _createObjectCopyPartRequest(destBucketName, uploadId) {
    const params = {
        bucketName: destBucketName,
        namespace,
        objectKey,
        headers: {},
        url: `/${destBucketName}/${objectKey}?partNumber=1`,
        query: {
            partNumber: 1,
            uploadId,
        },
    };
    return new DummyRequest(params);
}

const putDestBucketRequest = _createBucketPutRequest(destBucketName);
const putSourceBucketRequest = _createBucketPutRequest(sourceBucketName);
const initiateRequest = _createInitiateRequest(destBucketName);

describe('objectCopyPart', () => {
    let uploadId;
    const objData = Buffer.from('foo', 'utf8');
    const testPutObjectRequest =
        versioningTestUtils.createPutObjectRequest(sourceBucketName, objectKey,
            objData);
    before(done => {
        cleanup();
        async.waterfall([
            callback => bucketPut(authInfo, putDestBucketRequest, log,
                err => callback(err)),
            callback => bucketPut(authInfo, putSourceBucketRequest, log,
                err => callback(err)),
            callback => objectPut(authInfo, testPutObjectRequest,
                undefined, log, err => callback(err)),
            callback => initiateMultipartUpload(authInfo, initiateRequest,
                log, (err, res) => callback(err, res)),
        ], (err, res) => {
            if (err) {
                return done(err);
            }
            return parseString(res, (err, json) => {
                uploadId = json.InitiateMultipartUploadResult.UploadId[0];
                return done();
            });
        });
    });

    after(() => cleanup());

    it('should copy part even if legacy metadata without dataStoreName',
    done => {
        // force metadata for dataStoreName to be undefined
        metadata.keyMaps.get(sourceBucketName)
            .get(objectKey).dataStoreName = undefined;
        const testObjectCopyRequest =
            _createObjectCopyPartRequest(destBucketName, uploadId);
        objectPutCopyPart(authInfo, testObjectCopyRequest,
            sourceBucketName, objectKey,
            undefined, log, err => {
                assert.ifError(err, `Unexpected err: ${err}`);
                done();
            });
    });
});
