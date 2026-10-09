const assert = require('assert');
const async = require('async');
const sinon = require('sinon');
const { parseString } = require('xml2js');
const { storage } = require('arsenal');
const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutPolicy = require('../../../lib/api/bucketPutPolicy');
const objectPut = require('../../../lib/api/objectPut');
const objectPutCopyPart = require('../../../lib/api/objectPutCopyPart');
const initiateMultipartUpload
= require('../../../lib/api/initiateMultipartUpload');
const { metadata } = storage.metadata.inMemory.metadata;
const metadataswitch = require('../metadataswitch');
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

function _createObjectCopyPartRequest(destBucketName, uploadId, headers) {
    const params = {
        bucketName: destBucketName,
        namespace,
        objectKey,
        headers: headers || {},
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
        sinon.spy(metadataswitch, 'putObjectMD');
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

    after(() => {
        metadataswitch.putObjectMD.restore();
        cleanup();
    });

    it('should copy part even if legacy metadata without dataStoreName',
    done => {
        // force metadata for dataStoreName to be undefined
        metadata.keyMaps.get(sourceBucketName).get(objectKey).dataStoreName = undefined;
        const testObjectCopyRequest = _createObjectCopyPartRequest(destBucketName, uploadId);
        objectPutCopyPart(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
            assert.ifError(err);
            done();
        });
    });

    it('should return InvalidArgument error given invalid range', done => {
        const headers = { 'x-amz-copy-source-range': 'bad-range-parameter' };
        const req =
            _createObjectCopyPartRequest(destBucketName, uploadId, headers);
        objectPutCopyPart(
            authInfo, req, sourceBucketName, objectKey, undefined, log, err => {
                assert(err.is.InvalidArgument);
                assert.strictEqual(err.description,
                    'The x-amz-copy-source-range value must be of the form ' +
                    'bytes=first-last where first and last are the ' +
                    'zero-based offsets of the first and last bytes to copy');
                done();
            });
    });

    it('should pass overheadField', done => {
        const testObjectCopyRequest = _createObjectCopyPartRequest(destBucketName, uploadId);
        objectPutCopyPart(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
            assert.ifError(err);
            sinon.assert.calledWith(
                metadataswitch.putObjectMD,
                sinon.match.string, // MPU shadow bucket
                objectKey,
                sinon.match.any,
                sinon.match({ overheadField: sinon.match.array }),
                sinon.match.any,
                sinon.match.any
            );
            done();
        });
    });

    it('should set owner-id to the canonicalId of the dest bucket owner', done => {
        const testObjectCopyRequest = _createObjectCopyPartRequest(destBucketName, uploadId);
        objectPutCopyPart(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
            assert.ifError(err);
            sinon.assert.calledWith(
                metadataswitch.putObjectMD.lastCall,
                sinon.match.string, // MPU shadow bucket
                `${uploadId}..|..00001`,
                sinon.match({ 'owner-id': authInfo.canonicalID }),
                sinon.match.any,
                sinon.match.any,
                sinon.match.any
            );
            done();
        });
    });
});

describe('objectCopyPart with a bucket policy scoped to key prefixes', () => {
    const bucketName = 'prefixpolicybucket';
    const destKey = 'allowed/mpu-copy';
    const userAuthInfo = makeAuthInfo(canonicalID, 'user');
    const putPolicyRequest = new DummyRequest({
        bucketName,
        namespace,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
        socket: {},
        post: JSON.stringify({
            Version: '2012-10-17',
            Statement: [
                {
                    Effect: 'Allow',
                    Principal: '*',
                    Action: ['s3:GetObject', 's3:PutObject'],
                    Resource: `arn:aws:s3:::${bucketName}/allowed/*`,
                },
                {
                    Effect: 'Deny',
                    Principal: '*',
                    Action: 's3:GetObject',
                    Resource: `arn:aws:s3:::${bucketName}/forbidden/*`,
                },
            ],
        }),
    });
    const initiateUserRequest = new DummyRequest({
        bucketName,
        namespace,
        objectKey: destKey,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: `/${destKey}?uploads`,
        socket: {},
    });
    let uploadId;

    // An IAM user without identity policy: Vault returns an implicit deny
    // for both the source read and the destination write
    function _createUserCopyPartRequest() {
        const request = new DummyRequest({
            bucketName,
            namespace,
            objectKey: destKey,
            headers: {},
            url: `/${bucketName}/${destKey}?partNumber=1`,
            query: {
                partNumber: 1,
                uploadId,
            },
            socket: {},
        });
        request.actionImplicitDenies = { objectGet: true, objectPut: true };
        return request;
    }

    before(done => {
        cleanup();
        async.waterfall([
            callback => bucketPut(authInfo, _createBucketPutRequest(bucketName), log,
                err => callback(err)),
            callback => bucketPutPolicy(authInfo, putPolicyRequest, log,
                err => callback(err)),
            callback => objectPut(authInfo,
                versioningTestUtils.createPutObjectRequest(bucketName, 'forbidden/myfile', Buffer.from('foo', 'utf8')),
                undefined, log, err => callback(err)),
            callback => objectPut(authInfo,
                versioningTestUtils.createPutObjectRequest(bucketName, 'allowed/myfile', Buffer.from('bar', 'utf8')),
                undefined, log, err => callback(err)),
            callback => initiateMultipartUpload(userAuthInfo, initiateUserRequest,
                log, (err, res) => callback(err, res)),
        ], (err, res) => {
            if (err) {
                return done(err);
            }
            return parseString(res, (err, json) => {
                uploadId = json.InitiateMultipartUploadResult.UploadId[0];
                return done(err);
            });
        });
    });

    after(() => {
        cleanup();
    });

    it('should deny copying a part from a source denied by the bucket policy', done => {
        objectPutCopyPart(userAuthInfo, _createUserCopyPartRequest(), bucketName, 'forbidden/myfile',
            undefined, log, err => {
                assert(err, 'expected the part copy to be denied');
                assert.strictEqual(err.is.AccessDenied, true);
                done();
            });
    });

    it('should allow copying a part from a source allowed by the bucket policy', done => {
        objectPutCopyPart(userAuthInfo, _createUserCopyPartRequest(), bucketName, 'allowed/myfile',
            undefined, log, err => {
                assert.ifError(err);
                done();
            });
    });
});
