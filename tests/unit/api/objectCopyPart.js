const assert = require('assert');
const async = require('async');
const { parseString } = require('xml2js');
const { errors, storage } = require('arsenal');
const constants = require('../../../constants');
const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');
const objectPutCopyPart = require('../../../lib/api/objectPutCopyPart');
const initiateMultipartUpload
= require('../../../lib/api/initiateMultipartUpload');
const { metadata } = storage.metadata.inMemory.metadata;
const { ds } = storage.data.inMemory.datastore;
const { metastore } = storage.metadata.inMemory;
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
    const objData = Buffer.from('foo', 'utf8');
    let uploadId;

    beforeEach(done => {
        cleanup();
        const testPutObjectRequest = versioningTestUtils
            .createPutObjectRequest(sourceBucketName, objectKey, objData);
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
                if (err) {
                    return done(err);
                }
                uploadId = json.InitiateMultipartUploadResult.UploadId[0];
                return done();
            });
        });
    });

    afterEach(() => cleanup());

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

    describe('getObjectMD error condition', () => {
        function errorFunc(method, objName) {
            if (method !== metastore.getObject.name) {
                return null;
            }
            if (objName !== `${uploadId}${constants.splitter}00001`) {
                return null;
            }
            return errors.InternalError;
        }

        beforeEach(done => {
            assert.deepStrictEqual(ds[1].value, objData);
            const req = _createObjectCopyPartRequest(destBucketName, uploadId);
            metastore.setErrorFunc(errorFunc);
            objectPutCopyPart(
                authInfo, req, sourceBucketName, objectKey, null, log, err => {
                    assert.deepStrictEqual(err, errors.InternalError);
                    done();
                });
        });

        afterEach(() => metastore.clearErrorFunc());

        it('should delete the destination data', () => {
            assert.strictEqual(ds.length, 3);
            assert.strictEqual(ds[0], undefined);
            assert.deepStrictEqual(ds[1].value, objData); // The source data.
            assert.strictEqual(ds[2], undefined); // The destination data.
        });
    });

    describe('putObjectMD error condition', () => {
        function errorFunc(method) {
            if (method === metastore.putObject.name) {
                return errors.InternalError;
            }
            return null;
        }

        beforeEach(done => {
            assert.deepStrictEqual(ds[1].value, objData);
            const req = _createObjectCopyPartRequest(destBucketName, uploadId);
            metastore.setErrorFunc(errorFunc);
            objectPutCopyPart(
                authInfo, req, sourceBucketName, objectKey, null, log, err => {
                    assert.deepStrictEqual(err, errors.InternalError);
                    done();
                });
        });

        afterEach(() => metastore.clearErrorFunc());

        it('should delete the destination data', () => {
            assert.strictEqual(ds.length, 3);
            assert.strictEqual(ds[0], undefined);
            assert.deepStrictEqual(ds[1].value, objData); // The source data.
            assert.strictEqual(ds[2], undefined); // The destination data.
        });
    });
});
