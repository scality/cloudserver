const assert = require('assert');
const async = require('async');
const sinon = require('sinon');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const bucketPutPolicy = require('../../../lib/api/bucketPutPolicy');
const objectPut = require('../../../lib/api/objectPut');
const objectCopy = require('../../../lib/api/objectCopy');
const { ds } = require('arsenal').storage.data.inMemory.datastore;
const DummyRequest = require('../DummyRequest');
const { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils }
    = require('../helpers');
const mpuUtils = require('../utils/mpuUtils');
const metadata = require('../metadataswitch');

const any = sinon.match.any;

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const destBucketName = 'destbucketname';
const sourceBucketName = 'sourcebucketname';
const objectKey = 'objectName';
const originalputObjectMD = metadata.putObjectMD;

function _createBucketPutRequest(bucketName) {
    return new DummyRequest({
        bucketName,
        namespace,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
    });
}

function _createObjectCopyRequest(destBucketName) {
    const params = {
        bucketName: destBucketName,
        namespace,
        objectKey,
        headers: {},
        url: `/${destBucketName}/${objectKey}`,
        socket: {},
    };
    return new DummyRequest(params);
}

const putDestBucketRequest = _createBucketPutRequest(destBucketName);
const putSourceBucketRequest = _createBucketPutRequest(sourceBucketName);
const enableVersioningRequest = versioningTestUtils
    .createBucketPutVersioningReq(destBucketName, 'Enabled');
const suspendVersioningRequest = versioningTestUtils
    .createBucketPutVersioningReq(destBucketName, 'Suspended');
const objData = ['foo0', 'foo1', 'foo2'].map(str =>
    Buffer.from(str, 'utf8'));


describe('objectCopy with versioning', () => {
    const testPutObjectRequests = objData.slice(0, 2).map(data =>
        versioningTestUtils.createPutObjectRequest(destBucketName, objectKey,
            data));
    testPutObjectRequests.push(versioningTestUtils
        .createPutObjectRequest(sourceBucketName, objectKey, objData[2]));

    before(done => {
        cleanup();
        sinon.spy(metadata, 'putObjectMD');
        async.series([
            callback => bucketPut(authInfo, putDestBucketRequest, log,
                callback),
            callback => bucketPut(authInfo, putSourceBucketRequest, log,
                callback),
            // putting null version: put obj before versioning configured
            // in dest bucket
            callback => objectPut(authInfo, testPutObjectRequests[0],
                undefined, log, callback),
            callback => bucketPutVersioning(authInfo,
                enableVersioningRequest, log, callback),
            // put another version in dest bucket:
            callback => objectPut(authInfo, testPutObjectRequests[1],
                undefined, log, callback),
            callback => bucketPutVersioning(authInfo,
                suspendVersioningRequest, log, callback),
            // put source object in source bucket
            callback => objectPut(authInfo, testPutObjectRequests[2],
                undefined, log, callback),
        ], err => {
            if (err) {
                return done(err);
            }
            versioningTestUtils.assertDataStoreValues(ds, objData);
            return done();
        });
    });

    after(() => {
        metadata.putObjectMD.restore();
        cleanup();
    });

    it('should delete null version when creating new null version, ' +
    'even when null version is not the latest version', done => {
        // will have another copy of last object in datastore after objectCopy
        const expectedValues = [undefined, objData[1], objData[2], objData[2]];
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey,
            undefined, log, err => {
                assert.ifError(err, `Unexpected err: ${err}`);
                setImmediate(() => {
                    versioningTestUtils
                        .assertDataStoreValues(ds, expectedValues);
                    done();
                });
            });
    });

    it('should not set bucketOwnerId if requesting account owns dest bucket', done => {
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey,
            undefined, log, err => {
                assert.ifError(err);
                sinon.assert.calledWith(
                    metadata.putObjectMD.lastCall,
                    destBucketName,
                    objectKey,
                    sinon.match({ _data: { bucketOwnerId: sinon.match.typeOf('undefined') } }),
                    sinon.match.any,
                    sinon.match.any,
                    sinon.match.any
                );
                done();
            });
    });

    // TODO: S3C-9965
    // Skipped because the policy is not checked correctly
    // When source bucket policy is checked destination arn is used
    it.skip('should set bucketOwnerId if requesting account differs from dest bucket owner', done => {
        const authInfo2 = makeAuthInfo('accessKey2');
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        const testPutSrcPolicyRequest = new DummyRequest({
            bucketName: sourceBucketName,
            namespace,
            headers: { host: `${sourceBucketName}.s3.amazonaws.com` },
            url: '/',
            socket: {},
            post: JSON.stringify({
                Version: '2012-10-17',
                Statement: [
                    {
                        Sid: 'AllowCrossAccountRead',
                        Effect: 'Allow',
                        Principal: { AWS: `arn:aws:iam::${authInfo2.shortid}:root` },
                        Action: ['s3:GetObject'],
                        Resource: [
                            `arn:aws:s3:::${sourceBucketName}/*`,
                        ],
                    },
                ],
            }),
        });
        const testPutDestPolicyRequest = new DummyRequest({
            bucketName: destBucketName,
            namespace,
            headers: { host: `${destBucketName}.s3.amazonaws.com` },
            url: '/',
            socket: {},
            post: JSON.stringify({
                Version: '2012-10-17',
                Statement: [
                    {
                        Sid: 'AllowCrossAccountWrite',
                        Effect: 'Allow',
                        Principal: { AWS: `arn:aws:iam::${authInfo2.shortid}:root` },
                        Action: ['s3:PutObject'],
                        Resource: [
                            `arn:aws:s3:::${destBucketName}/*`,
                        ],
                    },
                ],
            }),
        });
        bucketPutPolicy(authInfo, testPutSrcPolicyRequest, log, err => {
            assert.ifError(err);
            bucketPutPolicy(authInfo, testPutDestPolicyRequest, log, err => {
                assert.ifError(err);
                objectCopy(authInfo2, testObjectCopyRequest, sourceBucketName, objectKey,
                    undefined, log, err => {
                        sinon.assert.calledWith(
                            metadata.putObjectMD.lastCall,
                            destBucketName,
                            objectKey,
                            sinon.match({ _data: { bucketOwnerId: authInfo.canonicalID } }),
                            sinon.match.any,
                            sinon.match.any,
                            sinon.match.any
                        );
                        assert.ifError(err);
                        done();
                    });
            });
        });
    });
});

describe('non-versioned objectCopy', () => {
    const testPutObjectRequest = versioningTestUtils
        .createPutObjectRequest(sourceBucketName, objectKey, objData[0]);

    before(done => {
        cleanup();
        sinon.stub(metadata, 'putObjectMD')
            .callsFake(originalputObjectMD);
        async.series([
            callback => bucketPut(authInfo, putDestBucketRequest, log,
                callback),
            callback => bucketPut(authInfo, putSourceBucketRequest, log,
                callback),
            // put source object in source bucket
            callback => objectPut(authInfo, testPutObjectRequest,
                undefined, log, callback),
        ], err => {
            if (err) {
                return done(err);
            }
            versioningTestUtils.assertDataStoreValues(ds, objData.slice(0, 1));
            return done();
        });
    });

    after(() => {
        cleanup();
        sinon.restore();
    });

    const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);

    it('should not leave orphans in data when overwriting a multipart upload', done => {
        mpuUtils.createMPU(namespace, destBucketName, objectKey, log,
        (err, testUploadId) => {
            assert.ifError(err);
            objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey,
                undefined, log, err => {
                    assert.ifError(err);
                    sinon.assert.calledWith(metadata.putObjectMD,
                        any, any, any, sinon.match({ oldReplayId: testUploadId }), any, any);
                    done();
                });
        });
    });
});

describe('objectCopy overheadField', () => {
    beforeEach(done => {
        cleanup();
        sinon.stub(metadata, 'putObjectMD').callsFake(originalputObjectMD);
        async.series([
            next => bucketPut(authInfo, putSourceBucketRequest, log, next),
            next => bucketPut(authInfo, putDestBucketRequest, log, next),
        ], done);
    });

    afterEach(() => {
        sinon.restore();
        cleanup();
    });

    it('should pass overheadField to metadata.putObjectMD for a non-versioned request', done => {
        const testPutObjectRequest =
            versioningTestUtils.createPutObjectRequest(sourceBucketName, objectKey, objData[0]);
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
            assert.ifError(err);
            objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log,
                err => {
                    assert.ifError(err);
                    sinon.assert.calledWith(metadata.putObjectMD.lastCall,
                        destBucketName, objectKey, any, sinon.match({ overheadField: sinon.match.array }), any, any);
                    done();
                }
            );
        });
    });

    it('should pass overheadField to metadata.putObjectMD for a versioned request', done => {
        const testPutObjectRequest =
            versioningTestUtils.createPutObjectRequest(sourceBucketName, objectKey, objData[0]);
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
            assert.ifError(err);
            bucketPutVersioning(authInfo, enableVersioningRequest, log, err => {
                assert.ifError(err);
                objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log,
                    err => {
                        assert.ifError(err);
                        sinon.assert.calledWith(metadata.putObjectMD.lastCall,
                            destBucketName, objectKey, any,
                            sinon.match({ overheadField: sinon.match.array }), any, any
                        );
                        done();
                    }
                );
            });
        });
    });

    it('should pass overheadField to metadata.putObjectMD for a version-suspended request', done => {
        const testPutObjectRequest =
            versioningTestUtils.createPutObjectRequest(sourceBucketName, objectKey, objData[0]);
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
            assert.ifError(err);
            bucketPutVersioning(authInfo, suspendVersioningRequest, log, err => {
                assert.ifError(err);
                objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log,
                    err => {
                        assert.ifError(err);
                        sinon.assert.calledWith(metadata.putObjectMD.lastCall,
                            destBucketName, objectKey, any,
                            sinon.match({ overheadField: sinon.match.array }), any, any
                        );
                        done();
                    }
                );
            });
        });
    });
});
