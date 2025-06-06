const assert = require('assert');
const async = require('async');
const { storage, versioning } = require('arsenal');
const sinon = require('sinon');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const bucketPutPolicy = require('../../../lib/api/bucketPutPolicy');
const objectPut = require('../../../lib/api/objectPut');
const objectCopy = require('../../../lib/api/objectCopy');
const DummyRequest = require('../DummyRequest');
const { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils }
    = require('../helpers');
const mpuUtils = require('../utils/mpuUtils');
const metadata = require('../metadataswitch');
const { data } = require('../../../lib/data/wrapper');
const { objectLocationConstraintHeader } = require('../../../constants');
const { fakeMetadataArchive } = require('../../functional/aws-node-sdk/test/utils/init');

const any = sinon.match.any;

const { ds } = storage.data.inMemory.datastore;

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

function _createObjectCopyRequest(destBucketName, headers = {}) {
    const params = {
        bucketName: destBucketName,
        namespace,
        objectKey,
        headers,
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

    it('should not copy object with storage-class header not equal to STANDARD', done => {
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        testObjectCopyRequest.headers['x-amz-storage-class'] = 'COLD';
        objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey,
            undefined, log, err => {
                setImmediate(() => {
                    assert.strictEqual(err.is.InvalidStorageClass, true);
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
    const testPutDestObjectRequest = versioningTestUtils
        .createPutObjectRequest(destBucketName, objectKey, objData[1]);

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

    it('should not pass needOplogUpdate when creating object', done => {
        async.series([
            next => objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey,
                undefined, log, next),
            async () => {
                sinon.assert.calledWith(metadata.putObjectMD.lastCall,
                    destBucketName, objectKey, any, sinon.match({
                        needOplogUpdate: undefined,
                        originOp: undefined,
                    }), any, any);
            },
        ], done);
    });

    it('should not pass needOplogUpdate when replacing object', done => {
        async.series([
            next => objectPut(authInfo, testPutDestObjectRequest, undefined, log, next),
            next => objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey,
                undefined, log, next),
            async () => {
                sinon.assert.calledWith(metadata.putObjectMD.lastCall,
                    destBucketName, objectKey, any, sinon.match({
                        needOplogUpdate: undefined,
                        originOp: undefined,
                    }), any, any);
            },
        ], done);
    });

    it('should pass needOplogUpdate to metadata when replacing archived object', done => {
        const archived = {
            archiveInfo: { foo: 0, bar: 'stuff' }
        };

        async.series([
            next => objectPut(authInfo, testPutDestObjectRequest, undefined, log, next),
            next => fakeMetadataArchive(destBucketName, objectKey, undefined, archived, next),
            next => objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey,
                undefined, log, next),
            async () => {
                sinon.assert.calledWith(metadata.putObjectMD.lastCall,
                    destBucketName, objectKey, any, sinon.match({
                        needOplogUpdate: true,
                        originOp: 's3:ReplaceArchivedObject',
                    }), any, any);
            },
        ], done);
    });

    it('should pass needOplogUpdate to metadata when replacing archived object in version suspended bucket', done => {
        const archived = {
            archiveInfo: { foo: 0, bar: 'stuff' }
        };

        async.series([
            next => bucketPutVersioning(authInfo, suspendVersioningRequest, log, next),
            next => objectPut(authInfo, testPutDestObjectRequest, undefined, log, next),
            next => fakeMetadataArchive(destBucketName, objectKey, undefined, archived, next),
            next => objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey,
                undefined, log, next),
            async () => {
                sinon.assert.calledWith(metadata.putObjectMD.lastCall,
                    destBucketName, objectKey, any, sinon.match({
                        needOplogUpdate: true,
                        originOp: 's3:ReplaceArchivedObject',
                    }), any, any);
            },
        ], done);
    });

    it('should fail to copy object when setting a crr location as the locationConstraint', done => {
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName, {
            'x-amz-metadata-directive': 'REPLACE', // needed to take the locationConstraint into account
            [objectLocationConstraintHeader]: 'location-crr-v1',
        });

        async.series([
            next => objectPut(authInfo, testPutDestObjectRequest, undefined, log, next),
            next => objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey,
                undefined, log, next),
        ], err => {
            assert(err.is.InvalidArgument);
            done();
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

describe('objectCopy in ingestion bucket', () => {
    const dataClient = data.client;
    const prevDataImplName = data.implName;
    const prevConfigBackendsData = data.config.backends.data;
    const prevConfigLocationConstraints1Type = data.config.locationConstraints['us-east-1'].type;
    const prevConfigLocationConstraints2Type = data.config.locationConstraints['us-east-2'].type;

    before(() => {
        // Setup multi-backend, this is required for ingestion
        data.switch(new storage.data.MultipleBackendGateway({
            'us-east-1': dataClient,
            'us-east-2': dataClient,
        }, metadata, data.locStorageCheckFn));
        data.implName = 'multipleBackends';

        // "mock" the data location, simulating a backend supporting server-side copy
        data.config.backends.data = 'multiple';
        data.config.locationConstraints['us-east-1'].type = 'aws_s3';
        data.config.locationConstraints['us-east-2'].type = 'aws_s3';
    });

    after(() => {
        data.switch(dataClient);
        data.implName = prevDataImplName;
        data.config.backends.data = prevConfigBackendsData;
        data.config.locationConstraints['us-east-1'].type = prevConfigLocationConstraints1Type;
        data.config.locationConstraints['us-east-2'].type = prevConfigLocationConstraints2Type;
    });

    const versionIDs = [];

    beforeEach(() => {
        cleanup();

        sinon.stub(dataClient, 'put').callsFake((writeStream, size, keyContext, reqUids, cb) => {
            const versionID = versioning.VersionID.encode(versioning.VersionID.generateVersionId('0', ''));
            versionIDs.push(versionID);
            cb(null, `${keyContext.bucketName}/${keyContext.objectKey}`, versionID, size, 'md5');
        });
    });

    afterEach(() => {
        sinon.restore();
    });

    const newPutIngestBucketRequest = location => new DummyRequest({
        bucketName: destBucketName,
        namespace,
        headers: { host: `${destBucketName}.s3.amazonaws.com` },
        url: '/',
        post: '<?xml version="1.0" encoding="UTF-8"?>' +
            '<CreateBucketConfiguration ' +
            'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
            `<LocationConstraint>${location}</LocationConstraint>` +
            '</CreateBucketConfiguration>',
    });
    const putSourceObjectRequest = versioningTestUtils.createPutObjectRequest(
        sourceBucketName, objectKey, objData[0]);
    const newPutObjectRequest = params => {
        const { location } = params || {};
        const r = _createObjectCopyRequest(destBucketName);
        if (location) {
            r.headers[objectLocationConstraintHeader] = location;

            // Need to 'replace' the metadata for the constraint to be taken into account
            r.headers['x-amz-metadata-directive'] = 'REPLACE';
        }
        return r;
    };

    it('should use the versionID from the backend', done => {
        const versionID = versioning.VersionID.encode(versioning.VersionID.generateVersionId('0', ''));
        dataClient.copyObject = sinon.stub().yields(null, objectKey, versionID);

        async.series([
            next => bucketPut(authInfo, putSourceBucketRequest, log, next),
            next => bucketPut(authInfo, newPutIngestBucketRequest('us-east-1:ingest'), log, next),
            next => objectPut(authInfo, putSourceObjectRequest, undefined, log, next),
            next => objectCopy(authInfo, newPutObjectRequest(), sourceBucketName, objectKey, undefined, log,
                (err, xml, headers) => {
                    assert.ifError(err);
                    assert.strictEqual(headers['x-amz-version-id'], versionID);
                    next();
                }),
        ], done);
    });

    it('should not use the versionID from the backend when writing in another location', done => {
        const versionID = versioning.VersionID.encode(versioning.VersionID.generateVersionId('0', ''));
        dataClient.copyObject = sinon.stub().yields(null, objectKey, versionID);

        const copyObjectRequest = newPutObjectRequest({ location: 'us-east-2' });
        async.series([
            next => bucketPut(authInfo, putSourceBucketRequest, log, next),
            next => bucketPut(authInfo, newPutIngestBucketRequest('us-east-1:ingest'), log, next),
            next => objectPut(authInfo, putSourceObjectRequest, undefined, log, next),
            next => objectCopy(authInfo, copyObjectRequest, sourceBucketName, objectKey, undefined, log,
                (err, xml, headers) => {
                    assert.ifError(err);
                    assert.notEqual(headers['x-amz-version-id'], versionID);
                    next();
                }),
        ], done);
    });

    it('should not use the versionID from the backend when it is not a valid versionID', done => {
        const versionID = undefined;
        dataClient.copyObject = sinon.stub().yields(null, objectKey, versionID);

        async.series([
            next => bucketPut(authInfo, putSourceBucketRequest, log, next),
            next => bucketPut(authInfo, newPutIngestBucketRequest('us-east-1:ingest'), log, next),
            next => objectPut(authInfo, putSourceObjectRequest, undefined, log, next),
            next => objectCopy(authInfo, newPutObjectRequest(), sourceBucketName, objectKey, undefined, log,
                (err, xml, headers) => {
                    assert.ifError(err);
                    assert.notEqual(headers['x-amz-version-id'], versionID);
                    next();
                }),
        ], done);
    });
});
