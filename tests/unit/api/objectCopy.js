const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const { storage, versioning } = require('arsenal');
const sinon = require('sinon');
const { parseString } = require('xml2js');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const bucketPutPolicy = require('../../../lib/api/bucketPutPolicy');
const completeMultipartUpload = require('../../../lib/api/completeMultipartUpload');
const initiateMultipartUpload = require('../../../lib/api/initiateMultipartUpload');
const objectPut = require('../../../lib/api/objectPut');
const objectPutPart = require('../../../lib/api/objectPutPart');
const objectPutCopyPart = require('../../../lib/api/objectPutCopyPart');
const objectCopy = require('../../../lib/api/objectCopy');
const objectGet = require('../../../lib/api/objectGet');
const kms = require('../../../lib/kms/wrapper');
const DummyRequest = require('../DummyRequest');
const { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils } = require('../helpers');
const mpuUtils = require('../utils/mpuUtils');
const metadata = require('../metadataswitch');
const { data } = require('../../../lib/data/wrapper');
const constants = require('../../../constants');
const { objectLocationConstraintHeader } = constants;
const { fakeMetadataArchive } = require('../../functional/aws-node-sdk/test/utils/init');
const { config } = require('../../../lib/Config');

const { LOCATION_NAME_CRR } = require('../../constants');

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
const enableVersioningRequest = versioningTestUtils.createBucketPutVersioningReq(destBucketName, 'Enabled');
const suspendVersioningRequest = versioningTestUtils.createBucketPutVersioningReq(destBucketName, 'Suspended');
const objData = ['foo0', 'foo1', 'foo2'].map(str => Buffer.from(str, 'utf8'));

describe('objectCopy with versioning', () => {
    const testPutObjectRequests = objData
        .slice(0, 2)
        .map(data => versioningTestUtils.createPutObjectRequest(destBucketName, objectKey, data));
    testPutObjectRequests.push(versioningTestUtils.createPutObjectRequest(sourceBucketName, objectKey, objData[2]));

    before(done => {
        cleanup();
        sinon.spy(metadata, 'putObjectMD');
        async.series(
            [
                callback => bucketPut(authInfo, putDestBucketRequest, log, callback),
                callback => bucketPut(authInfo, putSourceBucketRequest, log, callback),
                // putting null version: put obj before versioning configured
                // in dest bucket
                callback => objectPut(authInfo, testPutObjectRequests[0], undefined, log, callback),
                callback => bucketPutVersioning(authInfo, enableVersioningRequest, log, callback),
                // put another version in dest bucket:
                callback => objectPut(authInfo, testPutObjectRequests[1], undefined, log, callback),
                callback => bucketPutVersioning(authInfo, suspendVersioningRequest, log, callback),
                // put source object in source bucket
                callback => objectPut(authInfo, testPutObjectRequests[2], undefined, log, callback),
            ],
            err => {
                if (err) {
                    return done(err);
                }
                versioningTestUtils.assertDataStoreValues(ds, objData);
                return done();
            },
        );
    });

    after(() => {
        metadata.putObjectMD.restore();
        cleanup();
    });

    it(
        'should delete null version when creating new null version, ' +
            'even when null version is not the latest version',
        done => {
            // will have another copy of last object in datastore after objectCopy
            const expectedValues = [undefined, objData[1], objData[2], objData[2]];
            const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
            objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
                assert.ifError(err, `Unexpected err: ${err}`);
                sinon.assert.calledWith(
                    metadata.putObjectMD.lastCall,
                    destBucketName,
                    objectKey,
                    sinon.match({ _data: { originOp: 's3:ObjectCreated:Copy' } }),
                    sinon.match.any,
                    sinon.match.any,
                    sinon.match.any,
                );
                setImmediate(() => {
                    versioningTestUtils.assertDataStoreValues(ds, expectedValues);
                    done();
                });
            });
        },
    );

    it('should not copy object with storage-class header not equal to STANDARD', done => {
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        testObjectCopyRequest.headers['x-amz-storage-class'] = 'COLD';
        objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
            setImmediate(() => {
                assert.strictEqual(err.is.InvalidStorageClass, true);
                done();
            });
        });
    });

    it('should not set bucketOwnerId if requesting account owns dest bucket', done => {
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
            assert.ifError(err);
            sinon.assert.calledWith(
                metadata.putObjectMD.lastCall,
                destBucketName,
                objectKey,
                sinon.match({ _data: { bucketOwnerId: sinon.match.typeOf('undefined') } }),
                sinon.match.any,
                sinon.match.any,
                sinon.match.any,
            );
            done();
        });
    });

    it('should set bucketOwnerId if requesting account differs from dest bucket owner', done => {
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
                        Resource: [`arn:aws:s3:::${sourceBucketName}/*`],
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
                        Resource: [`arn:aws:s3:::${destBucketName}/*`],
                    },
                ],
            }),
        });
        bucketPutPolicy(authInfo, testPutSrcPolicyRequest, log, err => {
            assert.ifError(err);
            bucketPutPolicy(authInfo, testPutDestPolicyRequest, log, err => {
                assert.ifError(err);
                objectCopy(authInfo2, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
                    sinon.assert.calledWith(
                        metadata.putObjectMD.lastCall,
                        destBucketName,
                        objectKey,
                        sinon.match({ _data: { bucketOwnerId: authInfo.canonicalID } }),
                        sinon.match.any,
                        sinon.match.any,
                        sinon.match.any,
                    );
                    assert.ifError(err);
                    done();
                });
            });
        });
    });
});

describe('non-versioned objectCopy', () => {
    const testPutObjectRequest = versioningTestUtils.createPutObjectRequest(sourceBucketName, objectKey, objData[0]);
    const testPutDestObjectRequest = versioningTestUtils.createPutObjectRequest(destBucketName, objectKey, objData[1]);

    before(done => {
        cleanup();
        sinon.stub(metadata, 'putObjectMD').callsFake(originalputObjectMD);

        async.series(
            [
                callback => bucketPut(authInfo, putDestBucketRequest, log, callback),
                callback => bucketPut(authInfo, putSourceBucketRequest, log, callback),
                // put source object in source bucket
                callback => objectPut(authInfo, testPutObjectRequest, undefined, log, callback),
            ],
            err => {
                if (err) {
                    return done(err);
                }
                versioningTestUtils.assertDataStoreValues(ds, objData.slice(0, 1));
                return done();
            },
        );
    });

    after(() => {
        cleanup();
        sinon.restore();
    });

    const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);

    it('should not leave orphans in data when overwriting a multipart upload', done => {
        mpuUtils.createMPU(namespace, destBucketName, objectKey, log, (err, testUploadId) => {
            assert.ifError(err);
            objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
                assert.ifError(err);
                sinon.assert.calledWith(
                    metadata.putObjectMD,
                    any,
                    any,
                    any,
                    sinon.match({ oldReplayId: testUploadId }),
                    any,
                    any,
                );
                done();
            });
        });
    });

    it('should not pass needOplogUpdate when creating object', done => {
        async.series(
            [
                next => objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, next),
                async () => {
                    sinon.assert.calledWith(
                        metadata.putObjectMD.lastCall,
                        destBucketName,
                        objectKey,
                        sinon.match({
                            _data: { originOp: 's3:ObjectCreated:Copy' },
                        }),
                        sinon.match({
                            needOplogUpdate: undefined,
                            originOp: undefined,
                        }),
                        any,
                        any,
                    );
                },
            ],
            done,
        );
    });

    it('should not pass needOplogUpdate when replacing object', done => {
        async.series(
            [
                next => objectPut(authInfo, testPutDestObjectRequest, undefined, log, next),
                next => objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, next),
                async () => {
                    sinon.assert.calledWith(
                        metadata.putObjectMD.lastCall,
                        destBucketName,
                        objectKey,
                        sinon.match({
                            _data: { originOp: 's3:ObjectCreated:Copy' },
                        }),
                        sinon.match({
                            needOplogUpdate: undefined,
                            originOp: undefined,
                        }),
                        any,
                        any,
                    );
                },
            ],
            done,
        );
    });

    it('should pass needOplogUpdate to metadata when replacing archived object', done => {
        const archived = {
            archiveInfo: { foo: 0, bar: 'stuff' },
        };

        async.series(
            [
                next => objectPut(authInfo, testPutDestObjectRequest, undefined, log, next),
                next => fakeMetadataArchive(destBucketName, objectKey, undefined, archived, next),
                next => objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, next),
                async () => {
                    sinon.assert.calledWith(
                        metadata.putObjectMD.lastCall,
                        destBucketName,
                        objectKey,
                        any,
                        sinon.match({
                            needOplogUpdate: true,
                            originOp: 's3:ReplaceArchivedObject',
                        }),
                        any,
                        any,
                    );
                },
            ],
            done,
        );
    });

    it('should pass needOplogUpdate to metadata when replacing archived object in version suspended bucket', done => {
        const archived = {
            archiveInfo: { foo: 0, bar: 'stuff' },
        };

        async.series(
            [
                next => bucketPutVersioning(authInfo, suspendVersioningRequest, log, next),
                next => objectPut(authInfo, testPutDestObjectRequest, undefined, log, next),
                next => fakeMetadataArchive(destBucketName, objectKey, undefined, archived, next),
                next => objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, next),
                async () => {
                    sinon.assert.calledWith(
                        metadata.putObjectMD.lastCall,
                        destBucketName,
                        objectKey,
                        any,
                        sinon.match({
                            needOplogUpdate: true,
                            originOp: 's3:ReplaceArchivedObject',
                        }),
                        any,
                        any,
                    );
                },
            ],
            done,
        );
    });

    it('should fail to copy object when setting a crr location as the locationConstraint', done => {
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName, {
            'x-amz-metadata-directive': 'REPLACE', // needed to take the locationConstraint into account
            [objectLocationConstraintHeader]: LOCATION_NAME_CRR,
        });

        async.series(
            [
                next => objectPut(authInfo, testPutDestObjectRequest, undefined, log, next),
                next => objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, next),
            ],
            err => {
                assert(err.is.InvalidArgument);
                done();
            },
        );
    });
});

describe('objectCopy overheadField', () => {
    beforeEach(done => {
        cleanup();
        sinon.stub(metadata, 'putObjectMD').callsFake(originalputObjectMD);
        async.series(
            [
                next => bucketPut(authInfo, putSourceBucketRequest, log, next),
                next => bucketPut(authInfo, putDestBucketRequest, log, next),
            ],
            done,
        );
    });

    afterEach(() => {
        sinon.restore();
        cleanup();
    });

    it('should pass overheadField to metadata.putObjectMD for a non-versioned request', done => {
        const testPutObjectRequest = versioningTestUtils.createPutObjectRequest(
            sourceBucketName,
            objectKey,
            objData[0],
        );
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
            assert.ifError(err);
            objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
                assert.ifError(err);
                sinon.assert.calledWith(
                    metadata.putObjectMD.lastCall,
                    destBucketName,
                    objectKey,
                    any,
                    sinon.match({ overheadField: sinon.match.array }),
                    any,
                    any,
                );
                done();
            });
        });
    });

    it('should pass overheadField to metadata.putObjectMD for a versioned request', done => {
        const testPutObjectRequest = versioningTestUtils.createPutObjectRequest(
            sourceBucketName,
            objectKey,
            objData[0],
        );
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
            assert.ifError(err);
            bucketPutVersioning(authInfo, enableVersioningRequest, log, err => {
                assert.ifError(err);
                objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
                    assert.ifError(err);
                    sinon.assert.calledWith(
                        metadata.putObjectMD.lastCall,
                        destBucketName,
                        objectKey,
                        any,
                        sinon.match({ overheadField: sinon.match.array }),
                        any,
                        any,
                    );
                    done();
                });
            });
        });
    });

    it('should pass overheadField to metadata.putObjectMD for a version-suspended request', done => {
        const testPutObjectRequest = versioningTestUtils.createPutObjectRequest(
            sourceBucketName,
            objectKey,
            objData[0],
        );
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
            assert.ifError(err);
            bucketPutVersioning(authInfo, suspendVersioningRequest, log, err => {
                assert.ifError(err);
                objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
                    assert.ifError(err);
                    sinon.assert.calledWith(
                        metadata.putObjectMD.lastCall,
                        destBucketName,
                        objectKey,
                        any,
                        sinon.match({ overheadField: sinon.match.array }),
                        any,
                        any,
                    );
                    done();
                });
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
        data.switch(
            new storage.data.MultipleBackendGateway(
                {
                    'us-east-1': dataClient,
                    'us-east-2': dataClient,
                },
                metadata,
                data.locStorageCheckFn,
            ),
        );
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

    const newPutIngestBucketRequest = location =>
        new DummyRequest({
            bucketName: destBucketName,
            namespace,
            headers: { host: `${destBucketName}.s3.amazonaws.com` },
            url: '/',
            post:
                '<?xml version="1.0" encoding="UTF-8"?>' +
                '<CreateBucketConfiguration ' +
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
                `<LocationConstraint>${location}</LocationConstraint>` +
                '</CreateBucketConfiguration>',
        });
    const putSourceObjectRequest = versioningTestUtils.createPutObjectRequest(sourceBucketName, objectKey, objData[0]);
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

        async.series(
            [
                next => bucketPut(authInfo, putSourceBucketRequest, log, next),
                next => bucketPut(authInfo, newPutIngestBucketRequest('us-east-1:ingest'), log, next),
                next => objectPut(authInfo, putSourceObjectRequest, undefined, log, next),
                next =>
                    objectCopy(
                        authInfo,
                        newPutObjectRequest(),
                        sourceBucketName,
                        objectKey,
                        undefined,
                        log,
                        (err, xml, headers) => {
                            assert.ifError(err);
                            assert.strictEqual(headers['x-amz-version-id'], versionID);
                            next();
                        },
                    ),
            ],
            done,
        );
    });

    it('should not use the versionID from the backend when writing in another location', done => {
        const versionID = versioning.VersionID.encode(versioning.VersionID.generateVersionId('0', ''));
        dataClient.copyObject = sinon.stub().yields(null, objectKey, versionID);

        const copyObjectRequest = newPutObjectRequest({ location: 'us-east-2' });
        async.series(
            [
                next => bucketPut(authInfo, putSourceBucketRequest, log, next),
                next => bucketPut(authInfo, newPutIngestBucketRequest('us-east-1:ingest'), log, next),
                next => objectPut(authInfo, putSourceObjectRequest, undefined, log, next),
                next =>
                    objectCopy(
                        authInfo,
                        copyObjectRequest,
                        sourceBucketName,
                        objectKey,
                        undefined,
                        log,
                        (err, xml, headers) => {
                            assert.ifError(err);
                            assert.notEqual(headers['x-amz-version-id'], versionID);
                            next();
                        },
                    ),
            ],
            done,
        );
    });

    it('should not use the versionID from the backend when it is not a valid versionID', done => {
        const versionID = undefined;
        dataClient.copyObject = sinon.stub().yields(null, objectKey, versionID);

        async.series(
            [
                next => bucketPut(authInfo, putSourceBucketRequest, log, next),
                next => bucketPut(authInfo, newPutIngestBucketRequest('us-east-1:ingest'), log, next),
                next => objectPut(authInfo, putSourceObjectRequest, undefined, log, next),
                next =>
                    objectCopy(
                        authInfo,
                        newPutObjectRequest(),
                        sourceBucketName,
                        objectKey,
                        undefined,
                        log,
                        (err, xml, headers) => {
                            assert.ifError(err);
                            assert.notEqual(headers['x-amz-version-id'], versionID);
                            next();
                        },
                    ),
            ],
            done,
        );
    });
});

describe('objectCopy with objectKeyByteLimit', () => {
    const originalObjectKeyByteLimit = config.objectKeyByteLimit;

    beforeEach(done => {
        cleanup();
        async.series(
            [
                next => bucketPut(authInfo, putDestBucketRequest, log, next),
                next => bucketPut(authInfo, putSourceBucketRequest, log, next),
                next =>
                    objectPut(
                        authInfo,
                        versioningTestUtils.createPutObjectRequest(sourceBucketName, objectKey, objData[0]),
                        undefined,
                        log,
                        next,
                    ),
            ],
            done,
        );
    });

    afterEach(() => {
        config.objectKeyByteLimit = originalObjectKeyByteLimit;
    });

    it('should reject destination object key longer than 915 bytes by default', done => {
        const longDestKey = 'a'.repeat(916);
        const testCopyObjectRequest = _createObjectCopyRequest(destBucketName);
        testCopyObjectRequest.objectKey = longDestKey;
        testCopyObjectRequest.url = `/${destBucketName}/${longDestKey}`;

        objectCopy(authInfo, testCopyObjectRequest, sourceBucketName, objectKey, undefined, log, err => {
            assert(err);
            assert.strictEqual(err.KeyTooLong, true);
            assert.match(err.description, /915/);
            done();
        });
    });

    it('should accept destination object key longer than 915 bytes with objectKeyByteLimit', done => {
        config.objectKeyByteLimit = 1024;

        const longDestKey = 'a'.repeat(1024);
        const testCopyObjectRequest = _createObjectCopyRequest(destBucketName);
        testCopyObjectRequest.objectKey = longDestKey;
        testCopyObjectRequest.url = `/${destBucketName}/${longDestKey}`;

        objectCopy(authInfo, testCopyObjectRequest, sourceBucketName, objectKey, undefined, log, (err, xml) => {
            assert.ifError(err);
            assert(xml);
            done();
        });
    });

    it('should reject destination object key exceeding objectKeyByteLimit', done => {
        config.objectKeyByteLimit = 1024;

        const longDestKey = 'a'.repeat(1025);
        const testCopyObjectRequest = _createObjectCopyRequest(destBucketName);
        testCopyObjectRequest.objectKey = longDestKey;
        testCopyObjectRequest.url = `/${destBucketName}/${longDestKey}`;

        objectCopy(authInfo, testCopyObjectRequest, sourceBucketName, objectKey, undefined, log, err => {
            assert(err);
            assert.strictEqual(err.KeyTooLong, true);
            assert.match(err.description, /1024/);
            done();
        });
    });
});

describe('objectCopy source size limit', () => {
    const testPutObjectRequest = versioningTestUtils.createPutObjectRequest(sourceBucketName, objectKey, objData[0]);
    const sourceSize = objData[0].length;
    let originalMaximumUploadSize;

    before(done => {
        cleanup();
        originalMaximumUploadSize = constants.maximumAllowedUploadSize;
        async.series(
            [
                callback => bucketPut(authInfo, putDestBucketRequest, log, callback),
                callback => bucketPut(authInfo, putSourceBucketRequest, log, callback),
                callback => objectPut(authInfo, testPutObjectRequest, undefined, log, callback),
            ],
            done,
        );
    });

    after(() => {
        constants.maximumAllowedUploadSize = originalMaximumUploadSize;
        config.bypassMaxPutObjectSize = false;
        cleanup();
    });

    it('should allow CopyObject when source size equals the limit', done => {
        constants.maximumAllowedUploadSize = sourceSize;
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
            assert.ifError(err);
            done();
        });
    });

    it('should reject CopyObject when source size exceeds the limit', done => {
        constants.maximumAllowedUploadSize = sourceSize - 1;
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
            assert(err);
            assert.strictEqual(err.is.InvalidRequest, true);
            assert.match(
                err.description,
                /The specified copy source is larger than the maximum allowable size for a copy source/,
            );
            done();
        });
    });

    it('should allow CopyObject when source size exceeds the limit but bypass flag is set', done => {
        constants.maximumAllowedUploadSize = sourceSize - 1;
        config.bypassMaxPutObjectSize = true;
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey, undefined, log, err => {
            assert.ifError(err);
            done();
        });
    });
});

describe('objectCopy with a bucket policy scoped to key prefixes', () => {
    const bucketName = 'prefixpolicybucket';
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

    // An IAM user, with the given implicit denies returned by Vault
    function _createUserCopyRequest(destKey, actionImplicitDenies) {
        const request = new DummyRequest({
            bucketName,
            namespace,
            objectKey: destKey,
            headers: {},
            url: `/${bucketName}/${destKey}`,
            socket: {},
        });
        request.actionImplicitDenies = actionImplicitDenies;
        return request;
    }

    before(done => {
        cleanup();
        async.series(
            [
                callback => bucketPut(authInfo, _createBucketPutRequest(bucketName), log, callback),
                callback => bucketPutPolicy(authInfo, putPolicyRequest, log, callback),
                callback =>
                    objectPut(
                        authInfo,
                        versioningTestUtils.createPutObjectRequest(bucketName, 'forbidden/myfile', objData[0]),
                        undefined,
                        log,
                        callback,
                    ),
                callback =>
                    objectPut(
                        authInfo,
                        versioningTestUtils.createPutObjectRequest(bucketName, 'allowed/myfile', objData[1]),
                        undefined,
                        log,
                        callback,
                    ),
            ],
            done,
        );
    });

    after(() => {
        cleanup();
    });

    // IAM allows reading the source, so the bucket policy Deny is what denies it
    it('should deny copying a source denied by the bucket policy to an allowed destination', done => {
        const request = _createUserCopyRequest('allowed/myfile-copy', { objectGet: false, objectPut: true });
        objectCopy(userAuthInfo, request, bucketName, 'forbidden/myfile', undefined, log, err => {
            assert(err, 'expected the copy to be denied');
            assert.strictEqual(err.is.AccessDenied, true);
            done();
        });
    });

    // IAM allows nothing, so the bucket policy Allow is what allows it
    it('should allow copying a source allowed by the bucket policy to an allowed destination', done => {
        const request = _createUserCopyRequest('allowed/myfile-copy', { objectGet: true, objectPut: true });
        objectCopy(userAuthInfo, request, bucketName, 'allowed/myfile', undefined, log, err => {
            assert.ifError(err);
            done();
        });
    });
});

describe('objectCopy with a bucket policy on the source version ID', () => {
    const bucketName = 'versionidpolicybucket';
    const sourceKey = 'doc';
    const userAuthInfo = makeAuthInfo(canonicalID, 'user');
    const enableRequest = versioningTestUtils.createBucketPutVersioningReq(bucketName, 'Enabled');
    // The version IDs as the client sees them
    const versionIds = [];

    // An IAM user without identity policy: Vault returns an implicit deny
    // for both the source read and the destination write
    function _createUserCopyRequest() {
        const request = new DummyRequest({
            bucketName,
            namespace,
            objectKey: 'doc-copy',
            headers: {},
            url: `/${bucketName}/doc-copy`,
            query: {},
            socket: {},
        });
        request.actionImplicitDenies = { objectGet: true, objectGetVersion: true, objectPut: true };
        return request;
    }

    function _putObject(body, callback) {
        objectPut(
            authInfo,
            versioningTestUtils.createPutObjectRequest(bucketName, sourceKey, body),
            undefined,
            log,
            (err, resHeaders) => {
                if (err) {
                    return callback(err);
                }
                versionIds.push(resHeaders['x-amz-version-id']);
                return callback();
            },
        );
    }

    // PutBucketPolicy does not accept s3:VersionId as a condition key, but a
    // stored policy can still carry it, so it is written directly
    function _setBucketPolicy(callback) {
        const bucketArn = `arn:aws:s3:::${bucketName}`;
        const policy = {
            Version: '2012-10-17',
            Statement: [
                {
                    Effect: 'Allow',
                    Principal: '*',
                    Action: ['s3:GetObject', 's3:GetObjectVersion', 's3:PutObject'],
                    Resource: `${bucketArn}/*`,
                },
                {
                    Effect: 'Deny',
                    Principal: '*',
                    Action: ['s3:GetObject', 's3:GetObjectVersion'],
                    Resource: `${bucketArn}/*`,
                    Condition: {
                        StringEquals: { 's3:VersionId': versionIds[0] },
                    },
                },
            ],
        };
        metadata.getBucket(bucketName, log, (err, bucket) => {
            if (err) {
                return callback(err);
            }
            bucket.setBucketPolicy(policy);
            return metadata.updateBucket(bucketName, bucket, log, callback);
        });
    }

    before(done => {
        cleanup();
        async.series(
            [
                callback => bucketPut(authInfo, _createBucketPutRequest(bucketName), log, callback),
                callback => bucketPutVersioning(authInfo, enableRequest, log, callback),
                callback => _putObject(objData[0], callback),
                callback => _putObject(objData[1], callback),
                callback => _setBucketPolicy(callback),
            ],
            done,
        );
    });

    after(() => {
        cleanup();
    });

    it('should deny copying the version denied by the bucket policy', done => {
        const request = _createUserCopyRequest();
        objectCopy(userAuthInfo, request, bucketName, sourceKey, versionIds[0], log, err => {
            assert(err, 'expected the copy to be denied');
            assert.strictEqual(err.is.AccessDenied, true);
            done();
        });
    });

    it('should allow copying a version not denied by the bucket policy', done => {
        const request = _createUserCopyRequest();
        objectCopy(userAuthInfo, request, bucketName, sourceKey, versionIds[1], log, err => {
            assert.ifError(err);
            done();
        });
    });
});

describe('objectCopy of a specific source version', () => {
    const ownBucketName = 'ownversionbucket';
    const policyBucketName = 'policyversionbucket';
    const sharedBucketName = 'sharedversionbucket';
    const otherBucketName = 'otherversionbucket';
    const otherAuthInfo = makeAuthInfo('accessKey2');
    const userAuthInfo = makeAuthInfo(canonicalID, 'user');
    const otherUserAuthInfo = makeAuthInfo('accessKey2', 'user');
    const enableRequest = versioningTestUtils.createBucketPutVersioningReq(policyBucketName, 'Enabled');
    // The version IDs of the policy bucket source, as the client sees them
    const versionIds = [];

    function _createPutPolicyRequest(bucketName, statement) {
        return new DummyRequest({
            bucketName,
            namespace,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            url: '/',
            socket: {},
            post: JSON.stringify({
                Version: '2012-10-17',
                Statement: [statement],
            }),
        });
    }

    function _createUserCopyRequest(bucketName, actionImplicitDenies) {
        const request = new DummyRequest({
            bucketName,
            namespace,
            objectKey: 'copy',
            headers: {},
            url: `/${bucketName}/copy`,
            query: {},
            socket: {},
        });
        request.actionImplicitDenies = actionImplicitDenies;
        return request;
    }

    function _putObject(bucketName, body, callback) {
        objectPut(
            authInfo,
            versioningTestUtils.createPutObjectRequest(bucketName, 'secret', body),
            undefined,
            log,
            (err, resHeaders) => callback(err, resHeaders),
        );
    }

    before(done => {
        cleanup();
        async.series(
            [
                callback => bucketPut(authInfo, _createBucketPutRequest(ownBucketName), log, callback),
                callback => _putObject(ownBucketName, objData[0], callback),
                callback => bucketPut(authInfo, _createBucketPutRequest(policyBucketName), log, callback),
                callback => bucketPutVersioning(authInfo, enableRequest, log, callback),
                callback =>
                    _putObject(policyBucketName, objData[0], (err, resHeaders) => {
                        versionIds.push(resHeaders?.['x-amz-version-id']);
                        callback(err);
                    }),
                callback =>
                    _putObject(policyBucketName, objData[1], (err, resHeaders) => {
                        versionIds.push(resHeaders?.['x-amz-version-id']);
                        callback(err);
                    }),
                callback =>
                    bucketPutPolicy(
                        authInfo,
                        _createPutPolicyRequest(policyBucketName, {
                            Effect: 'Deny',
                            Principal: '*',
                            Action: 's3:GetObjectVersion',
                            Resource: `arn:aws:s3:::${policyBucketName}/*`,
                        }),
                        log,
                        callback,
                    ),
                callback => bucketPut(authInfo, _createBucketPutRequest(sharedBucketName), log, callback),
                callback => _putObject(sharedBucketName, objData[0], callback),
                callback =>
                    bucketPutPolicy(
                        authInfo,
                        _createPutPolicyRequest(sharedBucketName, {
                            Effect: 'Allow',
                            Principal: '*',
                            Action: ['s3:GetObject', 's3:GetObjectVersion'],
                            Resource: `arn:aws:s3:::${sharedBucketName}/*`,
                        }),
                        log,
                        callback,
                    ),
                callback => bucketPut(otherAuthInfo, _createBucketPutRequest(otherBucketName), log, callback),
            ],
            done,
        );
    });

    after(() => {
        cleanup();
    });

    it('should deny a null version copy when IAM does not allow reading the source version', done => {
        const request = _createUserCopyRequest(ownBucketName, { objectGetVersion: true, objectPut: false });
        objectCopy(userAuthInfo, request, ownBucketName, 'secret', 'null', log, err => {
            assert(err, 'expected the copy to be denied');
            assert.strictEqual(err.is.AccessDenied, true);
            done();
        });
    });

    it('should allow a null version copy when IAM allows reading the source version', done => {
        const request = _createUserCopyRequest(ownBucketName, { objectGetVersion: false, objectPut: false });
        objectCopy(userAuthInfo, request, ownBucketName, 'secret', 'null', log, err => {
            assert.ifError(err);
            done();
        });
    });

    it('should return InvalidArgument for a malformed source version ID', done => {
        const request = _createUserCopyRequest(ownBucketName, { objectGetVersion: false, objectPut: false });
        objectCopy(userAuthInfo, request, ownBucketName, 'secret', 'invalid', log, err => {
            assert(err, 'expected the copy to fail');
            assert.strictEqual(err.is.InvalidArgument, true);
            done();
        });
    });

    it('should deny copying a version when the bucket policy denies s3:GetObjectVersion', done => {
        const request = _createUserCopyRequest(policyBucketName, { objectGetVersion: false, objectPut: false });
        objectCopy(userAuthInfo, request, policyBucketName, 'secret', versionIds[0], log, err => {
            assert(err, 'expected the copy to be denied');
            assert.strictEqual(err.is.AccessDenied, true);
            done();
        });
    });

    it('should allow copying the latest version when the bucket policy denies s3:GetObjectVersion', done => {
        const request = _createUserCopyRequest(policyBucketName, { objectGet: false, objectPut: false });
        objectCopy(userAuthInfo, request, policyBucketName, 'secret', undefined, log, err => {
            assert.ifError(err);
            done();
        });
    });

    it('should deny a cross-account null version copy when IAM does not allow reading the source version', done => {
        const request = _createUserCopyRequest(otherBucketName, { objectGetVersion: true, objectPut: false });
        objectCopy(otherUserAuthInfo, request, sharedBucketName, 'secret', 'null', log, err => {
            assert(err, 'expected the copy to be denied');
            assert.strictEqual(err.is.AccessDenied, true);
            done();
        });
    });

    it('should allow a cross-account null version copy when IAM allows reading the source version', done => {
        const request = _createUserCopyRequest(otherBucketName, { objectGetVersion: false, objectPut: false });
        objectCopy(otherUserAuthInfo, request, sharedBucketName, 'secret', 'null', log, err => {
            assert.ifError(err);
            done();
        });
    });
});

describe('objectCopy data orphan cleanup on cross-backend copy-to-self', () => {
    const prevConfigBackendsData = data.config.backends.data;
    const prevConfigLocationConstraint1 = data.config.locationConstraints['us-east-1'].type;
    const prevConfigLocationConstraint2 = data.config.locationConstraints['us-east-2'].type;

    before(() => {
        data.config.backends.data = 'multiple';
        data.config.locationConstraints['us-east-1'].type = 'aws_s3';
        data.config.locationConstraints['us-east-2'].type = 'aws_s3';
    });

    after(() => {
        data.config.backends.data = prevConfigBackendsData;
        data.config.locationConstraints['us-east-1'].type = prevConfigLocationConstraint1;
        data.config.locationConstraints['us-east-2'].type = prevConfigLocationConstraint2;
    });

    beforeEach(done => {
        cleanup();
        async.series(
            [
                next => bucketPut(authInfo, putSourceBucketRequest, log, next),
                next =>
                    objectPut(
                        authInfo,
                        versioningTestUtils.createPutObjectRequest(sourceBucketName, objectKey, objData[0]),
                        undefined,
                        log,
                        next,
                    ),
            ],
            done,
        );
    });

    afterEach(() => cleanup());

    it('should reclaim the old location on copy-to-self that lands at a different backend key', done => {
        // Copy-to-self where the new metadata points to a different data key
        // than the source did (cross-backend rewrite via x-amz-meta-scal-
        // location-constraint). The old key is no longer referenced and must
        // be batchDeleted — guards against a pre-existing orphan bug.
        const batchDeleteSpy = sinon.spy(data, 'batchDelete');
        const copyObjectStub = sinon
            .stub(data, 'copyObject')
            .callsFake((req, srcLoc, sMP, dataLocator, ctx, backendInfo, srcBM, dstBM, sse, l, cb) =>
                cb(null, [{ key: 'new-backend-key', dataStoreName: 'us-east-2', size: objData[0].length, start: 0 }]),
            );
        metadata.getObjectMD(sourceBucketName, objectKey, {}, log, (err, srcMd) => {
            assert.ifError(err);
            const oldKeys = srcMd.location.map(l => l.key);
            const req = new DummyRequest({
                bucketName: sourceBucketName,
                namespace,
                objectKey,
                headers: {
                    'x-amz-metadata-directive': 'REPLACE',
                    'x-amz-meta-scal-location-constraint': 'us-east-2',
                },
                url: `/${sourceBucketName}/${objectKey}`,
                socket: {},
            });
            objectCopy(authInfo, req, sourceBucketName, objectKey, undefined, log, err => {
                batchDeleteSpy.restore();
                copyObjectStub.restore();
                assert.ifError(err);
                assert(batchDeleteSpy.calledOnce, 'data.batchDelete should reclaim the old data location');
                const reclaimed = batchDeleteSpy.firstCall.args[0];
                const reclaimedKeys = reclaimed.map(l => l.key);
                assert.deepStrictEqual(
                    reclaimedKeys,
                    oldKeys,
                    'batchDelete should target the old source key, not the new backend key',
                );
                done();
            });
        });
    });

    it('should reclaim the old location when the new backend key collides with the old one', done => {
        // bucketMatch-style external backends derive the backend key from the
        // S3 object key, so a copy-to-self that changes dataStoreName lands at
        // the same `key` in a different backend. Identity must include
        // dataStoreName or the old slot is silently leaked.
        const batchDeleteSpy = sinon.spy(data, 'batchDelete');
        metadata.getObjectMD(sourceBucketName, objectKey, {}, log, (err, srcMd) => {
            assert.ifError(err);
            const oldLoc = srcMd.location[0];
            const copyObjectStub = sinon
                .stub(data, 'copyObject')
                .callsFake((req, srcLoc, sMP, dataLocator, ctx, backendInfo, srcBM, dstBM, sse, l, cb) =>
                    cb(null, [{ key: oldLoc.key, dataStoreName: 'us-east-2', size: objData[0].length, start: 0 }]),
                );
            const req = new DummyRequest({
                bucketName: sourceBucketName,
                namespace,
                objectKey,
                headers: {
                    'x-amz-metadata-directive': 'REPLACE',
                    'x-amz-meta-scal-location-constraint': 'us-east-2',
                },
                url: `/${sourceBucketName}/${objectKey}`,
                socket: {},
            });
            objectCopy(authInfo, req, sourceBucketName, objectKey, undefined, log, err => {
                batchDeleteSpy.restore();
                copyObjectStub.restore();
                assert.ifError(err);
                assert(
                    batchDeleteSpy.calledOnce,
                    'data.batchDelete should reclaim the old slot even when the backend key matches',
                );
                const reclaimed = batchDeleteSpy.firstCall.args[0];
                assert.strictEqual(reclaimed.length, 1);
                assert.strictEqual(
                    reclaimed[0].dataStoreName,
                    oldLoc.dataStoreName,
                    'batchDelete must target the old dataStoreName, not the new one',
                );
                assert.strictEqual(reclaimed[0].key, oldLoc.key);
                done();
            });
        });
    });
});

describe('objectCopy legacy string-location copy-to-self', () => {
    beforeEach(done => {
        cleanup();
        async.series(
            [
                next => bucketPut(authInfo, putSourceBucketRequest, log, next),
                next =>
                    objectPut(
                        authInfo,
                        versioningTestUtils.createPutObjectRequest(sourceBucketName, objectKey, objData[0]),
                        undefined,
                        log,
                        next,
                    ),
            ],
            done,
        );
    });

    afterEach(() => cleanup());

    it('should not delete the reused location on copy-to-self with a legacy string location', done => {
        // Pre-md-model-version-2 objects store `location` as a bare string;
        // versioningPreprocessing then surfaces dataToDelete as a string array
        // while the reused new locator is { key }. The orphan filter must
        // normalize both or it deletes the live data on copy-to-self.
        const batchDeleteSpy = sinon.spy(data, 'batchDelete');
        metadata.getObjectMD(sourceBucketName, objectKey, {}, log, (err, md) => {
            assert.ifError(err);
            const legacyKey = String(md.location[0].key);
            // eslint-disable-next-line no-param-reassign
            md.location = legacyKey;
            // FULL_OBJECT checksum + no algo header keeps us on the propagate
            // (metadata-only reuse) path, so no data.get on the string key.
            // eslint-disable-next-line no-param-reassign
            md.checksum = { checksumAlgorithm: 'crc32', checksumValue: 'AAAAAA==', checksumType: 'FULL_OBJECT' };
            metadata.putObjectMD(sourceBucketName, objectKey, md, {}, log, err => {
                assert.ifError(err);
                const req = new DummyRequest({
                    bucketName: sourceBucketName,
                    namespace,
                    objectKey,
                    headers: { 'x-amz-metadata-directive': 'REPLACE' },
                    url: `/${sourceBucketName}/${objectKey}`,
                    socket: {},
                });
                objectCopy(authInfo, req, sourceBucketName, objectKey, undefined, log, err => {
                    batchDeleteSpy.restore();
                    assert.ifError(err);
                    assert(
                        batchDeleteSpy.notCalled,
                        'the reused legacy string location must not be treated as an orphan',
                    );
                    done();
                });
            });
        });
    });
});

describe('_orphanedDataLocations', () => {
    const orphanedDataLocations = objectCopy._orphanedDataLocations;

    it('should return null when there is nothing to delete', () => {
        const newLocs = [{ dataStoreName: 'l1', key: 'a' }];
        assert.strictEqual(orphanedDataLocations(undefined, newLocs), null);
        assert.strictEqual(orphanedDataLocations(null, newLocs), null);
        assert.strictEqual(orphanedDataLocations([], newLocs), null);
    });

    it('should return null when every prior location is still referenced', () => {
        const locs = [
            { dataStoreName: 'l1', key: 'a' },
            { dataStoreName: 'l1', key: 'b' },
        ];
        assert.strictEqual(orphanedDataLocations(locs, locs), null);
    });

    it('should flag a prior location whose key is no longer referenced', () => {
        const oldLocs = [{ dataStoreName: 'l1', key: 'a' }];
        const newLocs = [{ dataStoreName: 'l1', key: 'b' }];
        assert.deepStrictEqual(orphanedDataLocations(oldLocs, newLocs), oldLocs);
    });

    it('should treat the same key under a different dataStoreName as an orphan', () => {
        const oldLocs = [{ dataStoreName: 'us-east-1', key: 'a' }];
        const newLocs = [{ dataStoreName: 'us-east-2', key: 'a' }];
        assert.deepStrictEqual(orphanedDataLocations(oldLocs, newLocs), oldLocs);
    });

    it('should return only the subset of prior locations that are orphaned', () => {
        const reused = { dataStoreName: 'l1', key: 'keep' };
        const orphan = { dataStoreName: 'l1', key: 'gone' };
        assert.deepStrictEqual(orphanedDataLocations([reused, orphan], [reused]), [orphan]);
    });

    it('should not flag a key referenced by any of several new locations', () => {
        const oldLocs = [{ dataStoreName: 'l1', key: 'a' }];
        const newLocs = [
            { dataStoreName: 'l1', key: 'x' },
            { dataStoreName: 'l1', key: 'a' },
        ];
        assert.strictEqual(orphanedDataLocations(oldLocs, newLocs), null);
    });

    it('should normalize a reused legacy string location (not an orphan)', () => {
        // pre-md-model-version-2 string location reused as { key } by goGetData
        assert.strictEqual(orphanedDataLocations(['legacyKey'], [{ key: 'legacyKey' }]), null);
    });

    it('should flag a legacy string location that is no longer referenced', () => {
        assert.deepStrictEqual(orphanedDataLocations(['oldKey'], [{ key: 'newKey' }]), ['oldKey']);
    });
});

describe('objectCopy self-copy with SSE', () => {
    const plaintext = Buffer.from('this-must-become-encrypted', 'utf8');

    function putObject(headers, cb) {
        const request = new DummyRequest(
            {
                bucketName: destBucketName,
                namespace,
                objectKey,
                headers,
                url: `/${destBucketName}/${objectKey}`,
            },
            plaintext,
        );
        return objectPut(authInfo, request, undefined, log, err => cb(err));
    }

    function selfCopy(headers, cb) {
        const request = _createObjectCopyRequest(destBucketName, headers);
        return objectCopy(authInfo, request, destBucketName, objectKey, undefined, log, err => cb(err));
    }

    function getLocations(cb) {
        return metadata.getObjectMD(destBucketName, objectKey, {}, log, (err, md) => cb(err, md && md.location));
    }

    function getBody(cb) {
        const request = new DummyRequest({
            bucketName: destBucketName,
            namespace,
            objectKey,
            headers: {},
            url: `/${destBucketName}/${objectKey}`,
        });
        return objectGet(authInfo, request, false, log, (err, dataLocator) => {
            if (err) {
                return cb(err);
            }
            return async.mapSeries(
                dataLocator,
                (location, next) =>
                    data.get(location, null, log, (err, stream) => {
                        if (err) {
                            return next(err);
                        }
                        const chunks = [];
                        stream.on('data', chunk => chunks.push(chunk));
                        stream.on('error', next);
                        return stream.on('end', () => next(null, Buffer.concat(chunks)));
                    }),
                (err, parts) => cb(err, parts && Buffer.concat(parts)),
            );
        });
    }

    function createKey(cb) {
        return kms.client.createBucketKey(destBucketName, log, (err, keyId, keyArn) => cb(err, keyArn));
    }

    function kmsHeaders(keyArn) {
        return {
            'x-amz-server-side-encryption': 'aws:kms',
            'x-amz-server-side-encryption-aws-kms-key-id': keyArn,
        };
    }

    const replaceMetadataHeaders = {
        'x-amz-metadata-directive': 'REPLACE',
        'x-amz-meta-updated': 'true',
    };

    // Asserts the object data was rewritten: new locations, encrypted except
    // when the copy writes the data unencrypted (`encrypted` is false), the old
    // locations gone from the data store, and the original bytes read back.
    function assertDataRewritten(locationsBefore, done, { expectedBody = plaintext, encrypted = true } = {}) {
        return getLocations((err, locationsAfter) => {
            assert.ifError(err);
            assert.notDeepStrictEqual(
                locationsAfter.map(loc => loc.key),
                locationsBefore.map(loc => loc.key),
            );
            locationsAfter.forEach(loc => {
                if (encrypted) {
                    assert(loc.cipheredDataKey, 'new data location must be encrypted');
                    assert.notDeepStrictEqual(ds[loc.key].value, plaintext);
                } else {
                    assert.strictEqual(loc.cipheredDataKey, undefined);
                }
            });
            locationsBefore.forEach(loc => assert.strictEqual(ds[loc.key], undefined));
            return getBody((err, body) => {
                assert.ifError(err);
                assert.deepStrictEqual(body, expectedBody);
                done();
            });
        });
    }

    function assertDataKept(locationsBefore, done) {
        const dataOf = locations =>
            locations.map(({ key, dataStoreName, cipheredDataKey }) => ({ key, dataStoreName, cipheredDataKey }));
        return getLocations((err, locationsAfter) => {
            assert.ifError(err);
            assert.deepStrictEqual(dataOf(locationsAfter), dataOf(locationsBefore));
            return getBody((err, body) => {
                assert.ifError(err);
                assert.deepStrictEqual(body, plaintext);
                done();
            });
        });
    }

    function updateObjectMD(update, cb) {
        return metadata.getObjectMD(destBucketName, objectKey, {}, log, (err, md) => {
            if (err) {
                return cb(err);
            }
            update(md);
            return metadata.putObjectMD(destBucketName, objectKey, md, {}, log, err => cb(err, md.location));
        });
    }

    // The in-memory metadata returns the stored object itself: copy it to
    // compare before and after.
    function getObjectMD(cb) {
        return metadata.getObjectMD(destBucketName, objectKey, {}, log, (err, md) =>
            cb(err, md && JSON.parse(JSON.stringify(md))),
        );
    }

    function recordKeyOnLocations(md, keyArn) {
        md.location.forEach(location => {
            /* eslint-disable no-param-reassign */
            location.masterKeyId = keyArn;
            location.algorithm = 'aws:kms';
            /* eslint-enable no-param-reassign */
        });
    }

    // Puts the object encrypted under dataMasterKey, then sets
    // metadataMasterKey as the master key in the object metadata and records
    // recordedMasterKey on every location, as a metadata-only self-copy does.
    // A plain put records no key on the locations. With metadataMasterKey
    // other than dataMasterKey, the metadata does not name the key of the
    // data. Self-copies made before CLDSRV-910 left objects in this state:
    // they changed the master key without rewriting the data. The code no
    // longer produces it.
    function putObjectWithKeys({ dataMasterKey, metadataMasterKey, recordedMasterKey }, cb) {
        return putObject(kmsHeaders(dataMasterKey), err => {
            if (err) {
                return cb(err);
            }
            return updateObjectMD(md => {
                // eslint-disable-next-line no-param-reassign
                md['x-amz-server-side-encryption-aws-kms-key-id'] = metadataMasterKey;
                recordKeyOnLocations(md, recordedMasterKey);
            }, cb);
        });
    }

    function createKeys(count, cb) {
        return async.timesSeries(count, (n, next) => createKey(next), cb);
    }

    function initiateMpu(key, headers, cb) {
        const request = new DummyRequest({
            bucketName: destBucketName,
            namespace,
            objectKey: key,
            headers: { host: `${destBucketName}.s3.amazonaws.com`, ...headers },
            url: `/${key}?uploads`,
        });
        return initiateMultipartUpload(authInfo, request, log, (err, xml) => {
            if (err) {
                return cb(err);
            }
            return parseString(xml, (err, json) => cb(err, json && json.InitiateMultipartUploadResult.UploadId[0]));
        });
    }

    function partRequest(key, uploadId, partNumber, body) {
        return new DummyRequest(
            {
                bucketName: destBucketName,
                namespace,
                objectKey: key,
                headers: { host: `${destBucketName}.s3.amazonaws.com` },
                url: `/${key}?partNumber=${partNumber}&uploadId=${uploadId}`,
                query: { partNumber, uploadId },
            },
            body,
        );
    }

    function completeMpu(key, uploadId, partETags, cb) {
        const parts = partETags
            .map((eTag, i) => `<Part><PartNumber>${i + 1}</PartNumber><ETag>"${eTag}"</ETag></Part>`)
            .join('');
        const request = {
            bucketName: destBucketName,
            namespace,
            objectKey: key,
            parsedHost: 's3.amazonaws.com',
            url: `/${key}?uploadId=${uploadId}`,
            headers: { host: `${destBucketName}.s3.amazonaws.com` },
            query: { uploadId },
            post: `<CompleteMultipartUpload>${parts}</CompleteMultipartUpload>`,
        };
        return completeMultipartUpload(authInfo, request, log, err => cb(err));
    }

    const md5 = body => crypto.createHash('md5').update(body).digest('hex');

    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, putDestBucketRequest, log, done);
    });

    afterEach(() => {
        sinon.restore();
        cleanup();
    });

    it('should encrypt the data of an unencrypted object', done => {
        async.waterfall(
            [
                next => putObject({}, next),
                next => getLocations(next),
                (locationsBefore, next) =>
                    selfCopy({ 'x-amz-server-side-encryption': 'AES256' }, err => next(err, locationsBefore)),
            ],
            (err, locationsBefore) => {
                assert.ifError(err);
                assertDataRewritten(locationsBefore, done);
            },
        );
    });

    it('should re-encrypt the data when the copy names a different key', done => {
        async.waterfall(
            [
                next => createKey(next),
                (sourceKey, next) => putObject(kmsHeaders(sourceKey), next),
                next => getLocations(next),
                (locationsBefore, next) =>
                    createKey((err, destKey) => {
                        if (err) {
                            return next(err);
                        }
                        return selfCopy(kmsHeaders(destKey), err => next(err, locationsBefore));
                    }),
            ],
            (err, locationsBefore) => {
                assert.ifError(err);
                assertDataRewritten(locationsBefore, done);
            },
        );
    });

    it('should keep the data when the copy names the same key', done => {
        const cipherBundleSpy = sinon.spy(kms, 'createCipherBundle');
        let sourceKey;
        async.waterfall(
            [
                next => createKey(next),
                (key, next) => {
                    sourceKey = key;
                    return putObject(kmsHeaders(sourceKey), next);
                },
                next => getLocations(next),
                (locationsBefore, next) => {
                    cipherBundleSpy.resetHistory();
                    return selfCopy({ ...kmsHeaders(sourceKey), ...replaceMetadataHeaders }, err =>
                        next(err, locationsBefore),
                    );
                },
            ],
            (err, locationsBefore) => {
                assert.ifError(err);
                sinon.assert.notCalled(cipherBundleSpy);
                assertDataKept(locationsBefore, done);
            },
        );
    });

    it('should keep the data when the copy names the same key without its ARN prefix', done => {
        let sourceKey;
        async.waterfall(
            [
                next => createKey(next),
                (key, next) => {
                    sourceKey = key;
                    return putObject(kmsHeaders(sourceKey), next);
                },
                next => getLocations(next),
                (locationsBefore, next) =>
                    selfCopy({ ...kmsHeaders(sourceKey.slice(kms.arnPrefix.length)), ...replaceMetadataHeaders }, err =>
                        next(err, locationsBefore),
                    ),
            ],
            (err, locationsBefore) => {
                assert.ifError(err);
                assertDataKept(locationsBefore, done);
            },
        );
    });

    it('should keep the data when the bucket encryption applies the same key', done => {
        const putEncryptedBucketRequest = _createBucketPutRequest(destBucketName);
        putEncryptedBucketRequest.headers['x-amz-scal-server-side-encryption'] = 'AES256';
        cleanup();
        async.waterfall(
            [
                next => bucketPut(authInfo, putEncryptedBucketRequest, log, err => next(err)),
                next => putObject({}, next),
                next => getLocations(next),
                (locationsBefore, next) => selfCopy(replaceMetadataHeaders, err => next(err, locationsBefore)),
            ],
            (err, locationsBefore) => {
                assert.ifError(err);
                assertDataKept(locationsBefore, done);
            },
        );
    });

    it('should encrypt the data when only the metadata says it is encrypted', done => {
        async.waterfall(
            [
                next => createKey(next),
                (keyArn, next) => putObject({}, err => next(err, keyArn)),
                (keyArn, next) =>
                    metadata.getObjectMD(destBucketName, objectKey, {}, log, (err, md) => {
                        if (err) {
                            return next(err);
                        }
                        /* eslint-disable no-param-reassign */
                        md['x-amz-server-side-encryption'] = 'aws:kms';
                        md['x-amz-server-side-encryption-aws-kms-key-id'] = keyArn;
                        /* eslint-enable no-param-reassign */
                        return metadata.putObjectMD(destBucketName, objectKey, md, {}, log, err =>
                            next(err, keyArn, md.location),
                        );
                    }),
                (keyArn, locationsBefore, next) =>
                    selfCopy({ ...kmsHeaders(keyArn), ...replaceMetadataHeaders }, err => next(err, locationsBefore)),
            ],
            (err, locationsBefore) => {
                assert.ifError(err);
                assertDataRewritten(locationsBefore, done);
            },
        );
    });

    it('should keep the object unchanged when the KMS fails during the rewrite', done => {
        const kmsError = new Error('KMS unavailable');
        async.waterfall(
            [
                next => putObject({}, next),
                next => getLocations(next),
                (locationsBefore, next) => {
                    sinon.stub(kms, 'createCipherBundle').callsFake((sse, log, cb) => cb(kmsError));
                    return selfCopy({ 'x-amz-server-side-encryption': 'AES256' }, err => {
                        assert.strictEqual(err, kmsError);
                        sinon.restore();
                        next(null, locationsBefore);
                    });
                },
            ],
            (err, locationsBefore) => {
                assert.ifError(err);
                locationsBefore.forEach(loc => assert.deepStrictEqual(ds[loc.key].value, plaintext));
                assertDataKept(locationsBefore, done);
            },
        );
    });

    it('should keep the data when the copy is not encrypted', done => {
        async.waterfall(
            [
                next => putObject({}, next),
                next => getLocations(next),
                (locationsBefore, next) => selfCopy(replaceMetadataHeaders, err => next(err, locationsBefore)),
            ],
            (err, locationsBefore) => {
                assert.ifError(err);
                assertDataKept(locationsBefore, done);
            },
        );
    });

    describe('which master key decrypts the data', () => {
        // The test first sets up an object that a self-copy must refuse. Runs
        // the self-copy, and asserts it failed and changed neither the metadata
        // nor the data store.
        function selfCopyLeavesObjectUnchanged(headers, cb) {
            return getObjectMD((err, mdBefore) => {
                if (err) {
                    return cb(err);
                }
                const dataStoreSize = ds.length;
                return selfCopy(headers, copyErr => {
                    assert(copyErr, 'the copy must fail');
                    assert.strictEqual(ds.length, dataStoreSize);
                    mdBefore.location.forEach(loc => assert(ds[loc.key], 'original data must stay in the data store'));
                    return getObjectMD((err, mdAfter) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(mdAfter, mdBefore);
                        cb(null, copyErr);
                    });
                });
            });
        }

        it('should copy only the metadata when the copy asks for the key the data is encrypted under', done => {
            async.waterfall(
                [
                    next => createKeys(2, next),
                    // The data is encrypted under dataMasterKey, but the
                    // metadata names metadataMasterKey.
                    // Self-copies made before CLDSRV-910 left objects in this state.
                    ([dataMasterKey, metadataMasterKey], next) =>
                        putObjectWithKeys(
                            { dataMasterKey, metadataMasterKey, recordedMasterKey: dataMasterKey },
                            (err, locationsBefore) => next(err, dataMasterKey, locationsBefore),
                        ),
                    // The copy asks for dataMasterKey, which repairs the object.
                    (dataMasterKey, locationsBefore, next) =>
                        selfCopy(kmsHeaders(dataMasterKey), err => next(err, dataMasterKey, locationsBefore)),
                    (dataMasterKey, locationsBefore, next) =>
                        getObjectMD((err, md) => next(err, dataMasterKey, md, locationsBefore)),
                ],
                (err, dataMasterKey, md, locationsBefore) => {
                    assert.ifError(err);
                    // The metadata names dataMasterKey again, and the data was
                    // not rewritten.
                    assert.strictEqual(md['x-amz-server-side-encryption-aws-kms-key-id'], dataMasterKey);
                    assertDataKept(locationsBefore, done);
                },
            );
        });

        it('should decrypt the data with the key its locations record when re-encrypting it', done => {
            async.waterfall(
                [
                    next => createKeys(3, next),
                    // The data is encrypted under dataMasterKey, which its
                    // locations record, but the metadata names
                    // metadataMasterKey.
                    // Self-copies made before CLDSRV-910 left objects in this state.
                    ([dataMasterKey, metadataMasterKey, destMasterKey], next) =>
                        putObjectWithKeys(
                            { dataMasterKey, metadataMasterKey, recordedMasterKey: dataMasterKey },
                            (err, locationsBefore) => next(err, destMasterKey, locationsBefore),
                        ),
                    // The copy asks for a third key, so the data is rewritten.
                    (destMasterKey, locationsBefore, next) =>
                        selfCopy(kmsHeaders(destMasterKey), err => next(err, locationsBefore)),
                ],
                (err, locationsBefore) => {
                    assert.ifError(err);
                    // The original bytes read back: the copy decrypted the data
                    // with dataMasterKey, not metadataMasterKey.
                    assertDataRewritten(locationsBefore, done);
                },
            );
        });

        it('should keep the data when a copy asks for its master key after a copy removed the encryption', done => {
            async.waterfall(
                [
                    next => createKeys(2, next),
                    // The data is encrypted under dataMasterKey, but the
                    // metadata names metadataMasterKey.
                    // Self-copies made before CLDSRV-910 left objects in this state.
                    ([dataMasterKey, metadataMasterKey], next) =>
                        putObjectWithKeys({ dataMasterKey, metadataMasterKey, recordedMasterKey: dataMasterKey }, err =>
                            next(err, dataMasterKey),
                        ),
                    // A copy without SSE removes the encryption from the
                    // metadata, but the data stays encrypted.
                    (dataMasterKey, next) => selfCopy(replaceMetadataHeaders, err => next(err, dataMasterKey)),
                    (dataMasterKey, next) =>
                        getLocations((err, locationsBefore) => next(err, dataMasterKey, locationsBefore)),
                    // The copy asks for dataMasterKey: the data is already
                    // under it, so only the metadata changes.
                    (dataMasterKey, locationsBefore, next) =>
                        selfCopy(kmsHeaders(dataMasterKey), err => next(err, locationsBefore)),
                ],
                (err, locationsBefore) => {
                    assert.ifError(err);
                    assertDataKept(locationsBefore, done);
                },
            );
        });

        it('should refuse the copy when no key decrypts the data', done => {
            async.waterfall(
                [
                    next => createKeys(3, next),
                    // The data is encrypted under dataMasterKey, but neither
                    // the metadata nor the locations name it.
                    // Self-copies made before CLDSRV-910 left objects in this state.
                    ([dataMasterKey, metadataMasterKey, destMasterKey], next) =>
                        putObjectWithKeys(
                            { dataMasterKey, metadataMasterKey, recordedMasterKey: metadataMasterKey },
                            err => next(err, destMasterKey),
                        ),
                    // The copy tries metadataMasterKey and destMasterKey, and
                    // neither decrypts the data.
                    (destMasterKey, next) => selfCopyLeavesObjectUnchanged(kmsHeaders(destMasterKey), next),
                ],
                (err, copyErr) => {
                    assert.ifError(err);
                    assert(copyErr.is.InternalError, `expected InternalError, got ${copyErr}`);
                    done();
                },
            );
        });

        it('should refuse the copy when the encrypted data has no recorded MD5', done => {
            async.waterfall(
                [
                    next => createKeys(2, next),
                    // The locations record the key, so the copy checks which
                    // key decrypts the data before rewriting it.
                    ([dataMasterKey, destMasterKey], next) =>
                        putObjectWithKeys(
                            { dataMasterKey, metadataMasterKey: dataMasterKey, recordedMasterKey: dataMasterKey },
                            err => next(err, destMasterKey),
                        ),
                    // Without an MD5 to compare with, the check cannot pass.
                    (destMasterKey, next) =>
                        updateObjectMD(
                            // eslint-disable-next-line no-param-reassign
                            md => md.location.forEach(location => delete location.dataStoreETag),
                            err => next(err, destMasterKey),
                        ),
                    (destMasterKey, next) => selfCopyLeavesObjectUnchanged(kmsHeaders(destMasterKey), next),
                ],
                (err, copyErr) => {
                    assert.ifError(err);
                    assert(copyErr.is.InternalError, `expected InternalError, got ${copyErr}`);
                    done();
                },
            );
        });

        it('should return the KMS error of the metadata master key when no other key decrypts the data', done => {
            const kmsError = new Error('KMS unavailable');
            async.waterfall(
                [
                    next => createKeys(2, next),
                    ([dataMasterKey, destMasterKey], next) =>
                        putObjectWithKeys(
                            { dataMasterKey, metadataMasterKey: dataMasterKey, recordedMasterKey: dataMasterKey },
                            err => next(err, dataMasterKey, destMasterKey),
                        ),
                    // The KMS fails for dataMasterKey, the key the metadata
                    // and the locations name, and destMasterKey does not
                    // decrypt the data.
                    (dataMasterKey, destMasterKey, next) => {
                        const createDecipherBundle = kms.createDecipherBundle;
                        sinon
                            .stub(kms, 'createDecipherBundle')
                            .callsFake((sse, offset, log, cb) =>
                                kms.isSameMasterKey(sse.masterKeyId, dataMasterKey, log)
                                    ? cb(kmsError)
                                    : createDecipherBundle.call(kms, sse, offset, log, cb),
                            );
                        return selfCopyLeavesObjectUnchanged(kmsHeaders(destMasterKey), next);
                    },
                ],
                (err, copyErr) => {
                    assert.ifError(err);
                    assert.strictEqual(copyErr, kmsError);
                    done();
                },
            );
        });

        it('should rewrite the data without reading it first when its locations record no key', done => {
            async.waterfall(
                [
                    next => createKeys(2, next),
                    // A plain put records no key on the locations.
                    ([dataMasterKey, destMasterKey], next) =>
                        putObject(kmsHeaders(dataMasterKey), err => next(err, destMasterKey)),
                    (destMasterKey, next) =>
                        getLocations((err, locationsBefore) => next(err, destMasterKey, locationsBefore)),
                    (destMasterKey, locationsBefore, next) => {
                        const dataGetSpy = sinon.spy(data, 'get');
                        return selfCopy(kmsHeaders(destMasterKey), err =>
                            next(err, dataGetSpy.callCount, locationsBefore),
                        );
                    },
                ],
                (err, dataReads, locationsBefore) => {
                    assert.ifError(err);
                    // Only the read of the rewrite: no check before it.
                    assert.strictEqual(dataReads, 1);
                    assertDataRewritten(locationsBefore, done);
                },
            );
        });

        it('should encrypt plaintext data recorded as encrypted', done => {
            async.waterfall(
                [
                    next => createKey(next),
                    (keyArn, next) => putObject({}, err => next(err, keyArn)),
                    // Self-copies made before CLDSRV-910 left objects in this
                    // state: one that asked for encryption marked the
                    // plaintext data as encrypted without rewriting it, and a
                    // later one recorded the key on the locations.
                    (keyArn, next) =>
                        updateObjectMD(
                            md => {
                                /* eslint-disable no-param-reassign */
                                md['x-amz-server-side-encryption'] = 'aws:kms';
                                md['x-amz-server-side-encryption-aws-kms-key-id'] = keyArn;
                                /* eslint-enable no-param-reassign */
                                recordKeyOnLocations(md, keyArn);
                            },
                            (err, locationsBefore) => next(err, keyArn, locationsBefore),
                        ),
                    (keyArn, locationsBefore, next) => selfCopy(kmsHeaders(keyArn), err => next(err, locationsBefore)),
                ],
                (err, locationsBefore) => {
                    assert.ifError(err);
                    assertDataRewritten(locationsBefore, done);
                },
            );
        });

        it('should check every location of a part copied from several locations', done => {
            const sourceKey = 'mpusource';
            const partBodies = [Buffer.alloc(constants.minimumAllowedPartSize, 'a'), Buffer.from('last part')];
            const body = Buffer.concat(partBodies);
            let keys;
            async.waterfall(
                [
                    next => createKeys(2, next),
                    // A source of two parts, copied as one part: the object
                    // has one part over two locations, and both carry the MD5
                    // of the whole part.
                    (created, next) => {
                        keys = created;
                        return initiateMpu(sourceKey, kmsHeaders(keys[0]), next);
                    },
                    (uploadId, next) =>
                        async.timesSeries(
                            partBodies.length,
                            (i, partDone) =>
                                objectPutPart(
                                    authInfo,
                                    partRequest(sourceKey, uploadId, i + 1, partBodies[i]),
                                    undefined,
                                    log,
                                    err => partDone(err),
                                ),
                            err => next(err, uploadId),
                        ),
                    (uploadId, next) => completeMpu(sourceKey, uploadId, partBodies.map(md5), next),
                    next => initiateMpu(objectKey, kmsHeaders(keys[0]), next),
                    (uploadId, next) =>
                        objectPutCopyPart(
                            authInfo,
                            partRequest(objectKey, uploadId, 1),
                            destBucketName,
                            sourceKey,
                            undefined,
                            log,
                            err => next(err, uploadId),
                        ),
                    (uploadId, next) => completeMpu(objectKey, uploadId, [md5(body)], next),
                    // A metadata-only self-copy records the key on the
                    // locations, so the next copy checks it.
                    next => selfCopy({ ...kmsHeaders(keys[0]), ...replaceMetadataHeaders }, next),
                    next => getLocations(next),
                    (locationsBefore, next) => {
                        assert.strictEqual(locationsBefore.length, 2);
                        assert.strictEqual(locationsBefore[0].dataStoreETag, locationsBefore[1].dataStoreETag);
                        assert(locationsBefore[0].masterKeyId);
                        // The check finds the key only if it reads both
                        // locations to compute the MD5 of the part.
                        return selfCopy(kmsHeaders(keys[1]), err => next(err, locationsBefore));
                    },
                ],
                (err, locationsBefore) => {
                    assert.ifError(err);
                    assertDataRewritten(locationsBefore, done, { expectedBody: body });
                },
            );
        });

        const otherLocationHeaders = {
            'x-amz-metadata-directive': 'REPLACE',
            'x-amz-meta-scal-location-constraint': 'us-east-2',
        };

        it('should decrypt the data with the key its locations record when the copy changes location', done => {
            async.waterfall(
                [
                    next => createKeys(2, next),
                    // The data is encrypted under dataMasterKey, which its
                    // locations record, but the metadata names
                    // metadataMasterKey.
                    // Self-copies made before CLDSRV-910 left objects in this state.
                    ([dataMasterKey, metadataMasterKey], next) =>
                        putObjectWithKeys(
                            { dataMasterKey, metadataMasterKey, recordedMasterKey: dataMasterKey },
                            (err, locationsBefore) => next(err, metadataMasterKey, locationsBefore),
                        ),
                    // The copy keeps metadataMasterKey but moves the object to
                    // another location, so the data is rewritten.
                    (metadataMasterKey, locationsBefore, next) =>
                        selfCopy({ ...kmsHeaders(metadataMasterKey), ...otherLocationHeaders }, err =>
                            next(err, locationsBefore),
                        ),
                ],
                (err, locationsBefore) => {
                    assert.ifError(err);
                    assertDataRewritten(locationsBefore, done);
                },
            );
        });

        it('should decrypt with the key its locations record when an unencrypted copy changes location', done => {
            async.waterfall(
                [
                    next => createKeys(2, next),
                    // The data is encrypted under dataMasterKey, which its
                    // locations record, but the metadata names
                    // metadataMasterKey.
                    // Self-copies made before CLDSRV-910 left objects in this state.
                    ([dataMasterKey, metadataMasterKey], next) =>
                        putObjectWithKeys({ dataMasterKey, metadataMasterKey, recordedMasterKey: dataMasterKey }, next),
                    // The copy has no SSE headers and the bucket no default
                    // encryption: the data is rewritten unencrypted to
                    // another location.
                    (locationsBefore, next) => selfCopy(otherLocationHeaders, err => next(err, locationsBefore)),
                ],
                (err, locationsBefore) => {
                    assert.ifError(err);
                    assertDataRewritten(locationsBefore, done, { encrypted: false });
                },
            );
        });
    });
});

describe('_mustReencryptData', () => {
    const mustReencryptData = objectCopy._mustReencryptData;
    let keyArn;
    let otherKeyArn;

    before(done => {
        kms.client.createBucketKey(destBucketName, log, (err, id, arn) => {
            if (err) {
                return done(err);
            }
            keyArn = arn;
            return kms.client.createBucketKey(destBucketName, log, (err, otherId, otherArn) => {
                otherKeyArn = otherArn;
                done(err);
            });
        });
    });

    const encryptedLocation = masterKeyId => ({ key: 1, cipheredDataKey: 'ZGF0YWtleQ==', masterKeyId });

    it('should not re-encrypt when the destination is not encrypted', () => {
        assert.strictEqual(mustReencryptData([{ key: 1 }], null, log), false);
        assert.strictEqual(mustReencryptData([{ key: 1 }], {}, log), false);
    });

    it('should not re-encrypt a zero-byte object', () => {
        assert.strictEqual(mustReencryptData([], { algorithm: 'AES256', masterKeyId: keyArn }, log), false);
    });

    it('should re-encrypt a legacy string location', () => {
        assert.strictEqual(
            mustReencryptData([{ key: 'legacyKey' }], { algorithm: 'AES256', masterKeyId: keyArn }, log),
            true,
        );
    });

    it('should not re-encrypt when every location is under the destination key', () => {
        const locations = [encryptedLocation(keyArn), encryptedLocation(keyArn)];
        assert.strictEqual(mustReencryptData(locations, { algorithm: 'aws:kms', masterKeyId: keyArn }, log), false);
    });

    it('should re-encrypt when one location is under another key', () => {
        const locations = [encryptedLocation(keyArn), encryptedLocation(otherKeyArn)];
        assert.strictEqual(mustReencryptData(locations, { algorithm: 'aws:kms', masterKeyId: keyArn }, log), true);
    });

    it('should prefer the configured key of the destination', () => {
        const sse = { algorithm: 'aws:kms', masterKeyId: keyArn, configuredMasterKeyId: otherKeyArn };
        assert.strictEqual(mustReencryptData([encryptedLocation(keyArn)], sse, log), true);
        assert.strictEqual(mustReencryptData([encryptedLocation(otherKeyArn)], sse, log), false);
    });

    it('should re-encrypt when the destination has no key', () => {
        assert.strictEqual(mustReencryptData([encryptedLocation(keyArn)], { algorithm: 'AES256' }, log), true);
    });
});
