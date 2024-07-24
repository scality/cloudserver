const assert = require('assert');
const AWS = require('aws-sdk');
const async = require('async');
const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');
const { versioning } = require('arsenal');
const versionIdUtils = versioning.VersionID;

const { makeid } = require('../../unit/helpers');
const { makeRequest, makeBackbeatRequest } = require('../../functional/raw-node/utils/makeRequest');
const BucketUtility =
      require('../../functional/aws-node-sdk/lib/utility/bucket-util');
const {
    itSkipCeph,
    awsLocation,
    azureLocation,
    getAzureContainerName,
    getAzureClient,
} = require('../../functional/aws-node-sdk/test/multipleBackend/utils');
const { getRealAwsConfig } =
      require('../../functional/aws-node-sdk/test/support/awsConfig');
const { getCredentials } = require('../../functional/aws-node-sdk/test/support/credentials');
const { config } = require('../../../lib/Config');

const awsConfig = getRealAwsConfig(awsLocation);
const awsClient = new AWS.S3(awsConfig);
const awsBucket = config.locationConstraints[awsLocation].details.bucketName;
const azureClient = getAzureClient();
const containerName = getAzureContainerName(azureLocation);

const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';

const { accessKeyId, secretAccessKey } = getCredentials();

const backbeatAuthCredentials = {
    accessKey: accessKeyId,
    secretKey: secretAccessKey,
};
const TEST_BUCKET = 'backbeatbucket';
const TEST_ENCRYPTED_BUCKET = 'backbeatbucket-encrypted';
const TEST_KEY = 'fookey';
const NONVERSIONED_BUCKET = 'backbeatbucket-non-versioned';
const BUCKET_FOR_NULL_VERSION = 'backbeatbucket-null-version';

const testArn = 'aws::iam:123456789012:user/bart';
const testKey = 'testkey';
const testKeyUTF8 = '䆩鈁櫨㟔罳';
const testData = 'testkey data';
const testDataMd5 = crypto.createHash('md5')
          .update(testData, 'utf-8')
          .digest('hex');
const emptyContentsMd5 = 'd41d8cd98f00b204e9800998ecf8427e';
const testMd = {
    'md-model-version': 2,
    'owner-display-name': 'Bart',
    'owner-id': ('79a59df900b949e55d96a1e698fbaced' +
                 'fd6e09d98eacf8f8d5218e7cd47ef2be'),
    'last-modified': '2017-05-15T20:32:40.032Z',
    'content-length': testData.length,
    'content-md5': testDataMd5,
    'x-amz-server-version-id': '',
    'x-amz-storage-class': 'STANDARD',
    'x-amz-server-side-encryption': '',
    'x-amz-server-side-encryption-aws-kms-key-id': '',
    'x-amz-server-side-encryption-customer-algorithm': '',
    'location': null,
    'acl': {
        Canned: 'private',
        FULL_CONTROL: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    },
    'nullVersionId': '99999999999999999999RG001  ',
    'isDeleteMarker': false,
    'versionId': '98505119639965999999RG001  ',
    'replicationInfo': {
        status: 'COMPLETED',
        backends: [{ site: 'zenko', status: 'PENDING' }],
        content: ['DATA', 'METADATA'],
        destination: 'arn:aws:s3:::dummy-dest-bucket',
        storageClass: 'STANDARD',
    },
};

const nonVersionedTestMd = {
    'owner-display-name': 'Bart',
    'owner-id': ('79a59df900b949e55d96a1e698fbaced' +
                 'fd6e09d98eacf8f8d5218e7cd47ef2be'),
    'content-length': testData.length,
    'content-md5': testDataMd5,
    'x-amz-version-id': 'null',
    'x-amz-server-version-id': '',
    'x-amz-storage-class': 'awsbackend',
    'x-amz-server-side-encryption': '',
    'x-amz-server-side-encryption-aws-kms-key-id': '',
    'x-amz-server-side-encryption-customer-algorithm': '',
    'acl': {
        Canned: 'private',
        FULL_CONTROL: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    },
    'location': null,
    'isNull': '',
    'nullVersionId': '',
    'isDeleteMarker': false,
    'tags': {},
    'replicationInfo': {
        status: '',
        backends: [],
        content: [],
        destination: '',
        storageClass: '',
        role: '',
        storageType: '',
        dataStoreVersionId: '',
        isNFS: null,
    },
    'dataStoreName': 'us-east-1',
    'last-modified': '2018-12-18T01:22:15.986Z',
    'md-model-version': 3,
};

function checkObjectData(s3, bucket, objectKey, dataValue, done) {
    s3.getObject({
        Bucket: bucket,
        Key: objectKey,
    }, (err, data) => {
        assert.ifError(err);
        assert.strictEqual(data.Body.toString(), dataValue);
        done();
    });
}

function checkVersionData(s3, bucket, objectKey, versionId, dataValue, done) {
    return s3.getObject({
        Bucket: bucket,
        Key: objectKey,
        VersionId: versionId,
    }, (err, data) => {
        assert.ifError(err);
        assert.strictEqual(data.Body.toString(), dataValue);
        return done();
    });
}

describe.skip('backbeat DELETE routes', () => {
    it('abort MPU', done => {
        const awsKey = 'backbeat-mpu-test';
        async.waterfall([
            next =>
                awsClient.createMultipartUpload({
                    Bucket: awsBucket,
                    Key: awsKey,
                }, next),
            (response, next) => {
                const { UploadId } = response;
                makeBackbeatRequest({
                    method: 'DELETE',
                    bucket: awsBucket,
                    objectKey: awsKey,
                    resourceType: 'multiplebackenddata',
                    queryObj: { operation: 'abortmpu' },
                    headers: {
                        'x-scal-upload-id': UploadId,
                        'x-scal-storage-type': 'aws_s3',
                        'x-scal-storage-class': awsLocation,
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, response) => {
                    assert.ifError(err);
                    assert.strictEqual(response.statusCode, 200);
                    assert.deepStrictEqual(JSON.parse(response.body), {});
                    return next(null, UploadId);
                });
            }, (UploadId, next) =>
                awsClient.listMultipartUploads({
                    Bucket: awsBucket,
                }, (err, response) => {
                    assert.ifError(err);
                    const hasOngoingUpload =
                        response.Uploads.some(upload => (upload === UploadId));
                    assert(!hasOngoingUpload);
                    return next();
                }),
        ], err => {
            assert.ifError(err);
            done();
        });
    });
});

function updateStorageClass(data, storageClass) {
    let result;
    try {
        const parsedBody = JSON.parse(JSON.parse(data.body).Body);
        parsedBody['x-amz-storage-class'] = storageClass;
        result = JSON.stringify(parsedBody);
    } catch (err) {
        return { error: err };
    }

    return { result };
}

function getMetadataToPut(putDataResponse) {
    const mdToPut = Object.assign({}, testMd);
    // Reproduce what backbeat does to update target metadata
    mdToPut.location = JSON.parse(putDataResponse.body);
    ['x-amz-server-side-encryption',
     'x-amz-server-side-encryption-aws-kms-key-id',
     'x-amz-server-side-encryption-customer-algorithm'].forEach(headerName => {
         if (putDataResponse.headers[headerName]) {
             mdToPut[headerName] = putDataResponse.headers[headerName];
         }
     });
    return mdToPut;
}

describe('backbeat routes', () => {
    let bucketUtil;
    let s3;

    before(done => {
        bucketUtil = new BucketUtility(
            'default', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;
        bucketUtil.emptyManyIfExists([TEST_BUCKET, TEST_ENCRYPTED_BUCKET, NONVERSIONED_BUCKET])
            .then(() => s3.createBucket({ Bucket: TEST_BUCKET }).promise())
            .then(() => s3.putBucketVersioning(
                {
                    Bucket: TEST_BUCKET,
                    VersioningConfiguration: { Status: 'Enabled' },
                }).promise())
            .then(() => s3.createBucket({ Bucket: NONVERSIONED_BUCKET }).promise())
            .then(() => s3.createBucket({ Bucket: TEST_ENCRYPTED_BUCKET }).promise())
            .then(() => s3.putBucketVersioning(
                {
                    Bucket: TEST_ENCRYPTED_BUCKET,
                    VersioningConfiguration: { Status: 'Enabled' },
                }).promise())
            .then(() => s3.putBucketEncryption(
                {
                    Bucket: TEST_ENCRYPTED_BUCKET,
                    ServerSideEncryptionConfiguration: {
                        Rules: [
                            {
                                ApplyServerSideEncryptionByDefault: {
                                    SSEAlgorithm: 'AES256',
                                },
                            },
                        ],
                    },
                }).promise())
            .then(() => done())
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
    });

    after(done =>
        bucketUtil.empty(TEST_BUCKET)
            .then(() => s3.deleteBucket({ Bucket: TEST_BUCKET }).promise())
            .then(() => bucketUtil.empty(TEST_ENCRYPTED_BUCKET))
            .then(() => s3.deleteBucket({ Bucket: TEST_ENCRYPTED_BUCKET }).promise())
            .then(() => bucketUtil.empty(NONVERSIONED_BUCKET))
            .then(() => s3.deleteBucket({ Bucket: NONVERSIONED_BUCKET }).promise())
            .then(() => done(), err => done(err))
    );

    describe('null version', () => {
        const bucket = BUCKET_FOR_NULL_VERSION;
        const keyName = 'key0';
        const storageClass = 'foo';

        function assertVersionIsNullAndUpdated(version) {
            const { Key, VersionId, StorageClass } = version;
            assert.strictEqual(Key, keyName);
            assert.strictEqual(VersionId, 'null');
            assert.strictEqual(StorageClass, storageClass);
        }

        function assertVersionHasNotBeenUpdated(version, expectedVersionId) {
            const { Key, VersionId, StorageClass } = version;
            assert.strictEqual(Key, keyName);
            assert.strictEqual(VersionId, expectedVersionId);
            assert.strictEqual(StorageClass, 'STANDARD');
        }

        beforeEach(done =>
            bucketUtil.emptyIfExists(BUCKET_FOR_NULL_VERSION)
                .then(() => s3.createBucket({ Bucket: BUCKET_FOR_NULL_VERSION }).promise())
                .then(() => done(), err => done(err))
        );

        afterEach(done =>
            bucketUtil.empty(BUCKET_FOR_NULL_VERSION)
                .then(() => s3.deleteBucket({ Bucket: BUCKET_FOR_NULL_VERSION }).promise())
                .then(() => done(), err => done(err))
        );

        it('should update metadata of a current null version', done => {
            let objMD;
            return async.series({
                putObject: next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                enableVersioningSource: next => s3.putBucketVersioning(
                    { Bucket: bucket, VersioningConfiguration: { Status: 'Enabled' } }, next),
                getMetadata: next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                putMetadata: next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                headObject: next => s3.headObject(
                    { Bucket: bucket, Key: keyName, VersionId: 'null' }, next),
                getMetadataAfter: next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, next),
                listObjectVersions: next => s3.listObjectVersions({ Bucket: bucket }, next),
            }, (err, results) => {
                if (err) {
                    return done(err);
                }

                const headObjectRes = results.headObject;
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const getMetadataAfterRes = results.getMetadataAfter;
                const objMDAfter = JSON.parse(getMetadataAfterRes.body).Body;
                const expectedMd = JSON.parse(objMD);
                expectedMd.isNull = true; // TODO remove the line once CLDSRV-509 is fixed
                assert.deepStrictEqual(JSON.parse(objMDAfter), expectedMd);

                const listObjectVersionsRes = results.listObjectVersions;
                const { Versions } = listObjectVersionsRes;

                assert.strictEqual(Versions.length, 1);

                const [currentVersion] = Versions;
                assertVersionIsNullAndUpdated(currentVersion);
                return done();
            });
        });

        it('should update metadata of a non-current null version', done => {
            let objMD;
            let expectedVersionId;
            return async.series({
                putObjectInitial: next => s3.putObject(
                    { Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                enableVersioning: next => s3.putBucketVersioning(
                    { Bucket: bucket, VersioningConfiguration: { Status: 'Enabled' } }, next),
                putObjectAgain: next => s3.putObject(
                { Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    expectedVersionId = data.VersionId;
                    return next();
                }),
                getMetadata: next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                putMetadata: next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                headObject: next => s3.headObject({ Bucket: bucket, Key: keyName, VersionId: 'null' }, next),
                getMetadataAfter: next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, next),
                listObjectVersions: next => s3.listObjectVersions({ Bucket: bucket }, next),
            }, (err, results) => {
                if (err) {
                    return done(err);
                }
                const headObjectRes = results.headObject;
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const getMetadataAfterRes = results.getMetadataAfter;
                const objMDAfter = JSON.parse(getMetadataAfterRes.body).Body;
                assert.deepStrictEqual(JSON.parse(objMDAfter), JSON.parse(objMD));

                const listObjectVersionsRes = results.listObjectVersions;
                const { Versions } = listObjectVersionsRes;

                assert.strictEqual(Versions.length, 2);
                const currentVersion = Versions.find(v => v.IsLatest);
                assertVersionHasNotBeenUpdated(currentVersion, expectedVersionId);

                const nonCurrentVersion = Versions.find(v => !v.IsLatest);
                assertVersionIsNullAndUpdated(nonCurrentVersion);
                return done();
            });
        });

        it('should update metadata of a suspended null version', done => {
            let objMD;
            return async.series({
                suspendVersioning: next => s3.putBucketVersioning(
                    { Bucket: bucket, VersioningConfiguration: { Status: 'Suspended' } }, next),
                putObject: next => s3.putObject(
                    { Bucket: bucket, Key: keyName, Body: Buffer.from(testData) }, next),
                enableVersioning: next => s3.putBucketVersioning(
                    { Bucket: bucket, VersioningConfiguration: { Status: 'Enabled' } }, next),
                getMetadata: next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                putUpdatedMetadata: next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                headObject: next => s3.headObject({ Bucket: bucket, Key: keyName, VersionId: 'null' }, next),
                getMetadataAfter: next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, next),
                listObjectVersions: next => s3.listObjectVersions({ Bucket: bucket }, next),
            }, (err, results) => {
                if (err) {
                    return done(err);
                }
                const headObjectRes = results.headObject;
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const getMetadataAfterRes = results.getMetadataAfter;
                const objMDAfter = JSON.parse(getMetadataAfterRes.body).Body;
                assert.deepStrictEqual(JSON.parse(objMDAfter), JSON.parse(objMD));

                const listObjectVersionsRes = results.listObjectVersions;
                const { Versions } = listObjectVersionsRes;

                assert.strictEqual(Versions.length, 1);

                const [currentVersion] = Versions;
                assertVersionIsNullAndUpdated(currentVersion);
                return done();
            });
        });

        it('should update metadata of a suspended null version with internal version id', done => {
            let objMD;
            return async.series({
                suspendVersioning: next => s3.putBucketVersioning(
                    { Bucket: bucket, VersioningConfiguration: { Status: 'Suspended' } }, next),
                putObject: next => s3.putObject(
                    { Bucket: bucket, Key: keyName, Body: Buffer.from(testData) }, next),
                enableVersioning: next => s3.putBucketVersioning(
                    { Bucket: bucket, VersioningConfiguration: { Status: 'Enabled' } }, next),
                putObjectTagging: next => s3.putObjectTagging({
                    Bucket: bucket, Key: keyName, VersionId: 'null',
                    Tagging: { TagSet: [{ Key: 'key1', Value: 'value1' }] },
                }, next),
                getMetadata: next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                putUpdatedMetadata: next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                headObject: next => s3.headObject({ Bucket: bucket, Key: keyName, VersionId: 'null' }, next),
                getMetadataAfter: next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, next),
                listObjectVersions: next => s3.listObjectVersions({ Bucket: bucket }, next),
            }, (err, results) => {
                if (err) {
                    return done(err);
                }
                const headObjectRes = results.headObject;
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const getMetadataAfterRes = results.getMetadataAfter;
                const objMDAfter = JSON.parse(getMetadataAfterRes.body).Body;
                assert.deepStrictEqual(JSON.parse(objMDAfter), JSON.parse(objMD));

                const listObjectVersionsRes = results.listObjectVersions;
                const { Versions } = listObjectVersionsRes;

                assert.strictEqual(Versions.length, 1);

                const [currentVersion] = Versions;
                assertVersionIsNullAndUpdated(currentVersion);
                return done();
            });
        });

        it('should update metadata of a non-version object', done => {
            let objMD;
            return async.series([
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                next => s3.headObject({ Bucket: bucket, Key: keyName }, next),
                next => s3.listObjectVersions({ Bucket: bucket }, next),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }

                const headObjectRes = data[3];
                assert(!headObjectRes.VersionId);
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[4];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;

                assert.strictEqual(DeleteMarkers.length, 0);
                assert.strictEqual(Versions.length, 1);

                const currentVersion = Versions[0];
                assert(currentVersion.IsLatest);
                assertVersionIsNullAndUpdated(currentVersion);
                return done();
            });
        });

        it('should create a new null version if versioning suspended and no version', done => {
            let objMD;
            return async.series([
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Suspended' } },
                    next),
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                next => s3.deleteObject({ Bucket: bucket, Key: keyName, VersionId: 'null' }, next),
                next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                next => s3.headObject({ Bucket: bucket, Key: keyName }, next),
                next => s3.listObjectVersions({ Bucket: bucket }, next),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }
                const headObjectRes = data[5];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[6];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;

                assert.strictEqual(DeleteMarkers.length, 0);
                assert.strictEqual(Versions.length, 1);

                const currentVersion = Versions[0];
                assert(currentVersion.IsLatest);

                assertVersionIsNullAndUpdated(currentVersion);

                return done();
            });
        });

        it('should create a new null version if versioning suspended and delete marker null version', done => {
            let objMD;
            return async.series([
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Suspended' } },
                    next),
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                next => s3.deleteObject({ Bucket: bucket, Key: keyName }, next),
                next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                next => s3.headObject({ Bucket: bucket, Key: keyName }, next),
                next => s3.listObjectVersions({ Bucket: bucket }, next),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }
                const headObjectRes = data[5];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[6];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;

                assert.strictEqual(DeleteMarkers.length, 0);
                assert.strictEqual(Versions.length, 1);

                const currentVersion = Versions[0];
                assert(currentVersion.IsLatest);
                assertVersionIsNullAndUpdated(currentVersion);
                return done();
            });
        });

        it('should create a new null version if versioning suspended and version has version id', done => {
            let expectedVersionId;
            let objMD;
            return async.series([
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Enabled' } },
                    next),
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    expectedVersionId = data.VersionId;
                    return next();
                }),
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Suspended' } },
                    next),
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: null,
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                next => s3.deleteObject({ Bucket: bucket, Key: keyName, VersionId: 'null' }, next),
                next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                next => s3.headObject({ Bucket: bucket, Key: keyName }, next),
                next => s3.listObjectVersions({ Bucket: bucket }, next),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }
                const headObjectRes = data[7];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[8];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;

                assert.strictEqual(DeleteMarkers.length, 0);
                assert.strictEqual(Versions.length, 2);

                const currentVersion = Versions.find(v => v.IsLatest);
                assertVersionIsNullAndUpdated(currentVersion);

                const nonCurrentVersion = Versions.find(v => !v.IsLatest);
                assertVersionHasNotBeenUpdated(nonCurrentVersion, expectedVersionId);

                // give some time for the async deletes to complete
                return setTimeout(() => checkVersionData(s3, bucket, keyName, expectedVersionId, testData, done),
                       1000);
            });
        });

        it('should update null version with no version id and versioning suspended', done => {
            let objMD;
            return async.series([
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Suspended' } },
                    next),
                next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                next => s3.headObject({ Bucket: bucket, Key: keyName }, next),
                next => s3.listObjectVersions({ Bucket: bucket }, next),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }
                const headObjectRes = data[4];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[5];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;
                assert.strictEqual(DeleteMarkers.length, 0);
                assert.strictEqual(Versions.length, 1);

                const currentVersion = Versions[0];
                assert(currentVersion.IsLatest);
                assertVersionIsNullAndUpdated(currentVersion);

                return done();
            });
        });

        it('should update null version if versioning suspended and null version has a version id', done => {
            let objMD;
            return async.series([
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Suspended' } },
                    next),
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                next => s3.headObject({ Bucket: bucket, Key: keyName, VersionId: 'null' }, next),
                next => s3.listObjectVersions({ Bucket: bucket }, next),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }

                const headObjectRes = data[4];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[5];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;
                assert.strictEqual(Versions.length, 1);
                assert.strictEqual(DeleteMarkers.length, 0);

                const currentVersion = Versions[0];
                assert(currentVersion.IsLatest);
                assertVersionIsNullAndUpdated(currentVersion);
                return done();
            });
        });

        it('should update null version if versioning suspended and null version has a version id and' +
        'put object afterward', done => {
            let objMD;
            return async.series([
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Suspended' } },
                    next),
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                next => s3.headObject({ Bucket: bucket, Key: keyName, VersionId: 'null' }, next),
                next => s3.listObjectVersions({ Bucket: bucket }, next),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }

                const headObjectRes = data[5];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert(!headObjectRes.StorageClass);

                const listObjectVersionsRes = data[6];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;
                assert.strictEqual(DeleteMarkers.length, 0);
                assert.strictEqual(Versions.length, 1);

                const currentVersion = Versions[0];
                assert(currentVersion.IsLatest);
                assertVersionHasNotBeenUpdated(currentVersion, 'null');
                return done();
            });
        });

        it('should update null version if versioning suspended and null version has a version id and' +
        'put version afterward', done => {
            let objMD;
            let expectedVersionId;
            return async.series([
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Suspended' } },
                    next),
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Enabled' } },
                    next),
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    expectedVersionId = data.VersionId;
                    return next();
                }),
                next => s3.headObject({ Bucket: bucket, Key: keyName, VersionId: 'null' }, next),
                next => s3.listObjectVersions({ Bucket: bucket }, next),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }

                const headObjectRes = data[6];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[7];
                const { Versions } = listObjectVersionsRes;
                assert.strictEqual(Versions.length, 2);

                const [currentVersion] = Versions.filter(v => v.IsLatest);
                assertVersionHasNotBeenUpdated(currentVersion, expectedVersionId);

                const [nonCurrentVersion] = Versions.filter(v => !v.IsLatest);
                assertVersionIsNullAndUpdated(nonCurrentVersion);
                return done();
            });
        });

        it('should update non-current null version if versioning suspended', done => {
            let expectedVersionId;
            let objMD;
            return async.series([
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Enabled' } },
                    next),
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    expectedVersionId = data.VersionId;
                    return next();
                }),
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Suspended' } },
                    next),
                next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                next => s3.headObject({ Bucket: bucket, Key: keyName, VersionId: 'null' }, next),
                next => s3.listObjectVersions({ Bucket: bucket }, next),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }

                const headObjectRes = data[6];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[7];
                const deleteMarkers = listObjectVersionsRes.DeleteMarkers;
                assert.strictEqual(deleteMarkers.length, 0);
                const { Versions } = listObjectVersionsRes;
                assert.strictEqual(Versions.length, 2);

                const [currentVersion] = Versions.filter(v => v.IsLatest);
                assertVersionHasNotBeenUpdated(currentVersion, expectedVersionId);

                const [nonCurrentVersion] = Versions.filter(v => !v.IsLatest);
                assertVersionIsNullAndUpdated(nonCurrentVersion);

                return done();
            });
        });

        it('should update current null version if versioning suspended', done => {
            let objMD;
            let expectedVersionId;
            return async.series([
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Enabled' } },
                    next),
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    expectedVersionId = data.VersionId;
                    return next();
                }),
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Suspended' } },
                    next),
                next => s3.deleteObject({ Bucket: bucket, Key: keyName, VersionId: expectedVersionId }, next),
                next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                next => s3.headObject({ Bucket: bucket, Key: keyName, VersionId: 'null' }, next),
                next => s3.listObjectVersions({ Bucket: bucket }, next),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }

                const headObjectRes = data[7];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[8];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;
                assert.strictEqual(Versions.length, 1);
                assert.strictEqual(DeleteMarkers.length, 0);

                const currentVersion = Versions[0];
                assert(currentVersion.IsLatest);
                assertVersionIsNullAndUpdated(currentVersion);
                return done();
            });
        });

        it('should update current null version if versioning suspended and put a null version ' +
        'afterwards', done => {
            let objMD;
            let deletedVersionId;
            return async.series([
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Enabled' } },
                    next),
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    deletedVersionId = data.VersionId;
                    return next();
                }),
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Suspended' } },
                    next),
                next => s3.deleteObject({ Bucket: bucket, Key: keyName, VersionId: deletedVersionId }, next),
                next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                next => s3.headObject({ Bucket: bucket, Key: keyName, VersionId: 'null' }, next),
                next => s3.listObjectVersions({ Bucket: bucket }, next),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }

                const headObjectRes = data[8];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert(!headObjectRes.StorageClass);

                const listObjectVersionsRes = data[9];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;
                assert.strictEqual(DeleteMarkers.length, 0);
                assert.strictEqual(Versions.length, 1);

                const currentVersion = Versions[0];
                assert(currentVersion.IsLatest);
                assertVersionHasNotBeenUpdated(currentVersion, 'null');

                return done();
            });
        });

        it('should update current null version if versioning suspended and put a version afterwards', done => {
            let objMD;
            let deletedVersionId;
            let expectedVersionId;
            return async.series([
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, next),
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Enabled' } },
                    next),
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    deletedVersionId = data.VersionId;
                    return next();
                }),
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Suspended' } },
                    next),
                next => s3.deleteObject({ Bucket: bucket, Key: keyName, VersionId: deletedVersionId }, next),
                next => makeBackbeatRequest({
                    method: 'GET',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    const { error, result } = updateStorageClass(data, storageClass);
                    if (error) {
                        return next(error);
                    }
                    objMD = result;
                    return next();
                }),
                next => makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: objMD,
                }, next),
                next => s3.putBucketVersioning({ Bucket: bucket, VersioningConfiguration: { Status: 'Enabled' } },
                    next),
                next => s3.putObject({ Bucket: bucket, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    expectedVersionId = data.VersionId;
                    return next();
                }),
                next => s3.headObject({ Bucket: bucket, Key: keyName, VersionId: 'null' }, next),
                next => s3.listObjectVersions({ Bucket: bucket }, next),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }

                const headObjectRes = data[9];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[10];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;
                assert.strictEqual(DeleteMarkers.length, 0);
                assert.strictEqual(Versions.length, 2);

                const [currentVersion] = Versions.filter(v => v.IsLatest);
                assertVersionHasNotBeenUpdated(currentVersion, expectedVersionId);

                const [nonCurrentVersion] = Versions.filter(v => !v.IsLatest);
                assertVersionIsNullAndUpdated(nonCurrentVersion);

                return done();
            });
        });
    });

    // TODO: CLDSRV-394 unskip routeBackbeat tests
    describe.skip('backbeat PUT routes', () => {
        describe('PUT data + metadata should create a new complete object',
        () => {
            [{
                caption: 'with ascii test key',
                key: testKey, encodedKey: testKey,
            },
            {
                caption: 'with UTF8 key',
                key: testKeyUTF8, encodedKey: encodeURI(testKeyUTF8),
            },
            {
                caption: 'with percents and spaces encoded as \'+\' in key',
                key: '50% full or 50% empty',
                encodedKey: '50%25+full+or+50%25+empty',
            },
            {
                caption: 'with legacy API v1',
                key: testKey, encodedKey: testKey,
                legacyAPI: true,
            },
            {
                caption: 'with encryption configuration',
                key: testKey, encodedKey: testKey,
                encryption: true,
            },
            {
                caption: 'with encryption configuration and legacy API v1',
                key: testKey, encodedKey: testKey,
                encryption: true,
                legacyAPI: true,
            }].concat([
                `${testKeyUTF8}/${testKeyUTF8}/%42/mykey`,
                'Pâtisserie=中文-español-English',
                'notes/spring/1.txt',
                'notes/spring/2.txt',
                'notes/spring/march/1.txt',
                'notes/summer/1.txt',
                'notes/summer/2.txt',
                'notes/summer/august/1.txt',
                'notes/year.txt',
                'notes/yore.rs',
                'notes/zaphod/Beeblebrox.txt',
            ].map(key => ({
                key, encodedKey: encodeURI(key),
                caption: `with key ${key}`,
            })))
            .forEach(testCase => {
                it(testCase.caption, done => {
                    async.waterfall([next => {
                        const queryObj = testCase.legacyAPI ? {} : { v2: '' };
                        makeBackbeatRequest({
                            method: 'PUT', bucket: testCase.encryption ?
                                TEST_ENCRYPTED_BUCKET : TEST_BUCKET,
                            objectKey: testCase.encodedKey,
                            resourceType: 'data',
                            queryObj,
                            headers: {
                                'content-length': testData.length,
                                'x-scal-canonical-id': testArn,
                            },
                            authCredentials: backbeatAuthCredentials,
                            requestBody: testData }, next);
                    }, (response, next) => {
                        assert.strictEqual(response.statusCode, 200);
                        const newMd = getMetadataToPut(response);
                        if (testCase.encryption && !testCase.legacyAPI) {
                            assert.strictEqual(typeof newMd.location[0].cryptoScheme, 'number');
                            assert.strictEqual(typeof newMd.location[0].cipheredDataKey, 'string');
                        } else {
                            // if no encryption or legacy API, data should not be encrypted
                            assert.strictEqual(newMd.location[0].cryptoScheme, undefined);
                            assert.strictEqual(newMd.location[0].cipheredDataKey, undefined);
                        }
                        makeBackbeatRequest({
                            method: 'PUT', bucket: testCase.encryption ?
                                TEST_ENCRYPTED_BUCKET : TEST_BUCKET,
                            objectKey: testCase.encodedKey,
                            resourceType: 'metadata',
                            authCredentials: backbeatAuthCredentials,
                            requestBody: JSON.stringify(newMd),
                        }, next);
                    }, (response, next) => {
                        assert.strictEqual(response.statusCode, 200);
                        checkObjectData(
                            s3, testCase.encryption ? TEST_ENCRYPTED_BUCKET : TEST_BUCKET,
                            testCase.key, testData, next);
                    }], err => {
                        assert.ifError(err);
                        done();
                    });
                });
            });
        });

        it('should PUT metadata for a non-versioned bucket', done => {
            const bucket = NONVERSIONED_BUCKET;
            const objectKey = 'non-versioned-key';
            async.waterfall([
                next =>
                    makeBackbeatRequest({
                        method: 'PUT',
                        bucket,
                        objectKey,
                        resourceType: 'data',
                        queryObj: { v2: '' },
                        headers: {
                            'content-length': testData.length,
                            'content-md5': testDataMd5,
                            'x-scal-canonical-id': testArn,
                        },
                        authCredentials: backbeatAuthCredentials,
                        requestBody: testData,
                    }, (err, response) => {
                        assert.ifError(err);
                        const metadata = Object.assign({}, nonVersionedTestMd, {
                            location: JSON.parse(response.body),
                        });
                        return next(null, metadata);
                    }),
                (metadata, next) =>
                    makeBackbeatRequest({
                        method: 'PUT',
                        bucket,
                        objectKey,
                        resourceType: 'metadata',
                        authCredentials: backbeatAuthCredentials,
                        requestBody: JSON.stringify(metadata),
                    }, (err, response) => {
                        assert.ifError(err);
                        assert.strictEqual(response.statusCode, 200);
                        next();
                    }),
                next =>
                    s3.headObject({
                        Bucket: bucket,
                        Key: objectKey,
                    }, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data.StorageClass, 'awsbackend');
                        next();
                    }),
                next => checkObjectData(s3, bucket, objectKey, testData, next),
            ], done);
        });

        it('PUT metadata with "x-scal-replication-content: METADATA"' +
        'header should replicate metadata only', done => {
            async.waterfall([next => {
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_ENCRYPTED_BUCKET,
                    objectKey: 'test-updatemd-key',
                    resourceType: 'data',
                    queryObj: { v2: '' },
                    headers: {
                        'content-length': testData.length,
                        'x-scal-canonical-id': testArn,
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: testData,
                }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                const newMd = getMetadataToPut(response);
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_ENCRYPTED_BUCKET,
                    objectKey: 'test-updatemd-key',
                    resourceType: 'metadata',
                    authCredentials: backbeatAuthCredentials,
                    requestBody: JSON.stringify(newMd),
                }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                // Don't update the sent metadata since it is sent by
                // backbeat as received from the replication queue,
                // without updated data location or encryption info
                // (since that info is not known by backbeat)
                const newMd = Object.assign({}, testMd);
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_ENCRYPTED_BUCKET,
                    objectKey: 'test-updatemd-key',
                    resourceType: 'metadata',
                    headers: { 'x-scal-replication-content': 'METADATA' },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: JSON.stringify(newMd),
                }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                checkObjectData(s3, TEST_ENCRYPTED_BUCKET, 'test-updatemd-key',
                    testData, next);
            }], err => {
                assert.ifError(err);
                done();
            });
        });

        it('should PUT tags for a non-versioned bucket', function test(done) {
            this.timeout(10000);
            const bucket = NONVERSIONED_BUCKET;
            const awsBucket =
                  config.locationConstraints[awsLocation].details.bucketName;
            const awsKey = uuidv4();
            async.waterfall([
                next =>
                    makeBackbeatRequest({
                        method: 'PUT',
                        bucket,
                        objectKey: awsKey,
                        resourceType: 'multiplebackenddata',
                        queryObj: { operation: 'putobject' },
                        headers: {
                            'content-length': testData.length,
                            'x-scal-canonical-id': testArn,
                            'x-scal-storage-type': 'aws_s3',
                            'x-scal-storage-class': awsLocation,
                            'x-scal-tags': JSON.stringify({ Key1: 'Value1' }),
                        },
                        authCredentials: backbeatAuthCredentials,
                        requestBody: testData,
                    }, (err, response) => {
                        assert.ifError(err);
                        assert.strictEqual(response.statusCode, 200);
                        return next();
                    }),
                next =>
                    awsClient.getObjectTagging({
                        Bucket: awsBucket,
                        Key: awsKey,
                    }, (err, data) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(data.TagSet, [{
                            Key: 'Key1',
                            Value: 'Value1'
                        }]);
                        next();
                    }),
            ], done);
        });

        it('should refuse PUT data if no x-scal-canonical-id header ' +
           'is provided', done => makeBackbeatRequest({
               method: 'PUT', bucket: TEST_BUCKET,
               objectKey: testKey, resourceType: 'data',
               queryObj: { v2: '' },
               headers: {
                   'content-length': testData.length,
               },
               authCredentials: backbeatAuthCredentials,
               requestBody: testData,
           },
           err => {
               assert.strictEqual(err.code, 'BadRequest');
               done();
           }));

        it('should refuse PUT in metadata-only mode if object does not exist',
        done => {
            async.waterfall([next => {
                const newMd = Object.assign({}, testMd);
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: 'does-not-exist',
                    resourceType: 'metadata',
                    headers: { 'x-scal-replication-content': 'METADATA' },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: JSON.stringify(newMd),
                }, next);
            }], err => {
                assert.strictEqual(err.statusCode, 404);
                done();
            });
        });

        it('should remove old object data locations if version is overwritten ' +
        'with same contents', done => {
            let oldLocation;
            const testKeyOldData = `${testKey}-old-data`;
            async.waterfall([next => {
                // put object's data locations
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: testKey,
                    resourceType: 'data',
                    headers: {
                        'content-length': testData.length,
                        'x-scal-canonical-id': testArn,
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: testData }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                // put object metadata
                const newMd = Object.assign({}, testMd);
                newMd.location = JSON.parse(response.body);
                oldLocation = newMd.location;
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: testKey,
                    resourceType: 'metadata',
                    queryObj: {
                        versionId: versionIdUtils.encode(testMd.versionId),
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: JSON.stringify(newMd),
                }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                // put another object which metadata reference the
                // same data locations, we will attempt to retrieve
                // this object at the end of the test to confirm that
                // its locations have been deleted
                const oldDataMd = Object.assign({}, testMd);
                oldDataMd.location = oldLocation;
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: testKeyOldData,
                    resourceType: 'metadata',
                    authCredentials: backbeatAuthCredentials,
                    requestBody: JSON.stringify(oldDataMd),
                }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                // create new data locations
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: testKey,
                    resourceType: 'data',
                    headers: {
                        'content-length': testData.length,
                        'x-scal-canonical-id': testArn,
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: testData }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                // overwrite the original object version, now
                // with references to the new data locations
                const newMd = Object.assign({}, testMd);
                newMd.location = JSON.parse(response.body);
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: testKey,
                    resourceType: 'metadata',
                    queryObj: {
                        versionId: versionIdUtils.encode(testMd.versionId),
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: JSON.stringify(newMd),
                }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                // give some time for the async deletes to complete
                setTimeout(() => checkObjectData(s3, TEST_BUCKET, testKey, testData, next),
                           1000);
            }, next => {
                // check that the object copy referencing the old data
                // locations is unreadable, confirming that the old
                // data locations have been deleted
                s3.getObject({
                    Bucket: TEST_BUCKET,
                    Key: testKeyOldData,
                }, err => {
                    assert(err, 'expected error to get object with old data ' +
                           'locations, got success');
                    next();
                });
            }], err => {
                assert.ifError(err);
                done();
            });
        });

        it('should remove old object data locations if version is overwritten ' +
        'with empty contents', done => {
            let oldLocation;
            const testKeyOldData = `${testKey}-old-data`;
            async.waterfall([next => {
                // put object's data locations
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: testKey,
                    resourceType: 'data',
                    headers: {
                        'content-length': testData.length,
                        'x-scal-canonical-id': testArn,
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: testData }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                // put object metadata
                const newMd = Object.assign({}, testMd);
                newMd.location = JSON.parse(response.body);
                oldLocation = newMd.location;
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: testKey,
                    resourceType: 'metadata',
                    queryObj: {
                        versionId: versionIdUtils.encode(testMd.versionId),
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: JSON.stringify(newMd),
                }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                // put another object which metadata reference the
                // same data locations, we will attempt to retrieve
                // this object at the end of the test to confirm that
                // its locations have been deleted
                const oldDataMd = Object.assign({}, testMd);
                oldDataMd.location = oldLocation;
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: testKeyOldData,
                    resourceType: 'metadata',
                    queryObj: {
                        versionId: versionIdUtils.encode(testMd.versionId),
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: JSON.stringify(oldDataMd),
                }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                // overwrite the original object version with an empty location
                const newMd = Object.assign({}, testMd);
                newMd['content-length'] = 0;
                newMd['content-md5'] = emptyContentsMd5;
                newMd.location = null;
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: testKey,
                    resourceType: 'metadata',
                    queryObj: {
                        versionId: versionIdUtils.encode(testMd.versionId),
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: JSON.stringify(newMd),
                }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                // give some time for the async deletes to complete
                setTimeout(() => checkObjectData(s3, TEST_BUCKET, testKey, '', next),
                           1000);
            }, next => {
                // check that the object copy referencing the old data
                // locations is unreadable, confirming that the old
                // data locations have been deleted
                s3.getObject({
                    Bucket: TEST_BUCKET,
                    Key: testKeyOldData,
                }, err => {
                    assert(err, 'expected error to get object with old data ' +
                           'locations, got success');
                    next();
                });
            }], err => {
                assert.ifError(err);
                done();
            });
        });

        it('should not remove data locations on replayed metadata PUT',
        done => {
            let serializedNewMd;
            async.waterfall([next => {
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: testKey,
                    resourceType: 'data',
                    headers: {
                        'content-length': testData.length,
                        'x-scal-canonical-id': testArn,
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: testData }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                const newMd = Object.assign({}, testMd);
                newMd.location = JSON.parse(response.body);
                serializedNewMd = JSON.stringify(newMd);
                async.timesSeries(2, (i, putDone) => makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: testKey,
                    resourceType: 'metadata',
                    queryObj: {
                        versionId: versionIdUtils.encode(testMd.versionId),
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: serializedNewMd,
                }, (err, response) => {
                    assert.ifError(err);
                    assert.strictEqual(response.statusCode, 200);
                    putDone(err);
                }), () => next());
            }, next => {
                // check that the object is still readable to make
                // sure we did not remove the data keys
                s3.getObject({
                    Bucket: TEST_BUCKET,
                    Key: testKey,
                }, (err, data) => {
                    assert.ifError(err);
                    assert.strictEqual(data.Body.toString(), testData);
                    next();
                });
            }], err => {
                assert.ifError(err);
                done();
            });
        });

        it('should create a new version when no versionId is passed in query string', done => {
            let newVersion;
            async.waterfall([next => {
                // put object's data locations
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: testKey,
                    resourceType: 'data',
                    headers: {
                        'content-length': testData.length,
                        'x-scal-canonical-id': testArn,
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: testData }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                // put object metadata
                const oldMd = Object.assign({}, testMd);
                oldMd.location = JSON.parse(response.body);
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: testKey,
                    resourceType: 'metadata',
                    queryObj: {
                        versionId: versionIdUtils.encode(testMd.versionId),
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: JSON.stringify(oldMd),
                }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                const parsedResponse = JSON.parse(response.body);
                assert.strictEqual(parsedResponse.versionId, testMd.versionId);
                // create new data locations
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: testKey,
                    resourceType: 'data',
                    headers: {
                        'content-length': testData.length,
                        'x-scal-canonical-id': testArn,
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: testData }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                // create a new version with the new data locations,
                // not passing 'versionId' in the query string
                const newMd = Object.assign({}, testMd);
                newMd.location = JSON.parse(response.body);
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: testKey,
                    resourceType: 'metadata',
                    authCredentials: backbeatAuthCredentials,
                    requestBody: JSON.stringify(newMd),
                }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                const parsedResponse = JSON.parse(response.body);
                newVersion = parsedResponse.versionId;
                assert.notStrictEqual(newVersion, testMd.versionId);
                // give some time for the async deletes to complete,
                // then check that we can read the latest version
                setTimeout(() => s3.getObject({
                    Bucket: TEST_BUCKET,
                    Key: testKey,
                }, (err, data) => {
                    assert.ifError(err);
                    assert.strictEqual(data.Body.toString(), testData);
                    next();
                }), 1000);
            }, next => {
                // check that the previous object version is still readable
                s3.getObject({
                    Bucket: TEST_BUCKET,
                    Key: testKey,
                    VersionId: versionIdUtils.encode(testMd.versionId),
                }, (err, data) => {
                    assert.ifError(err);
                    assert.strictEqual(data.Body.toString(), testData);
                    next();
                });
            }], err => {
                assert.ifError(err);
                done();
            });
        });
    });
    describe.skip('backbeat authorization checks', () => {
        [{ method: 'PUT', resourceType: 'metadata' },
         { method: 'PUT', resourceType: 'data' }].forEach(test => {
             const queryObj = test.resourceType === 'data' ? { v2: '' } : {};
             it(`${test.method} ${test.resourceType} should respond with ` +
             '403 Forbidden if no credentials are provided',
             done => {
                 makeBackbeatRequest({
                     method: test.method, bucket: TEST_BUCKET,
                     objectKey: TEST_KEY, resourceType: test.resourceType,
                     queryObj,
                 },
                 err => {
                     assert(err);
                     assert.strictEqual(err.statusCode, 403);
                     assert.strictEqual(err.code, 'AccessDenied');
                     done();
                 });
             });
             it(`${test.method} ${test.resourceType} should respond with ` +
                '403 Forbidden if wrong credentials are provided',
                done => {
                    makeBackbeatRequest({
                        method: test.method, bucket: TEST_BUCKET,
                        objectKey: TEST_KEY, resourceType: test.resourceType,
                        queryObj,
                        authCredentials: {
                            accessKey: 'wrong',
                            secretKey: 'still wrong',
                        },
                    },
                    err => {
                        assert(err);
                        assert.strictEqual(err.statusCode, 403);
                        assert.strictEqual(err.code, 'InvalidAccessKeyId');
                        done();
                    });
                });
             it(`${test.method} ${test.resourceType} should respond with ` +
                '403 Forbidden if the account does not match the ' +
                'backbeat user',
                done => {
                    makeBackbeatRequest({
                        method: test.method, bucket: TEST_BUCKET,
                        objectKey: TEST_KEY, resourceType: test.resourceType,
                        queryObj,
                        authCredentials: {
                            accessKey: 'accessKey2',
                            secretKey: 'verySecretKey2',
                        },
                    },
                    err => {
                        assert(err);
                        assert.strictEqual(err.statusCode, 403);
                        assert.strictEqual(err.code, 'AccessDenied');
                        done();
                    });
                });
             it(`${test.method} ${test.resourceType} should respond with ` +
                '403 Forbidden if backbeat user has wrong secret key',
                done => {
                    makeBackbeatRequest({
                        method: test.method, bucket: TEST_BUCKET,
                        objectKey: TEST_KEY, resourceType: test.resourceType,
                        queryObj,
                        authCredentials: {
                            accessKey: backbeatAuthCredentials.accessKey,
                            secretKey: 'hastalavista',
                        },
                    },
                    err => {
                        assert(err);
                        assert.strictEqual(err.statusCode, 403);
                        assert.strictEqual(err.code, 'SignatureDoesNotMatch');
                        done();
                    });
                });
         });
        it('GET  /_/backbeat/api/... should respond with ' +
           '503 on authenticated requests (API server down)',
           done => {
               const options = {
                   authCredentials: {
                       accessKey: 'accessKey2',
                       secretKey: 'verySecretKey2',
                   },
                   hostname: ipAddress,
                   port: 8000,
                   method: 'GET',
                   path: '/_/backbeat/api/crr/failed',
                   jsonResponse: true,
               };
               makeRequest(options, err => {
                   assert(err);
                   assert.strictEqual(err.statusCode, 503);
                   assert.strictEqual(err.code, 'ServiceUnavailable');
                   done();
               });
           });
        it('GET  /_/backbeat/api/... should respond with ' +
           '403 Forbidden if the request is unauthenticated',
           done => {
               const options = {
                   hostname: ipAddress,
                   port: 8000,
                   method: 'GET',
                   path: '/_/backbeat/api/crr/failed',
                   jsonResponse: true,
               };
               makeRequest(options, err => {
                   assert(err);
                   assert.strictEqual(err.statusCode, 403);
                   assert.strictEqual(err.code, 'AccessDenied');
                   done();
               });
           });
    });

    describe.skip('GET Metadata route', () => {
        beforeEach(done => makeBackbeatRequest({
            method: 'PUT', bucket: TEST_BUCKET,
            objectKey: TEST_KEY,
            resourceType: 'metadata',
            queryObj: {
                versionId: versionIdUtils.encode(testMd.versionId),
            },
            authCredentials: backbeatAuthCredentials,
            requestBody: JSON.stringify(testMd),
        }, done));

        it('should return metadata blob for a versionId', done => {
            makeBackbeatRequest({
                method: 'GET', bucket: TEST_BUCKET,
                objectKey: TEST_KEY, resourceType: 'metadata',
                authCredentials: backbeatAuthCredentials,
                queryObj: {
                    versionId: versionIdUtils.encode(testMd.versionId),
                },
            }, (err, data) => {
                const parsedBody = JSON.parse(JSON.parse(data.body).Body);
                assert.strictEqual(data.statusCode, 200);
                assert.deepStrictEqual(parsedBody, testMd);
                done();
            });
        });

        it('should return error if bucket does not exist', done => {
            makeBackbeatRequest({
                method: 'GET', bucket: 'blah',
                objectKey: TEST_KEY, resourceType: 'metadata',
                authCredentials: backbeatAuthCredentials,
                queryObj: {
                    versionId: versionIdUtils.encode(testMd.versionId),
                },
            }, (err, data) => {
                assert.strictEqual(data.statusCode, 404);
                assert.strictEqual(JSON.parse(data.body).code, 'NoSuchBucket');
                done();
            });
        });

        it('should return error if object does not exist', done => {
            makeBackbeatRequest({
                method: 'GET', bucket: TEST_BUCKET,
                objectKey: 'blah', resourceType: 'metadata',
                authCredentials: backbeatAuthCredentials,
                queryObj: {
                    versionId: versionIdUtils.encode(testMd.versionId),
                },
            }, (err, data) => {
                assert.strictEqual(data.statusCode, 404);
                assert.strictEqual(JSON.parse(data.body).code, 'ObjNotFound');
                done();
            });
        });
    });
    describe.skip('backbeat multipart upload operations', function test() {
        this.timeout(10000);

        // The ceph image does not support putting tags during initiate MPU.
        itSkipCeph('should put tags if the source is AWS and tags are ' +
        'provided when initiating the multipart upload', done => {
            const awsBucket =
                config.locationConstraints[awsLocation].details.bucketName;
            const awsKey = uuidv4();
            const multipleBackendPath =
                `/_/backbeat/multiplebackenddata/${awsBucket}/${awsKey}`;
            let uploadId;
            let partData;
            async.series([
                next =>
                    makeRequest({
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path: multipleBackendPath,
                        queryObj: { operation: 'initiatempu' },
                        headers: {
                            'x-scal-storage-class': awsLocation,
                            'x-scal-storage-type': 'aws_s3',
                            'x-scal-tags': JSON.stringify({ 'key1': 'value1' }),
                        },
                        jsonResponse: true,
                    }, (err, data) => {
                        if (err) {
                            return next(err);
                        }
                        uploadId = JSON.parse(data.body).uploadId;
                        return next();
                    }),
                next =>
                    makeRequest({
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'PUT',
                        path: multipleBackendPath,
                        queryObj: { operation: 'putpart' },
                        headers: {
                            'x-scal-storage-class': awsLocation,
                            'x-scal-storage-type': 'aws_s3',
                            'x-scal-upload-id': uploadId,
                            'x-scal-part-number': '1',
                            'content-length': testData.length,
                        },
                        requestBody: testData,
                        jsonResponse: true,
                    },  (err, data) => {
                        if (err) {
                            return next(err);
                        }
                        const body = JSON.parse(data.body);
                        partData = [{
                            PartNumber: [body.partNumber],
                            ETag: [body.ETag],
                        }];
                        return next();
                    }),
                next =>
                    makeRequest({
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path: multipleBackendPath,
                        queryObj: { operation: 'completempu' },
                        headers: {
                            'x-scal-storage-class': awsLocation,
                            'x-scal-storage-type': 'aws_s3',
                            'x-scal-upload-id': uploadId,
                        },
                        requestBody: JSON.stringify(partData),
                        jsonResponse: true,
                    }, next),
                next =>
                    awsClient.getObjectTagging({
                        Bucket: awsBucket,
                        Key: awsKey,
                    }, (err, data) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(data.TagSet, [{
                            Key: 'key1',
                            Value: 'value1',
                        }]);
                        next();
                    }),
            ], done);
        });
        it('should put tags if the source is Azure and tags are provided ' +
        'when completing the multipart upload', done => {
            const containerName = getAzureContainerName(azureLocation);
            const blob = uuidv4();
            const multipleBackendPath =
                `/_/backbeat/multiplebackenddata/${containerName}/${blob}`;
            const uploadId = uuidv4().replace(/-/g, '');
            let partData;
            async.series([
                next =>
                    makeRequest({
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'PUT',
                        path: multipleBackendPath,
                        queryObj: { operation: 'putpart' },
                        headers: {
                            'x-scal-storage-class': azureLocation,
                            'x-scal-storage-type': 'azure',
                            'x-scal-upload-id': uploadId,
                            'x-scal-part-number': '1',
                            'content-length': testData.length,
                        },
                        requestBody: testData,
                        jsonResponse: true,
                    },  (err, data) => {
                        if (err) {
                            return next(err);
                        }
                        const body = JSON.parse(data.body);
                        partData = [{
                            PartNumber: [body.partNumber],
                            ETag: [body.ETag],
                            NumberSubParts: [body.numberSubParts],
                        }];
                        return next();
                    }),
                next =>
                    makeRequest({
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path: multipleBackendPath,
                        queryObj: { operation: 'completempu' },
                        headers: {
                            'x-scal-storage-class': azureLocation,
                            'x-scal-storage-type': 'azure',
                            'x-scal-upload-id': uploadId,
                            'x-scal-tags': JSON.stringify({ 'key1': 'value1' }),
                        },
                        requestBody: JSON.stringify(partData),
                        jsonResponse: true,
                    }, next),
                next =>
                    azureClient.getBlobProperties(
                        containerName, blob, (err, result) => {
                            if (err) {
                                return next(err);
                            }
                            const tags = JSON.parse(result.metadata.tags);
                            assert.deepStrictEqual(tags, { key1: 'value1' });
                            return next();
                        }),
            ], done);
        });
    });
    describe.skip('Batch Delete Route', function test() {
        this.timeout(30000);
        it('should batch delete a local location', done => {
            let versionId;
            let location;
            const testKey = 'batch-delete-test-key';

            async.series([
                done => s3.putObject({
                    Bucket: TEST_BUCKET,
                    Key: testKey,
                    Body: new Buffer('hello'),
                }, (err, data) => {
                    assert.ifError(err);
                    versionId = data.VersionId;
                    done();
                }),
                done => {
                    makeBackbeatRequest({
                        method: 'GET', bucket: TEST_BUCKET,
                        objectKey: testKey,
                        resourceType: 'metadata',
                        authCredentials: backbeatAuthCredentials,
                        queryObj: {
                            versionId,
                        },
                    }, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data.statusCode, 200);
                        const metadata = JSON.parse(
                            JSON.parse(data.body).Body);
                        location = metadata.location;
                        done();
                    });
                },
                done => {
                    const options = {
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path: '/_/backbeat/batchdelete',
                        requestBody:
                        `{"Locations":${JSON.stringify(location)}}`,
                        jsonResponse: true,
                    };
                    makeRequest(options, done);
                },
                done => s3.getObject({
                    Bucket: TEST_BUCKET,
                    Key: testKey,
                }, err => {
                    // should error out as location shall no longer exist
                    assert(err);
                    done();
                }),
            ], done);
        });
        it('should batch delete a versioned AWS location', done => {
            let versionId;
            const awsKey = `${TEST_BUCKET}/batch-delete-test-key-${makeid(8)}`;

            async.series([
                done => awsClient.putObject({
                    Bucket: awsBucket,
                    Key: awsKey,
                    Body: new Buffer('hello'),
                }, (err, data) => {
                    assert.ifError(err);
                    versionId = data.VersionId;
                    done();
                }),
                done => {
                    const location = [{
                        key: awsKey,
                        size: 5,
                        dataStoreName: awsLocation,
                        dataStoreVersionId: versionId,
                    }];
                    const reqBody = `{"Locations":${JSON.stringify(location)}}`;
                    const options = {
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path: '/_/backbeat/batchdelete',
                        requestBody: reqBody,
                        jsonResponse: true,
                    };
                    makeRequest(options, done);
                },
                done => awsClient.getObject({
                    Bucket: awsBucket,
                    Key: awsKey,
                }, err => {
                    // should error out as location shall no longer exist
                    assert(err);
                    done();
                }),
            ], done);
        });
        it('should fail with error if given malformed JSON', done => {
            async.series([
                done => {
                    const options = {
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path: '/_/backbeat/batchdelete',
                        requestBody: 'NOTJSON',
                        jsonResponse: true,
                    };
                    makeRequest(options, done);
                },
            ], err => {
                assert(err);
                done();
            });
        });
        it('should skip batch delete of a non-existent location', done => {
            async.series([
                done => {
                    const options = {
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path: '/_/backbeat/batchdelete',
                        requestBody:
                        '{"Locations":' +
                            '[{"key":"abcdef","dataStoreName":"us-east-1"}]}',
                        jsonResponse: true,
                    };
                    makeRequest(options, done);
                },
            ], done);
        });
        it('should skip batch delete of empty location array', done => {
            async.series([
                done => {
                    const options = {
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path: '/_/backbeat/batchdelete',
                        requestBody: '{"Locations":[]}',
                        jsonResponse: true,
                    };
                    makeRequest(options, done);
                },
            ], done);
        });

        it('should not put delete tags if the source is not Azure and ' +
        'if-unmodified-since header is not provided', done => {
            const awsKey = uuidv4();
            async.series([
                next =>
                    awsClient.putObject({
                        Bucket: awsBucket,
                        Key: awsKey,
                    }, next),
                next =>
                    makeRequest({
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path: '/_/backbeat/batchdelete',
                        headers: {
                            'x-scal-storage-class': awsLocation,
                            'x-scal-tags': JSON.stringify({
                                'scal-delete-marker': 'true',
                                'scal-delete-service': 'lifecycle-transition',
                            }),
                        },
                        requestBody: JSON.stringify({
                            Locations: [{
                                key: awsKey,
                                dataStoreName: awsLocation,
                            }],
                        }),
                        jsonResponse: true,
                    }, next),
                next =>
                    awsClient.getObjectTagging({
                        Bucket: awsBucket,
                        Key: awsKey,
                    }, (err, data) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(data.TagSet, []);
                        next();
                    }),
            ], done);
        });

        it('should not put tags if the source is not Azure and ' +
        'if-unmodified-since condition is not met', done => {
            const awsKey = uuidv4();
            async.series([
                next =>
                    awsClient.putObject({
                        Bucket: awsBucket,
                        Key: awsKey,
                    }, next),
                next =>
                    makeRequest({
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path: '/_/backbeat/batchdelete',
                        headers: {
                            'if-unmodified-since':
                                'Sun, 31 Mar 2019 00:00:00 GMT',
                            'x-scal-storage-class': awsLocation,
                            'x-scal-tags': JSON.stringify({
                                'scal-delete-marker': 'true',
                                'scal-delete-service': 'lifecycle-transition',
                            }),
                        },
                        requestBody: JSON.stringify({
                            Locations: [{
                                key: awsKey,
                                dataStoreName: awsLocation,
                            }],
                        }),
                        jsonResponse: true,
                    }, next),
                next =>
                    awsClient.getObjectTagging({
                        Bucket: awsBucket,
                        Key: awsKey,
                    }, (err, data) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(data.TagSet, []);
                        next();
                    }),
            ], done);
        });

        it('should put tags if the source is not Azure and ' +
        'if-unmodified-since condition is met', done => {
            const awsKey = uuidv4();
            let lastModified;
            async.series([
                next =>
                    awsClient.putObject({
                        Bucket: awsBucket,
                        Key: awsKey,
                    }, next),
                next =>
                    awsClient.headObject({
                        Bucket: awsBucket,
                        Key: awsKey,
                    }, (err, data) => {
                        if (err) {
                            return next(err);
                        }
                        lastModified = data.LastModified;
                        return next();
                    }),
                next =>
                    makeRequest({
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path: `/_/backbeat/batchdelete/${awsBucket}/${awsKey}`,
                        headers: {
                            'if-unmodified-since': lastModified,
                            'x-scal-storage-class': awsLocation,
                            'x-scal-tags': JSON.stringify({
                                'scal-delete-marker': 'true',
                                'scal-delete-service': 'lifecycle-transition',
                            }),
                        },
                        requestBody: JSON.stringify({
                            Locations: [{
                                key: awsKey,
                                dataStoreName: awsLocation,
                            }],
                        }),
                        jsonResponse: true,
                    }, next),
                next =>
                    awsClient.getObjectTagging({
                        Bucket: awsBucket,
                        Key: awsKey,
                    }, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data.TagSet.length, 2);
                        data.TagSet.forEach(tag => {
                            const { Key, Value } = tag;
                            const isValidTag =
                                Key === 'scal-delete-marker' ||
                                Key === 'scal-delete-service';
                            assert(isValidTag);
                            if (Key === 'scal-delete-marker') {
                                assert.strictEqual(Value, 'true');
                            }
                            if (Key === 'scal-delete-service') {
                                assert.strictEqual(
                                    Value, 'lifecycle-transition');
                            }
                        });
                        next();
                    }),
            ], done);
        });

        it('should not delete the object if the source is Azure and ' +
        'if-unmodified-since condition is not met', done => {
            const blob = uuidv4();
            async.series([
                next =>
                    azureClient.createBlockBlobFromText(
                        containerName, blob, 'a', null, next),
                next =>
                    makeRequest({
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path:
                            `/_/backbeat/batchdelete/${containerName}/${blob}`,
                        headers: {
                            'if-unmodified-since':
                                'Sun, 31 Mar 2019 00:00:00 GMT',
                            'x-scal-storage-class': azureLocation,
                            'x-scal-tags': JSON.stringify({
                                'scal-delete-marker': 'true',
                                'scal-delete-service': 'lifecycle-transition',
                            }),
                        },
                        requestBody: JSON.stringify({
                            Locations: [{
                                key: blob,
                                dataStoreName: azureLocation,
                            }],
                        }),
                        jsonResponse: true,
                    }, err => {
                        if (err && err.statusCode === 412) {
                            return next();
                        }
                        return next(err);
                    }),
                next =>
                    azureClient.getBlobProperties(
                        containerName, blob, (err, result) => {
                            if (err) {
                                return next(err);
                            }
                            assert(result);
                            return next();
                        }),
            ], done);
        });

        it('should delete the object if the source is Azure and ' +
        'if-unmodified-since condition is met', done => {
            const blob = uuidv4();
            let lastModified;
            async.series([
                next =>
                    azureClient.createBlockBlobFromText(
                        containerName, blob, 'a', null, next),
                next =>
                    azureClient.getBlobProperties(
                        containerName, blob, (err, result) => {
                            if (err) {
                                return next(err);
                            }
                            lastModified = result.lastModified;
                            return next();
                        }),
                next =>
                    makeRequest({
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path:
                            `/_/backbeat/batchdelete/${containerName}/${blob}`,
                        headers: {
                            'if-unmodified-since': lastModified,
                            'x-scal-storage-class': azureLocation,
                            'x-scal-tags': JSON.stringify({
                                'scal-delete-marker': 'true',
                                'scal-delete-service': 'lifecycle-transition',
                            }),
                        },
                        requestBody: JSON.stringify({
                            Locations: [{
                                key: blob,
                                dataStoreName: azureLocation,
                            }],
                        }),
                        jsonResponse: true,
                    }, next),
                next =>
                    azureClient.getBlobProperties(containerName, blob, err => {
                        assert(err.statusCode === 404);
                        return next();
                    }),
            ], done);
        });
    });
});
