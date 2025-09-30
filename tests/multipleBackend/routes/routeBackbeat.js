const assert = require('assert');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    HeadObjectCommand,
    GetObjectCommand,
    GetObjectTaggingCommand,
    PutObjectTaggingCommand,
    PutBucketEncryptionCommand,
    ListObjectVersionsCommand,
    CreateMultipartUploadCommand,
    ListMultipartUploadsCommand,
} = require('@aws-sdk/client-s3');
const async = require('async');
const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');
const { versioning } = require('arsenal');
const versionIdUtils = versioning.VersionID;

const { makeid } = require('../../unit/helpers');
const { makeRequest, makeBackbeatRequest } = require('../../functional/raw-node/utils/makeRequest');
const BucketUtility = require('../../functional/aws-node-sdk/lib/utility/bucket-util');
const {
    describeSkipIfNotMultipleOrCeph,
    itSkipCeph,
    hasLocation
} = require('../../functional/aws-node-sdk/lib/utility/test-utils');
const {
    awsLocation,
    awsS3: awsClient,
    awsBucket,
    azureLocation,
    getAzureContainerName,
    getAzureClient,
} = require('../../functional/aws-node-sdk/test/multipleBackend/utils');
const { getCredentials } = require('../../functional/aws-node-sdk/test/support/credentials');
const { config } = require('../../../lib/Config');
const azureClient = getAzureClient();
const containerName = getAzureContainerName(azureLocation);

const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';
const isNullVersionCompatMode = process.env.ENABLE_NULL_VERSION_COMPAT_MODE === 'true';

const { accessKeyId, secretAccessKey } = getCredentials();

const backbeatAuthCredentials = {
    accessKey: accessKeyId,
    secretKey: secretAccessKey,
};
const TEST_BUCKET_PREFIX = 'backbeatbucket';
const TEST_ENCRYPTED_BUCKET_PREFIX = 'backbeatbucket-encrypted';
const TEST_KEY = 'fookey';
const NONVERSIONED_BUCKET_PREFIX = 'backbeatbucket-non-versioned';
const VERSION_SUSPENDED_BUCKET_PREFIX = 'backbeatbucket-version-suspended';
const BUCKET_FOR_NULL_VERSION_PREFIX = 'backbeatbucket-null-version';

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

// S3_TESTVAL_OWNERCANONICALID variable is used by Integration that runs E2E tests with real Vault account.
if (process.env.S3_TESTVAL_OWNERCANONICALID) {
    testMd['owner-id'] = process.env.S3_TESTVAL_OWNERCANONICALID;
}

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
    s3.send(new GetObjectCommand({
        Bucket: bucket,
        Key: objectKey,
    })).then(async data => {
        try {
            const body = await data.Body.transformToString();
            assert.strictEqual(body, dataValue);
            return done();
        } catch (err) {
            return done(err);
        }
    }).catch(err => done(err));
}

function checkVersionData(s3, bucket, objectKey, versionId, dataValue, done) {
    return s3.send(new GetObjectCommand({
        Bucket: bucket,
        Key: objectKey,
        VersionId: versionId,
    })).then(async data => {
        try {
            const body = await data.Body.transformToString();
            assert.strictEqual(body, dataValue);
            return done();
        } catch (err) {
            return done(err);
        }
    }).catch(err => done(err));
}

function updateStorageClass(data, storageClass) {
    let result;
    try {
        const parsedBody = JSON.parse(JSON.parse(data.body).Body);
        parsedBody['x-amz-storage-class'] = storageClass;
        parsedBody['location'] = [];
        result = JSON.stringify(parsedBody);
    } catch (err) {
        return { error: err };
    }

    return { result };
}

function generateUniqueBucketName(prefix, suffix = uuidv4()) {
    return `${prefix}-${suffix.substring(0, 8)}`.substring(0, 63);
}
const describeIfLocationAws = hasLocation(awsLocation) ? describe : describe.skip;
const itIfLocationAwsSkipCeph = hasLocation(awsLocation) ? itSkipCeph : it.skip;
const itIfLocationAws = hasLocation(awsLocation) ? it : it.skip;
const itIfLocationAzure = hasLocation(azureLocation) ? it : it.skip;
const itSkipS3C = process.env.S3_END_TO_END ? it.skip : it;

// FIXME: does not pass for Ceph, see CLDSRV-443
describeSkipIfNotMultipleOrCeph('backbeat DELETE routes', () => {
    it('abort MPU', done => {
        const awsKey = 'backbeat-mpu-test';
        async.waterfall([
            next => {
                awsClient.send(new CreateMultipartUploadCommand({
                    Bucket: awsBucket,
                    Key: awsKey,
                })).then(response => next(null, response)).catch(err => next(err));
            },
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
            }, (UploadId, next) => {
                awsClient.send(new ListMultipartUploadsCommand({
                    Bucket: awsBucket,
                })).then(response => {
                    const hasOngoingUpload =
                        response.Uploads.some(upload => (upload === UploadId));
                    assert(!hasOngoingUpload);
                    return next();
                }).catch(err => next(err));
            },
        ], err => {
            assert.ifError(err);
            done();
        });
    });
});

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
    const suffix = uuidv4();
    // These buckets are created once before tests
    const TEST_BUCKET = generateUniqueBucketName(TEST_BUCKET_PREFIX, suffix);
    const TEST_ENCRYPTED_BUCKET = generateUniqueBucketName(TEST_ENCRYPTED_BUCKET_PREFIX, suffix);
    const NONVERSIONED_BUCKET = generateUniqueBucketName(NONVERSIONED_BUCKET_PREFIX, suffix);
    const VERSION_SUSPENDED_BUCKET = generateUniqueBucketName(VERSION_SUSPENDED_BUCKET_PREFIX, suffix);

    before(done => {
        bucketUtil = new BucketUtility('default', {});
        s3 = bucketUtil.s3;
        bucketUtil.emptyManyIfExists([TEST_BUCKET, TEST_ENCRYPTED_BUCKET, NONVERSIONED_BUCKET,
        VERSION_SUSPENDED_BUCKET])
            .then(async () => {
                try {
                    await s3.send(new CreateBucketCommand({ Bucket: TEST_BUCKET }));
                    await s3.send(new PutBucketVersioningCommand({
                        Bucket: TEST_BUCKET,
                        VersioningConfiguration: { Status: 'Enabled' },
                    }));
                    await s3.send(new CreateBucketCommand({
                        Bucket: NONVERSIONED_BUCKET,
                    }));
                    await s3.send(new CreateBucketCommand({ Bucket: VERSION_SUSPENDED_BUCKET }));
                    await s3.send(new PutBucketVersioningCommand({
                        Bucket: VERSION_SUSPENDED_BUCKET,
                        VersioningConfiguration: { Status: 'Suspended' },
                    }));
                    await s3.send(new CreateBucketCommand({ Bucket: TEST_ENCRYPTED_BUCKET }));
                    await s3.send(new PutBucketVersioningCommand({
                        Bucket: TEST_ENCRYPTED_BUCKET,
                        VersioningConfiguration: { Status: 'Enabled' },
                    }));
                    await s3.send(new PutBucketEncryptionCommand({
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
                    }));
                    done();
                } catch (err) {
                    done(err);
                }
            })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                done(err);
            });
    });

    after(async () => {
            await bucketUtil.empty(TEST_BUCKET);
            await s3.send(new DeleteBucketCommand({ Bucket: TEST_BUCKET }));
            await bucketUtil.empty(TEST_ENCRYPTED_BUCKET);
            await s3.send(new DeleteBucketCommand({ Bucket: TEST_ENCRYPTED_BUCKET }));
            await bucketUtil.empty(NONVERSIONED_BUCKET);
            await s3.send(new DeleteBucketCommand({ Bucket: NONVERSIONED_BUCKET }));
            await bucketUtil.empty(VERSION_SUSPENDED_BUCKET);
            await s3.send(new DeleteBucketCommand({ Bucket: VERSION_SUSPENDED_BUCKET }));
    });

    describe('null version', () => {
        let bucket;
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

        beforeEach(() => {
            bucket = generateUniqueBucketName(BUCKET_FOR_NULL_VERSION_PREFIX);
            return bucketUtil.emptyIfExists(bucket)
                .then(() => s3.send(new CreateBucketCommand({ Bucket: bucket })));
        });

        afterEach(() => bucketUtil.empty(bucket)
                .then(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })))
        );

        it('should update metadata of a current null version', done => {
            let objMD;
            async.series({
                putObject: next => {
                    s3.send(new PutObjectCommand({
                        Bucket: bucket,
                        Key: keyName,
                        Body: Buffer.from(testData),
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
                enableVersioningSource: next => {
                    s3.send(new PutBucketVersioningCommand({
                        Bucket: bucket,
                        VersioningConfiguration: { Status: 'Enabled' },
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
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
                headObject: next => s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    VersionId: 'null',
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
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
                listObjectVersions: next => s3.send(new ListObjectVersionsCommand({
                    Bucket: bucket,
                })).then(result => {
                    next(null, result); 
                }).catch(err => {
                    next(err);
                }),
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
                if (!isNullVersionCompatMode) {
                    expectedMd.isNull2 = true; // TODO remove the line once CLDSRV-509 is fixed
                }
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
                putObjectInitial: next => {
                    s3.send(new PutObjectCommand({
                        Bucket: bucket,
                        Key: keyName,
                        Body: Buffer.from(testData),
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
                enableVersioning: next => {
                    s3.send(new PutBucketVersioningCommand({
                        Bucket: bucket,
                        VersioningConfiguration: { Status: 'Enabled' },
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
                putObjectAgain: next => {
                    s3.send(new PutObjectCommand({
                        Bucket: bucket,
                        Key: keyName,
                        Body: Buffer.from(testData),
                    })).then(data => {
                        expectedVersionId = data.VersionId;
                        return next(null, data);
                    }).catch(err => {
                        next(err);
                    });
                },
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
                headObject: next => {
                    s3.send(new HeadObjectCommand({
                        Bucket: bucket,
                        Key: keyName,
                        VersionId: 'null',
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
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
                listObjectVersions: next => {
                    s3.send(new ListObjectVersionsCommand({
                        Bucket: bucket,
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
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

                const [nonCurrentVersion] = Versions.filter(v => !v.IsLatest);
                assertVersionIsNullAndUpdated(nonCurrentVersion);
                return done();
            });
        });

        it('should update metadata of a suspended null version', done => {
            let objMD;
            return async.series({
                suspendVersioning: next => {
                    s3.send(new PutBucketVersioningCommand({
                        Bucket: bucket,
                        VersioningConfiguration: { Status: 'Suspended' },
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
                putObject: next => {
                    s3.send(new PutObjectCommand({
                        Bucket: bucket,
                        Key: keyName,
                        Body: Buffer.from(testData),
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
                enableVersioning: next => {
                    s3.send(new PutBucketVersioningCommand({
                        Bucket: bucket,
                        VersioningConfiguration: { Status: 'Enabled' },
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
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
                headObject: next => {
                    s3.send(new HeadObjectCommand({
                        Bucket: bucket,
                        Key: keyName,
                        VersionId: 'null',
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
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
                listObjectVersions: next => {
                    s3.send(new ListObjectVersionsCommand({
                        Bucket: bucket,
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
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
                suspendVersioning: next => {
                    s3.send(new PutBucketVersioningCommand({
                        Bucket: bucket,
                        VersioningConfiguration: { Status: 'Suspended' },
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
                putObject: next => {
                    s3.send(new PutObjectCommand({
                        Bucket: bucket,
                        Key: keyName,
                        Body: Buffer.from(testData),
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
                enableVersioning: next => {
                    s3.send(new PutBucketVersioningCommand({
                        Bucket: bucket,
                        VersioningConfiguration: { Status: 'Enabled' },
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
                putObjectTagging: next => {
                    s3.send(new PutObjectTaggingCommand({
                        Bucket: bucket,
                        Key: keyName,
                        VersionId: 'null',
                        Tagging: { TagSet: [{ Key: 'key1', Value: 'value1' }] },
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
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
                headObject: next => {
                    s3.send(new HeadObjectCommand({
                        Bucket: bucket,
                        Key: keyName,
                        VersionId: 'null',
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
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
                listObjectVersions: next => {
                    s3.send(new ListObjectVersionsCommand({
                        Bucket: bucket,
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
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

                const currentVersion = Versions[0];
                assert(currentVersion.IsLatest);
                assertVersionIsNullAndUpdated(currentVersion);
                return done();
            });
        });

        it('should update metadata of a non-version object', done => {
            let objMD;
            async.series([
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
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
                next => s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new ListObjectVersionsCommand({
                    Bucket: bucket,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }

                const headObjectRes = data[3];
                assert(!headObjectRes.VersionId);
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[4];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;

                assert.strictEqual(DeleteMarkers, undefined);
                assert.strictEqual(Versions.length, 1);

                const currentVersion = Versions[0];
                assert(currentVersion.IsLatest);
                assertVersionIsNullAndUpdated(currentVersion);
                return done();
            });
        });

        it('should create a new null version if versioning suspended and no version', done => {
            let objMD;
            async.series([
                next => {
                    s3.send(new PutBucketVersioningCommand({
                        Bucket: bucket,
                        VersioningConfiguration: { Status: 'Suspended' },
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
                next => {
                    s3.send(new PutObjectCommand({
                        Bucket: bucket,
                        Key: keyName,
                        Body: Buffer.from(testData),
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
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
                next => {
                    s3.send(new DeleteObjectCommand({
                        Bucket: bucket,
                        Key: keyName,
                        VersionId: 'null',
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
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
                next => {
                    s3.send(new HeadObjectCommand({
                        Bucket: bucket,
                        Key: keyName,
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
                next => {
                    s3.send(new ListObjectVersionsCommand({ 
                        Bucket: bucket 
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
            ], (err, data) => {
                if (err) {
                    return done(err);
                }
                const headObjectRes = data[5];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[6];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;

                assert.strictEqual(DeleteMarkers, undefined);
                assert.strictEqual(Versions.length, 1);

                const currentVersion = Versions[0];
                assert(currentVersion.IsLatest);

                assertVersionIsNullAndUpdated(currentVersion);

                return done();
            });
        });

        // TODO fix broken on S3C with metadata backend,create 2 null Versions
        itSkipS3C('should create a new null version if versioning suspended and delete marker null version', done => {
            let objMD;
            return async.series([
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Suspended' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
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
                next => s3.send(new DeleteObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
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
                next => s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new ListObjectVersionsCommand({
                    Bucket: bucket
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }
                const headObjectRes = data[5];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[6];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;

                assert.strictEqual(DeleteMarkers, undefined);
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
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Enabled' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(data => {
                    expectedVersionId = data.VersionId;
                    return next(null, data);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Suspended' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
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
                next => s3.send(new DeleteObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    VersionId: 'null',
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
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
                next => s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new ListObjectVersionsCommand({ Bucket: bucket })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }
                const headObjectRes = data[7];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[8];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;

                assert.strictEqual(DeleteMarkers, undefined);
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
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Suspended' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
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
                next => s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new ListObjectVersionsCommand({
                    Bucket: bucket,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }
                const headObjectRes = data[4];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[5];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;
                assert.strictEqual(DeleteMarkers, undefined);
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
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Suspended' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
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
                next => s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    VersionId: 'null',
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new ListObjectVersionsCommand({
                    Bucket: bucket,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
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
                assert.strictEqual(DeleteMarkers, undefined);

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
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Suspended' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
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
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    VersionId: 'null',
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new ListObjectVersionsCommand({
                    Bucket: bucket,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    VersionId: 'null',
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }

                const headObjectRes = data[5];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert(!headObjectRes.StorageClass);

                const listObjectVersionsRes = data[6];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;
                assert.strictEqual(DeleteMarkers, undefined);
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
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Suspended' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
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
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Enabled' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(data => {
                    expectedVersionId = data.VersionId;
                    return next(null, data);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    VersionId: 'null',
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new ListObjectVersionsCommand({
                    Bucket: bucket,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
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
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Enabled' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(data => {
                    expectedVersionId = data.VersionId;
                    return next(null, data);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Suspended' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
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
                next => s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    VersionId: 'null',
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new ListObjectVersionsCommand({
                    Bucket: bucket,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    VersionId: 'null',
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }

                const headObjectRes = data[6];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[7];
                const deleteMarkers = listObjectVersionsRes.DeleteMarkers;
                assert.strictEqual(deleteMarkers, undefined);
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
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }), 
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Enabled' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    expectedVersionId = result.VersionId;
                    return next(null, result);
                }).catch(err => {
                    next(err);   
                }),
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Suspended' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new DeleteObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    VersionId: expectedVersionId,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
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
                next => s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    VersionId: 'null',
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new ListObjectVersionsCommand({
                    Bucket: bucket,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
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
                assert.strictEqual(DeleteMarkers, undefined);

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
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Enabled' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(data => {
                    deletedVersionId = data.VersionId;
                    return next(null, data);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Suspended' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new DeleteObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    VersionId: deletedVersionId,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
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
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    VersionId: 'null',
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new ListObjectVersionsCommand({
                    Bucket: bucket,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    VersionId: 'null',
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }

                const headObjectRes = data[8];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert(!headObjectRes.StorageClass);

                const listObjectVersionsRes = data[9];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;
                assert.strictEqual(DeleteMarkers, undefined);
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
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Enabled' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    deletedVersionId = result.VersionId;
                    return next();
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Suspended' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new DeleteObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    VersionId: deletedVersionId,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
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
                next => s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Enabled' },
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    Body: Buffer.from(testData),
                })).then(result => {
                    expectedVersionId = result.VersionId;
                    return next();
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new HeadObjectCommand({
                    Bucket: bucket,
                    Key: keyName,
                    VersionId: 'null',
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
                next => s3.send(new ListObjectVersionsCommand({
                    Bucket: bucket,
                })).then(result => {
                    next(null, result);
                }).catch(err => {
                    next(err);
                }),
            ], (err, data) => {
                if (err) {
                    return done(err);
                }
                const headObjectRes = data[9];
                assert.strictEqual(headObjectRes.VersionId, 'null');
                assert.strictEqual(headObjectRes.StorageClass, storageClass);

                const listObjectVersionsRes = data[10];
                const { DeleteMarkers, Versions } = listObjectVersionsRes;
                assert.strictEqual(DeleteMarkers, undefined);
                assert.strictEqual(Versions.length, 2);

                const [currentVersion] = Versions.filter(v => v.IsLatest);
                assertVersionHasNotBeenUpdated(currentVersion, expectedVersionId);

                const [nonCurrentVersion] = Versions.filter(v => !v.IsLatest);
                assertVersionIsNullAndUpdated(nonCurrentVersion);

                return done();
            });
        });
    });

    describe('backbeat PUT routes', () => {
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
                    s3.send(new HeadObjectCommand({
                        Bucket: bucket,
                        Key: objectKey,
                    })).then(result => {
                        assert.strictEqual(result.StorageClass, 'awsbackend');
                        next();
                    }).catch(err => {
                        next(err);
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

        itIfLocationAwsSkipCeph('should PUT tags for a non-versioned bucket (awslocation)', function test(done) {
            this.timeout(10000);
            const bucket = NONVERSIONED_BUCKET;
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
                    awsClient.send(new GetObjectTaggingCommand({
                        Bucket: awsBucket,
                        Key: awsKey,
                    })).then(data => {
                        assert.deepStrictEqual(data.TagSet, [{
                            Key: 'Key1',
                            Value: 'Value1'
                        }]);
                        next(null, data);
                    }).catch(err => {
                        next(err);
                    }),
            ], done);
        });

        const testCases = [
            {
                description: 'bucket is version suspended',
                bucket: VERSION_SUSPENDED_BUCKET,
            },
            {
                description: 'bucket is not versioned',
                bucket: NONVERSIONED_BUCKET,
            },
        ];

        testCases.forEach(({ description, bucket }) => {
            it(`should PUT metadata and data if ${description} and x-scal-versioning-required is not set`, done => {
                let objectMd;
                async.waterfall([
                    next => s3.send(new PutObjectCommand({
                        Bucket: bucket,
                        Key: 'sourcekey',
                        Body: Buffer.from(testData),
                    })).then(res => next(null, res)).catch(err => next(err)),
                    (resp, next) => makeBackbeatRequest({
                        method: 'GET',
                        resourceType: 'metadata',
                        bucket,
                        objectKey: 'sourcekey',
                        authCredentials: backbeatAuthCredentials,
                    }, (err, resp) => {
                        objectMd = JSON.parse(resp.body).Body;
                        return next();
                    }),
                    next => {
                        makeBackbeatRequest({
                            method: 'PUT', bucket,
                            objectKey: 'destinationkey',
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
                        makeBackbeatRequest({
                            method: 'PUT', bucket,
                            objectKey: 'destinationkey',
                            resourceType: 'metadata',
                            authCredentials: backbeatAuthCredentials,
                            requestBody: objectMd,
                        }, next);
                    }],
                    err => {
                        assert.ifError(err);
                        done();
                    });
            });
        });

        testCases.forEach(({ description, bucket }) => {
            it(`should refuse PUT data if ${description} and x-scal-versioning-required is true`, done => {
                makeBackbeatRequest({
                    method: 'PUT',
                    bucket,
                    objectKey: testKey,
                    resourceType: 'data',
                    queryObj: { v2: '' },
                    headers: {
                        'content-length': testData.length,
                        'x-scal-canonical-id': testArn,
                        'x-scal-versioning-required': 'true',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: testData,
                }, err => {
                    assert.strictEqual(err.code, 'InvalidBucketState');
                    done();
                });
            });
        });

        testCases.forEach(({ description, bucket }) => {
            it(`should refuse PUT metadata if ${description} and x-scal-versioning-required is true`, done => {
                makeBackbeatRequest({
                    method: 'PUT',
                    bucket,
                    objectKey: testKey,
                    resourceType: 'metadata',
                    queryObj: {
                        versionId: versionIdUtils.encode(testMd.versionId),
                    },
                    headers: {
                        'x-scal-versioning-required': 'true',
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: JSON.stringify(testMd),
                }, err => {
                    assert.strictEqual(err.code, 'InvalidBucketState');
                    done();
                });
            });
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
                s3.send(new GetObjectCommand({
                    Bucket: TEST_BUCKET,
                    Key: testKeyOldData,
                })).catch(err => {
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
                s3.send(new GetObjectCommand({
                    Bucket: TEST_BUCKET,
                    Key: testKeyOldData,
                })).catch(err => {
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
                s3.send(new GetObjectCommand({
                    Bucket: TEST_BUCKET,
                    Key: testKey,
                })).then(async data => {
                    const body = await data.Body.transformToString();
                    assert.strictEqual(body, testData);
                    next();
                }).catch(err => {
                    next(err);
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
                setTimeout(() => s3.send(new GetObjectCommand({
                    Bucket: TEST_BUCKET,
                    Key: testKey,
                })).then(async data => {
                    const body = await data.Body.transformToString();
                    assert.strictEqual(body, testData);
                    next();
                }).catch(err => {
                    next(err);
                }), 1000);
            }, next => {
                // check that the previous object version is still readable
                s3.send(new GetObjectCommand({
                    Bucket: TEST_BUCKET,
                    Key: testKey,
                    VersionId: versionIdUtils.encode(testMd.versionId),
                })).then(async data => {
                    const body = await data.Body.transformToString();
                    assert.strictEqual(body, testData);
                    next();
                }).catch(err => {
                    next(err);
                });
            }], err => {
                assert.ifError(err);
                done();
            });
        });
    });
    describe('backbeat authorization checks', () => {
        const { accessKeyId: accessKeyLisa, secretAccessKey: secretAccessKeyLisa } = getCredentials('lisa');

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
                            accessKey: accessKeyLisa,
                            secretKey: secretAccessKeyLisa,
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

        const apiProxy = !!config.backbeat;
        describe(`when api proxy is ${apiProxy ? '' : 'NOT '}configured`, () => {
            const errors = {
                403: 'AccessDenied',
                405: 'MethodNotAllowed',
                503: 'ServiceUnavailable',
            };

            it(`GET /_/backbeat/api/... should respond with ${
                apiProxy ? 503 : 405
            } on authenticated requests (API server down)`,
                done => {
                    const options = {
                        authCredentials: {
                            accessKey: accessKeyLisa,
                            secretKey: secretAccessKeyLisa,
                        },
                        hostname: ipAddress,
                        port: 8000,
                        method: 'GET',
                        path: '/_/backbeat/api/crr/failed',
                        jsonResponse: true,
                    };
                    makeRequest(options, err => {
                        assert(err);
                        const expected = apiProxy ? 503 : 405;
                        assert.strictEqual(err.statusCode, expected);
                        assert.strictEqual(err.code, errors[expected]);
                        done();
                    });
                });

            it(`GET /_/backbeat/api/... should respond with ${
                apiProxy ? 403 : 405
            } if the request is unauthenticated`,
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
                        const expected = apiProxy ? 403 : 405;
                        assert.strictEqual(err.statusCode, expected);
                        assert.strictEqual(err.code, errors[expected]);
                        done();
                    });
                });
        });
    });

    describe('GET Metadata route', () => {
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
                assert.ifError(err);
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
                const body = JSON.parse(data.body);
                assert.strictEqual(body.code, 'NoSuchBucket');
                // err is parsed data.body + statusCode
                assert.deepStrictEqual(err, { ...body, statusCode: data.statusCode });
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
                const body = JSON.parse(data.body);
                assert.strictEqual(body.code, 'ObjNotFound');
                // err is parsed data.body + statusCode
                assert.deepStrictEqual(err, { ...body, statusCode: data.statusCode });
                done();
            });
        });
    });

    describeIfLocationAws('backbeat multipart upload operations (external location)', function test() {
        this.timeout(10000);

        // The ceph image does not support putting tags during initiate MPU.
        itSkipCeph('should put tags if the source is AWS and tags are ' +
            'provided when initiating the multipart upload', done => {
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
                    awsClient.send(new GetObjectTaggingCommand({
                        Bucket: awsBucket,
                        Key: awsKey,
                    }), (err, data) => {
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
                    azureClient.getContainerClient(containerName).getBlobClient(blob).getProperties()
                        .then(result => {
                            const tags = JSON.parse(result.metadata.tags);
                            assert.deepStrictEqual(tags, { key1: 'value1' });
                            return next();
                        }, next),
            ], done);
        });
    });
    describe('Batch Delete Route', function test() {
        this.timeout(30000);
        it('should batch delete a local location', done => {
            let versionId;
            let location;
            const testKey = 'batch-delete-test-key';

            async.series([
                done => {
                    s3.send(new PutObjectCommand({
                        Bucket: TEST_BUCKET,
                        Key: testKey,
                        Body: Buffer.from('hello'),
                    })).then(data => {
                        versionId = data.VersionId;
                        done();
                    }).catch(err => {
                        done(err);
                    });
                },
                done => {
                    makeBackbeatRequest({
                        method: 'GET',
                        bucket: TEST_BUCKET,
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
                        path: `/_/backbeat/batchdelete/${TEST_BUCKET}/${testKey}`,
                        requestBody:
                        `{"Locations":${JSON.stringify(location)}}`,
                        jsonResponse: true,
                    };
                    makeRequest(options, done);
                },
                done => {
                    s3.send(new GetObjectCommand({
                        Bucket: TEST_BUCKET,
                        Key: testKey,
                    })).then(() => {
                        done(new Error('Expected error'));
                    }).catch(err => {
                        // should error out as location shall no longer exist
                        assert(err);
                        assert.strictEqual(err.$metadata.httpStatusCode, 503);
                        done();
                    });
                },
            ], done);
        });

        itIfLocationAwsSkipCeph('should batch delete a versioned AWS location', done => {
            let versionId;
            const awsKey = `${TEST_BUCKET}/batch-delete-test-key-${makeid(8)}`;

            async.series([
                done => {
                    awsClient.send(new PutObjectCommand({
                        Bucket: awsBucket,
                        Key: awsKey,
                        Body: Buffer.from('hello'),
                    })).then(data => {
                        versionId = data.VersionId;
                        done();
                    }).catch(err => {
                        done(err);
                    });
                },
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
                done => {
                    awsClient.send(new GetObjectCommand({
                        Bucket: awsBucket,
                        Key: awsKey,
                    })).then(() => {
                        done(new Error('Expected error'));
                    }).catch(err => {
                        // should error out as location shall no longer exist
                        assert(err);
                        done();
                    });
                },
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
        // TODO: unskip test when S3C-9123 is fixed
        itSkipS3C('should skip batch delete of a non-existent location', done => {
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

        itIfLocationAws('should not put delete tags if the source is not Azure and ' +
        'if-unmodified-since header is not provided', done => {
            const awsKey = uuidv4();
            async.series([
                next => {
                    awsClient.send(new PutObjectCommand({
                        Bucket: awsBucket,
                        Key: awsKey,
                    })).then(result => {
                        next(null, result);
                    }).catch(err => {
                        next(err);
                    });
                },
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
                next => {
                    awsClient.send(new GetObjectTaggingCommand({
                        Bucket: awsBucket,
                        Key: awsKey,
                    })).then(data => {
                        assert.deepStrictEqual(data.TagSet, []);
                        next(null, data);
                    }).catch(err => {
                        next(err);
                    });
                },
            ], done);
        });

        itIfLocationAwsSkipCeph('should not put tags if the source is not Azure and ' +
        'if-unmodified-since condition is not met', done => {
            const awsKey = uuidv4();
            async.series([
                next =>
                    awsClient.send(new PutObjectCommand({
                        Bucket: awsBucket,
                        Key: awsKey,
                    })).then(result => next(null, result)).catch(err => next(err)),
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
                next => {
                    awsClient.send(new GetObjectTaggingCommand({
                        Bucket: awsBucket,
                        Key: awsKey,
                    })).then(data => {
                        assert.deepStrictEqual(data.TagSet, []);
                        next();
                    }).catch(err => {
                        next(err);
                    });
                },
            ], done);
        });

        itIfLocationAwsSkipCeph('should put tags if the source is not Azure and ' +
        'if-unmodified-since condition is met', done => {
            const awsKey = uuidv4();
            let lastModified;
            async.series([
                next =>
                    awsClient.send(new PutObjectCommand({
                        Bucket: awsBucket,
                        Key: awsKey,
                    })).then(result => next(null, result)).catch(err => next(err)),
                next =>
                    awsClient.send(new HeadObjectCommand({
                        Bucket: awsBucket,
                        Key: awsKey,
                    })).then(data => {
                        lastModified = data.LastModified;
                        next(null, data);
                    }).catch(err => next(err)),
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
                    awsClient.send(new GetObjectTaggingCommand({
                        Bucket: awsBucket,
                        Key: awsKey,
                    })).then(data => {
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
                        next(null, data);
                    }).catch(err => {
                        assert.ifError(err);
                        next(err);
                    }),
            ], done);
        });

        itIfLocationAzure('should not delete the object if the source is Azure and ' +
        'if-unmodified-since condition is not met', done => {
            const blob = uuidv4();
            async.series([
                next =>
                    azureClient.getContainerClient(containerName).uploadBlockBlob(blob, 'a', 1)
                        .then(() => next(), next),
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
                    azureClient.getContainerClient(containerName).getBlobClient(blob).getProperties()
                        .then(result => {
                            assert(result);
                            return next();
                        }, next),
            ], done);
        });

        itIfLocationAzure('should delete the object if the source is Azure and ' +
        'if-unmodified-since condition is met', done => {
            const blob = uuidv4();
            let lastModified;
            async.series([
                next =>
                    azureClient.getContainerClient(containerName).uploadBlockBlob(blob, 'a', 1)
                        .then(() => next(), next),
                next =>
                    azureClient.getContainerClient(containerName).getBlobClient(blob).getProperties()
                        .then(result => {
                            lastModified = result.lastModified;
                            return next();
                        }, next),
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
                    azureClient.getContainerClient(containerName).getBlobClient(blob).getProperties()
                        .then(() => assert.fail('Expected error'), err => {
                            assert.strictEqual(err.statusCode, 404);
                            return next();
                        }),
            ], done);
        });
    });
});
