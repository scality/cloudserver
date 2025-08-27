const assert = require('assert');
const async = require('async');
const { models } = require('arsenal');
const { ObjectMD } = models;
const { v4: uuidv4 } = require('uuid');

const { makeBackbeatRequest } = require('../../functional/raw-node/utils/makeRequest');
const BucketUtility = require('../../functional/aws-node-sdk/lib/utility/bucket-util');
const { getCredentials } = require('../../functional/aws-node-sdk/test/support/credentials');

const sourceCreds = getCredentials();
const sourceAuthCredentials = {
    accessKey: sourceCreds.accessKeyId,
    secretKey: sourceCreds.secretAccessKey,
};

const destinationCreds = getCredentials('replication');
const destinationAuthCredentials = {
    accessKey: destinationCreds.accessKeyId,
    secretKey: destinationCreds.secretAccessKey,
};

// Note: for S3C tests, those conf files needs to be modified beforehand
const dstAccountInfo = require('../../../conf/authdata.json')
    .accounts.find(acc => acc.name === 'Replication');
const srcAccountInfo = require('../../../conf/authdata.json')
    .accounts.find(acc => acc.name === 'Bart');

const srcBucketUtil = new BucketUtility('default', { signatureVersion: 'v4' });
const srcS3 = srcBucketUtil.s3;

const dstBucketUtil = new BucketUtility('replication', { signatureVersion: 'v4' });
const dstS3 = dstBucketUtil.s3;

const src = {
    credentials: sourceAuthCredentials,
    bucketUtil: srcBucketUtil,
    s3: srcS3,
    accountInfo: srcAccountInfo,
};

const dst = {
    credentials: destinationAuthCredentials,
    bucketUtil: dstBucketUtil,
    s3: dstS3,
    accountInfo: dstAccountInfo,
};

const testData = 'testkey data';

// Generate unique bucket name function
function generateUniqueBucketName(prefix) {
    return `${prefix}-${uuidv4().substring(0, 8)}`;
}

function objectMDFromRequestBody(data) {
    const bodyStr = JSON.parse(data.body).Body;
    return new ObjectMD(JSON.parse(bodyStr));
}

// Backbeat updates account info in metadata to the destination account info
function objectMDWithUpdatedAccountInfo(data, dstAccountInfo = null) {
    const objMD = objectMDFromRequestBody(data);

    if (dstAccountInfo) {
        objMD
            .setOwnerDisplayName(dstAccountInfo.name)
            .setOwnerId(dstAccountInfo.canonicalID);
    }

    return objMD.getSerialized();
}

const scenarios = [
    // S3C Integration can replicate to the same account
    { name: 'same account', src, dst: src },
    { name: 'cross account', src, dst },
];

scenarios.forEach(({ name, src, dst }) => {
describe(`backbeat routes for replication (${name})`, () => {
    const { s3: srcS3, bucketUtil: srcBucketUtil, credentials: sourceAuthCredentials } = src;
    const { s3: dstS3, bucketUtil: dstBucketUtil, credentials: destinationAuthCredentials } = dst;
    const { accountInfo: dstAccountInfo } = dst;

    let bucketSource;
    let bucketDestination;
    const keyName = 'key0';
    const storageClass = 'foo';

    beforeEach(async () => {
        bucketSource = generateUniqueBucketName('backbeatbucket-replication-source');
        bucketDestination = generateUniqueBucketName('backbeatbucket-replication-destination');
        await srcBucketUtil.emptyIfExists(bucketSource);
        await srcS3.createBucket({ Bucket: bucketSource }).promise();
        await dstBucketUtil.emptyIfExists(bucketDestination);
        await dstS3.createBucket({ Bucket: bucketDestination }).promise();
    });

    afterEach(async () => {
        await srcBucketUtil.empty(bucketSource);
        await srcS3.deleteBucket({ Bucket: bucketSource }).promise();
        await dstBucketUtil.empty(bucketDestination);
        await dstS3.deleteBucket({ Bucket: bucketDestination }).promise();
    });

    it('should successfully replicate a version', done => {
        let objMD;
        let versionId;

        async.series({
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObject: next => srcS3.putObject(
                { Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    versionId = data.VersionId;
                    return next();
                }),
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMD,
            }, next),
            headObject: next => dstS3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: versionId }, next),
            listObjectVersions: next => dstS3.listObjectVersions({ Bucket: bucketDestination }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const headObjectRes = results.headObject;
            assert.strictEqual(headObjectRes.VersionId, versionId);

            const listObjectVersionsRes = results.listObjectVersions;
            const { Versions } = listObjectVersionsRes;
            assert.strictEqual(Versions.length, 1);
            assert.strictEqual(Versions[0].IsLatest, true);
            assert.strictEqual(Versions[0].VersionId, versionId);

            return done();
        });
    });

    it('should successfully replicate a version and update it', done => {
        let objMD;
        let versionId;

        async.series({
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObject: next => srcS3.putObject(
                { Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    versionId = data.VersionId;
                    return next();
                }),
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMD,
            }, next),
            updateMetadata: next => {
                const { result, error } = ObjectMD.createFromBlob(objMD);
                if (error) {
                    return next(error);
                }
                result.setTags({ foo: 'bar' });
                return makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket: bucketDestination,
                    objectKey: keyName,
                    queryObj: {
                        versionId,
                    },
                    authCredentials: destinationAuthCredentials,
                    requestBody: result.getSerialized(),
                }, next);
            },
            getObjectTagging: next => dstS3.getObjectTagging(
                { Bucket: bucketDestination, Key: keyName, VersionId: versionId }, next),
            listObjectVersions: next => dstS3.listObjectVersions({ Bucket: bucketDestination }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const getObjectTaggingRes = results.getObjectTagging;
            assert.strictEqual(getObjectTaggingRes.VersionId, versionId);
            assert.deepStrictEqual(getObjectTaggingRes.TagSet, [{ Key: 'foo', Value: 'bar' }]);

            const listObjectVersionsRes = results.listObjectVersions;
            const { Versions } = listObjectVersionsRes;
            assert.strictEqual(Versions.length, 1);
            assert.strictEqual(Versions[0].IsLatest, true);
            assert.strictEqual(Versions[0].VersionId, versionId);

            return done();
        });
    });

    it('should successfully replicate a version and update account info', done => {
        let objMD;
        let versionId;

        async.series({
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObject: next => srcS3.putObject(
                { Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    versionId = data.VersionId;
                    return next();
                }),
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                // AccountInfo not provided here as the replicateMetadata request should do it
                objMD = objectMDWithUpdatedAccountInfo(data, null);
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId,
                    // Specifying the account id in the query string
                    // should make it update the account info in the
                    // metadata to the destination account info
                    accountId: dstAccountInfo.shortid,
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMD,
            }, next),
            getDestinationMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: destinationAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                return next(null, objectMDFromRequestBody(data));
            }),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const dstObjMD = results.getDestinationMetadata;
            assert.strictEqual(dstObjMD.getOwnerDisplayName(), dstAccountInfo.name);
            assert.strictEqual(dstObjMD.getOwnerId(), dstAccountInfo.canonicalID);

            return done();
        });
    });

    it('should fail to replicate a version if the provided account is invalid', done => {
        let objMD;
        let versionId;

        async.series({
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObject: next => srcS3.putObject(
                { Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    versionId = data.VersionId;
                    return next();
                }),
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                // AccountInfo not provided here as the replicateMetadata request should do it
                objMD = objectMDWithUpdatedAccountInfo(data, null);
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId,
                    accountId: '888888888888', // Vault v1 differentiate InvalidAccountId from NoSuchEntity
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMD,
            }, next),
        }, err => {
            assert.strictEqual(err.code, process.env.S3_END_TO_END ? 'NoSuchEntity' : 'AccountNotFound');
            return done();
        });
    });

    it('should successfully replicate multiple versions and keep original order', done => {
        let objMDCurrent, objMDNonCurrent;
        let versionIdCurrent, versionIdNonCurrent;

        async.series({
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObjectNonCurrent: next => srcS3.putObject(
                { Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    versionIdNonCurrent = data.VersionId;
                    return next();
                }),
            putObjectCurrent: next => srcS3.putObject(
                { Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    versionIdCurrent = data.VersionId;
                    return next();
                }),
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadataNonCurrent: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: versionIdNonCurrent,
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMDNonCurrent = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            getMetadataCurrent: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: versionIdCurrent,
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMDCurrent = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            // replicating the objects in the reverse order
            replicateMetadataCurrent: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId: versionIdCurrent,
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMDCurrent,
            }, next),
            replicateMetadataNonCurrent: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId: versionIdNonCurrent,
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMDNonCurrent,
            }, next),
            listObjectVersions: next => dstS3.listObjectVersions({ Bucket: bucketDestination }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const listObjectVersionsRes = results.listObjectVersions;
            const { Versions } = listObjectVersionsRes;
            assert.strictEqual(Versions.length, 2);

            const [currentVersion, nonCurrentVersion] = Versions;

            assert.strictEqual(currentVersion.IsLatest, true);
            assert.strictEqual(currentVersion.VersionId, versionIdCurrent);

            assert.strictEqual(nonCurrentVersion.IsLatest, false);
            assert.strictEqual(nonCurrentVersion.VersionId, versionIdNonCurrent);

            return done();
        });
    });

    it('should successfully replicate a delete marker', done => {
        let objMDVersion, objMDDeleteMarker;
        let versionIdVersion, versionIdDeleteMarker;

        async.series({
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObject: next => srcS3.putObject(
                { Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    versionIdVersion = data.VersionId;
                    return next();
                }),
            deleteObject: next => srcS3.deleteObject(
                { Bucket: bucketSource, Key: keyName }, (err, data) => {
                    if (err) {
                        return next(err);
                    }
                    versionIdDeleteMarker = data.VersionId;
                    return next();
                }),
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadataVersion: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: versionIdVersion,
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMDVersion = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            replicateMetadataVersion: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId: versionIdVersion,
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMDVersion,
            }, next),
            getMetadataDeleteMarker: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: versionIdDeleteMarker,
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMDDeleteMarker = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            replicateMetadataDeleteMarker: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId: versionIdDeleteMarker,
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMDDeleteMarker,
            }, next),
            listObjectVersions: next => dstS3.listObjectVersions({ Bucket: bucketDestination }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const listObjectVersionsRes = results.listObjectVersions;
            const { Versions, DeleteMarkers } = listObjectVersionsRes;

            assert.strictEqual(Versions.length, 1);
            assert.strictEqual(DeleteMarkers.length, 1);

            assert.strictEqual(Versions[0].IsLatest, false);
            assert.strictEqual(Versions[0].VersionId, versionIdVersion);

            assert.strictEqual(DeleteMarkers[0].IsLatest, true);
            assert.strictEqual(DeleteMarkers[0].VersionId, versionIdDeleteMarker);

            return done();
        });
    });

    it('should successfully replicate a null version', done => {
        let objMD;

        async.series({
            putObject: next => srcS3.putObject(
                { Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, next),
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMD,
            }, next),
            headObject: next => dstS3.headObject({ Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => dstS3.listObjectVersions({ Bucket: bucketDestination }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const headObjectRes = results.headObject;
            assert.strictEqual(headObjectRes.VersionId, 'null');

            const listObjectVersionsRes = results.listObjectVersions;
            const { Versions } = listObjectVersionsRes;

            assert.strictEqual(Versions.length, 1);

            const [currentVersion] = Versions;
            assert.strictEqual(currentVersion.IsLatest, true);
            assert.strictEqual(currentVersion.VersionId, 'null');

            return done();
        });
    });

    it('should successfully replicate a suspended null version', done => {
        let objMD;

        async.series({
            suspendVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Suspended' } }, next),
            putObject: next => srcS3.putObject(
                { Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, next),
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMD,
            }, next),
            headObject: next => dstS3.headObject({ Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => dstS3.listObjectVersions({ Bucket: bucketDestination }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const headObjectRes = results.headObject;
            assert.strictEqual(headObjectRes.VersionId, 'null');

            const listObjectVersionsRes = results.listObjectVersions;
            const { Versions } = listObjectVersionsRes;

            assert.strictEqual(Versions.length, 1);

            const [currentVersion] = Versions;
            assert.strictEqual(currentVersion.IsLatest, true);
            assert.strictEqual(currentVersion.VersionId, 'null');

            return done();
        });
    });

    it('should successfully replicate a null version and update it', done => {
        let objMD;

        async.series({
            putObject: next => srcS3.putObject(
                { Bucket: bucketSource, Key: keyName, Body: Buffer.from(testData) }, next),
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMD,
            }, next),
            updateMetadata: next => {
                const { result, error } = ObjectMD.createFromBlob(objMD);
                if (error) {
                    return next(error);
                }
                result.setAmzStorageClass(storageClass);
                return makeBackbeatRequest({
                    method: 'PUT',
                    resourceType: 'metadata',
                    bucket: bucketDestination,
                    objectKey: keyName,
                    queryObj: {
                        versionId: 'null',
                    },
                    authCredentials: destinationAuthCredentials,
                    requestBody: result.getSerialized(),
                }, next);
            },
            headObject: next => dstS3.headObject({ Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => dstS3.listObjectVersions({ Bucket: bucketDestination }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const headObjectRes = results.headObject;
            assert.strictEqual(headObjectRes.VersionId, 'null');
            assert.strictEqual(headObjectRes.StorageClass, storageClass);

            const listObjectVersionsRes = results.listObjectVersions;
            const { Versions } = listObjectVersionsRes;

            assert.strictEqual(Versions.length, 1);

            const [currentVersion] = Versions;
            assert.strictEqual(currentVersion.IsLatest, true);
            assert.strictEqual(currentVersion.VersionId, 'null');
            assert.strictEqual(currentVersion.StorageClass, storageClass);

            return done();
        });
    });

    it('should successfully put object after replicating a null version', done => {
        let objMD;
        let expectedVersionId;

        async.series({
            putObjectSource: next => srcS3.putObject(
                { Bucket: bucketSource, Key: keyName, Body: Buffer.from(testData) }, next),
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMD,
            }, next),
            putObjectDestination: next => dstS3.putObject(
            { Bucket: bucketDestination, Key: keyName, Body: Buffer.from(testData) }, (err, data) => {
                if (err) {
                    return next(err);
                }
                expectedVersionId = data.VersionId;
                return next();
            }),
            headObject: next => dstS3.headObject({ Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => dstS3.listObjectVersions({ Bucket: bucketDestination }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const headObjectRes = results.headObject;
            assert.strictEqual(headObjectRes.VersionId, 'null');

            const listObjectVersionsRes = results.listObjectVersions;
            const { Versions } = listObjectVersionsRes;

            assert.strictEqual(Versions.length, 2);

            const [currentVersion, nonCurrentVersion] = Versions;
            assert.strictEqual(currentVersion.VersionId, expectedVersionId);
            assert.strictEqual(nonCurrentVersion.VersionId, 'null');

            return done();
        });
    });

    it('should replicate/put metadata to a destination that has a version', done => {
        let objMD;
        let firstVersionId;
        let secondVersionId;

        async.series({
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObjectDestination: next => dstS3.putObject(
            { Bucket: bucketDestination, Key: keyName, Body: Buffer.from(testData) }, (err, data) => {
                if (err) {
                    return next(err);
                }
                firstVersionId = data.VersionId;
                return next();
            }),
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObjectSource: next => srcS3.putObject(
            { Bucket: bucketSource, Key: keyName, Body: Buffer.from(testData) }, (err, data) => {
                if (err) {
                    return next(err);
                }
                secondVersionId = data.VersionId;
                return next();
            }),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: secondVersionId,
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId: secondVersionId,
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMD,
            }, next),
            headObjectFirstVersion: next => dstS3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: firstVersionId }, next),
            headObjectSecondVersion: next => dstS3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: secondVersionId }, next),
            listObjectVersions: next => dstS3.listObjectVersions({ Bucket: bucketDestination }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const firstHeadObjectRes = results.headObjectFirstVersion;
            assert.strictEqual(firstHeadObjectRes.VersionId, firstVersionId);

            const secondHeadObjectRes = results.headObjectSecondVersion;
            assert.strictEqual(secondHeadObjectRes.VersionId, secondVersionId);

            const listObjectVersionsRes = results.listObjectVersions;
            const { Versions } = listObjectVersionsRes;

            assert.strictEqual(Versions.length, 2);
            const [currentVersion, nonCurrentVersion] = Versions;

            assert.strictEqual(currentVersion.VersionId, secondVersionId);
            assert.strictEqual(currentVersion.IsLatest, true);

            assert.strictEqual(nonCurrentVersion.VersionId, firstVersionId);
            assert.strictEqual(nonCurrentVersion.IsLatest, false);

            return done();
        });
    });

    // TODO fix and unskip by CLDSRV-632
    const itSkipNotS3C = process.env.S3_END_TO_END ? it : it.skip;
    itSkipNotS3C('should replicate/put metadata to a destination that has a null version', done => {
        let objMD;
        let versionId;

        async.series({
            putObjectDestinationInitial: next => dstS3.putObject(
                { Bucket: bucketDestination, Key: keyName, Body: Buffer.from(testData) }, next),
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObjectSource: next => srcS3.putObject(
            { Bucket: bucketSource, Key: keyName, Body: Buffer.from(testData) }, (err, data) => {
                if (err) {
                    return next(err);
                }
                versionId = data.VersionId;
                return next();
            }),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMD,
            }, next),
            headObjectNullVersion: next => dstS3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => dstS3.listObjectVersions(
                { Bucket: bucketDestination }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const headObjectRes = results.headObjectNullVersion;
            assert.strictEqual(headObjectRes.VersionId, 'null');

            const listObjectVersionsRes = results.listObjectVersions;
            const { Versions } = listObjectVersionsRes;

            assert.strictEqual(Versions.length, 2);
            const [currentVersion, nonCurrentVersion] = Versions;

            assert.strictEqual(currentVersion.VersionId, versionId);
            assert.strictEqual(currentVersion.IsLatest, true);

            assert.strictEqual(nonCurrentVersion.VersionId, 'null');
            assert.strictEqual(nonCurrentVersion.IsLatest, false);

            return done();
        });
    });

    itSkipNotS3C('should replicate/put metadata to a destination that has a suspended null version', done => {
        let objMD;
        let versionId;

        async.series({
            suspendVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Suspended' } }, next),
            putObjectDestinationInitial: next => dstS3.putObject(
                { Bucket: bucketDestination, Key: keyName, Body: Buffer.from(testData) }, next),
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObjectSource: next => srcS3.putObject(
            { Bucket: bucketSource, Key: keyName, Body: Buffer.from(testData) }, (err, data) => {
                if (err) {
                    return next(err);
                }
                versionId = data.VersionId;
                return next();
            }),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMD,
            }, next),
            headObjectNullVersion: next => dstS3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => dstS3.listObjectVersions({ Bucket: bucketDestination }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const headObjectRes = results.headObjectNullVersion;
            assert.strictEqual(headObjectRes.VersionId, 'null');

            const listObjectVersionsRes = results.listObjectVersions;
            const { Versions } = listObjectVersionsRes;

            assert.strictEqual(Versions.length, 2);
            const [currentVersion, nonCurrentVersion] = Versions;

            assert.strictEqual(currentVersion.VersionId, versionId);
            assert.strictEqual(currentVersion.IsLatest, true);

            assert.strictEqual(nonCurrentVersion.VersionId, 'null');
            assert.strictEqual(nonCurrentVersion.IsLatest, false);

            return done();
        });
    });

    itSkipNotS3C('should replicate/put metadata to a destination that has a previously updated null version', done => {
        let objMD;
        let objMDNull;
        let versionId;

        async.series({
            putObjectDestinationInitial: next => dstS3.putObject(
                { Bucket: bucketDestination, Key: keyName, Body: Buffer.from(testData) }, next),
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadataNullVersion: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: destinationAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMDNull = JSON.parse(data.body).Body;
                return next();
            }),
            updateMetadataNullVersion: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMDNull,
            }, next),
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObjectSource: next => srcS3.putObject(
            { Bucket: bucketSource, Key: keyName, Body: Buffer.from(testData) }, (err, data) => {
                if (err) {
                    return next(err);
                }
                versionId = data.VersionId;
                return next();
            }),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMD,
            }, next),
            headObjectNullVersion: next => dstS3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => dstS3.listObjectVersions({ Bucket: bucketDestination }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const headObjectRes = results.headObjectNullVersion;
            assert.strictEqual(headObjectRes.VersionId, 'null');

            const listObjectVersionsRes = results.listObjectVersions;
            const { Versions } = listObjectVersionsRes;

            assert.strictEqual(Versions.length, 2);
            const [currentVersion, nonCurrentVersion] = Versions;

            assert.strictEqual(currentVersion.VersionId, versionId);
            assert.strictEqual(currentVersion.IsLatest, true);

            assert.strictEqual(nonCurrentVersion.VersionId, 'null');
            assert.strictEqual(nonCurrentVersion.IsLatest, false);

            return done();
        });
    });

    itSkipNotS3C(
        'should replicate/put metadata to a destination that has a suspended null version with internal version',
    done => {
        const tagSet = [
            {
                Key: 'key1',
                Value: 'value1',
            },
        ];
        let objMD;
        let versionId;

        async.series({
            suspendVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Suspended' } }, next),
            putObjectDestinationInitial: next => dstS3.putObject(
                { Bucket: bucketDestination, Key: keyName, Body: Buffer.from(testData) }, next),
            putObjectTagging: next => dstS3.putObjectTagging(
                { Bucket: bucketDestination, Key: keyName, Tagging: { TagSet: tagSet } }, next),
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObjectSource: next => srcS3.putObject(
            { Bucket: bucketSource, Key: keyName, Body: Buffer.from(testData) }, (err, data) => {
                if (err) {
                    return next(err);
                }
                versionId = data.VersionId;
                return next();
            }),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMD,
            }, next),
            headObjectNullVersion: next => dstS3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            getObjectTaggingNullVersion: next => dstS3.getObjectTagging(
                { Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => dstS3.listObjectVersions({ Bucket: bucketDestination }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const headObjectRes = results.headObjectNullVersion;
            assert.strictEqual(headObjectRes.VersionId, 'null');

            const getObjectTaggingRes = results.getObjectTaggingNullVersion;
            assert.deepStrictEqual(getObjectTaggingRes.TagSet, tagSet);

            const listObjectVersionsRes = results.listObjectVersions;
            const { Versions } = listObjectVersionsRes;

            assert.strictEqual(Versions.length, 2);
            const [currentVersion, nonCurrentVersion] = Versions;

            assert.strictEqual(currentVersion.VersionId, versionId);
            assert.strictEqual(currentVersion.IsLatest, true);

            assert.strictEqual(nonCurrentVersion.VersionId, 'null');
            assert.strictEqual(nonCurrentVersion.IsLatest, false);

            return done();
        });
    });

    itSkipNotS3C('should mimic null version replication by crrExistingObjects, then replicate version', done => {
        let objMDNull;
        let objMDNullReplicated;
        let objMDVersion;
        let versionId;

        async.series({
            createNullSoloMasterKey: next => srcS3.putObject(
                { Bucket: bucketSource, Key: keyName, Body: Buffer.from(testData) }, next),
            enableVersioningSource: next => srcS3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            simulateCrrExistingObjectsGetMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMDNull = JSON.parse(data.body).Body;
                assert.strictEqual(JSON.parse(objMDNull).versionId, undefined);
                return next();
            }),
            simulateCrrExistingObjectsPutMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: sourceAuthCredentials,
                requestBody: objMDNull,
            }, next),
            enableVersioningDestination: next => dstS3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            replicateNullVersion: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMDNullReplicated = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            putReplicatedNullVersion: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMDNullReplicated,
            }, next),
            putNewVersionSource: next => srcS3.putObject(
            { Bucket: bucketSource, Key: keyName, Body: Buffer.from(testData) }, (err, data) => {
                if (err) {
                    return next(err);
                }
                versionId = data.VersionId;
                return next();
            }),
            simulateMetadataReplicationVersion: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMDVersion = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            listObjectVersionsBeforeReplicate: next => dstS3.listObjectVersions({ Bucket: bucketDestination }, next),
            putReplicatedVersion: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: destinationAuthCredentials,
                requestBody: objMDVersion,
            }, next),
            checkReplicatedNullVersion: next => dstS3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            checkReplicatedVersion: next => dstS3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: versionId }, next),
            listObjectVersionsAfterReplicate: next => dstS3.listObjectVersions({ Bucket: bucketDestination }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const headObjectNullVersionRes = results.checkReplicatedNullVersion;
            assert.strictEqual(headObjectNullVersionRes.VersionId, 'null');

            const headObjectVersionRes = results.checkReplicatedVersion;
            assert.strictEqual(headObjectVersionRes.VersionId, versionId);

            const listObjectVersionsRes = results.listObjectVersionsAfterReplicate;
            const { Versions } = listObjectVersionsRes;

            assert.strictEqual(Versions.length, 2);

            const [currentVersion, nonCurrentVersion] = Versions;

            assert.strictEqual(currentVersion.VersionId, versionId);
            assert.strictEqual(currentVersion.IsLatest, true);

            assert.strictEqual(nonCurrentVersion.VersionId, 'null');
            assert.strictEqual(nonCurrentVersion.IsLatest, false);

            return done();
        });
    });

    it('should replicate/put NULL metadata to a destination that has a version', done => {
        let objMD;
        let versionId;

        async.series({
            enableVersioningDestination: next => dstS3.putBucketVersioning({
                Bucket: bucketDestination,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            putObjectDestination: next => dstS3.putObject({
                Bucket: bucketDestination,
                Key: keyName,
                Body: Buffer.from(testData),
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                versionId = data.VersionId;
                return next();
            }),
            putObjectSource: next => srcS3.putObject({
                Bucket: bucketSource,
                Key: keyName,
                Body: Buffer.from(testData),
            }, next),
            enableVersioningSource: next => srcS3.putBucketVersioning({
                Bucket: bucketSource,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: { versionId: 'null' },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: { versionId: 'null' },
                authCredentials: destinationAuthCredentials,
                requestBody: objMD,
            }, next),
            headObjectByVersionId: next => dstS3.headObject({
                Bucket: bucketDestination,
                Key: keyName,
                VersionId: versionId,
            }, next),
            headObjectByNullVersionId: next => dstS3.headObject({
                Bucket: bucketDestination,
                Key: keyName,
                VersionId: 'null',
            }, next),
            listObjectVersions: next => dstS3.listObjectVersions({
                Bucket: bucketDestination,
            }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const firstHeadObjectRes = results.headObjectByVersionId;
            assert.strictEqual(firstHeadObjectRes.VersionId, versionId);

            const secondHeadObjectRes = results.headObjectByNullVersionId;
            assert.strictEqual(secondHeadObjectRes.VersionId, 'null');

            const listObjectVersionsRes = results.listObjectVersions;
            const { Versions } = listObjectVersionsRes;

            assert.strictEqual(Versions.length, 2);
            const [currentVersion, nonCurrentVersion] = Versions;

            assert.strictEqual(currentVersion.VersionId, 'null');
            assert.strictEqual(currentVersion.IsLatest, true);

            assert.strictEqual(nonCurrentVersion.VersionId, versionId);
            assert.strictEqual(nonCurrentVersion.IsLatest, false);

            return done();
        });
    });

    it('should replicate/put NULL metadata to a destination that has a null version', done => {
        let objMD;

        async.series({
            putObjectDestinationInitial: next => dstS3.putObject({
                Bucket: bucketDestination,
                Key: keyName,
                Body: Buffer.from(testData),
            }, next),
            enableVersioningDestination: next => dstS3.putBucketVersioning({
                Bucket: bucketDestination,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            putObjectSource: next => srcS3.putObject({
                Bucket: bucketSource,
                Key: keyName,
                Body: Buffer.from(testData),
            }, next),
            enableVersioningSource: next => srcS3.putBucketVersioning({
                Bucket: bucketSource,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            putObjectTaggingSource: next => srcS3.putObjectTagging({
                Bucket: bucketSource,
                Key: keyName,
                VersionId: 'null',
                Tagging: { TagSet: [{ Key: 'key1', Value: 'value1' }] },
            }, next),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: { versionId: 'null' },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: { versionId: 'null' },
                authCredentials: destinationAuthCredentials,
                requestBody: objMD,
            }, next),
            headObjectNullVersion: next => dstS3.headObject({
                Bucket: bucketDestination,
                Key: keyName,
                VersionId: 'null',
            }, next),
            getObjectTaggingNullVersion: next => dstS3.getObjectTagging({
                Bucket: bucketDestination,
                Key: keyName,
                VersionId: 'null',
            }, next),
            listObjectVersions: next => dstS3.listObjectVersions({
                Bucket: bucketDestination,
            }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const headObjectRes = results.headObjectNullVersion;
            assert.strictEqual(headObjectRes.VersionId, 'null');

            const getObjectTaggingRes = results.getObjectTaggingNullVersion;
            assert.deepStrictEqual(getObjectTaggingRes.TagSet, [{ Key: 'key1', Value: 'value1' }]);

            const listObjectVersionsRes = results.listObjectVersions;
            const { Versions } = listObjectVersionsRes;

            assert.strictEqual(Versions.length, 1);
            const [currentVersion] = Versions;

            assert.strictEqual(currentVersion.VersionId, 'null');
            assert.strictEqual(currentVersion.IsLatest, true);

            return done();
        });
    });

    it('should replicate/put a lifecycled NULL metadata to a destination that has a version', done => {
        let objMDUpdated;
        let objMDReplicated;
        let versionId;

        async.series({
            // === SETUP PHASE ===
            enableVersioningDestination: next => dstS3.putBucketVersioning({
                Bucket: bucketDestination,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            putObjectDestination: next => dstS3.putObject({
                Bucket: bucketDestination,
                Key: keyName,
                Body: Buffer.from(testData),
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                versionId = data.VersionId;
                return next();
            }),
            putObjectSource: next => srcS3.putObject({
                Bucket: bucketSource,
                Key: keyName,
                Body: Buffer.from(testData),
            }, next),
            enableVersioningSource: next => srcS3.putBucketVersioning({
                Bucket: bucketSource,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            // === LIFECYCLE SIMULATION PHASE ===
            // Lifecycle Simulation: GET current null version metadata
            getSourceNullVersionForLifecycle: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: { versionId: 'null' },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMDUpdated = JSON.parse(data.body).Body;
                return next();
            }),
            // Lifecycle Simulation: Apply lifecycle changes to null version metadata
            // Lifecycle changes can consist of:
            // - storage class transitions (STANDARD -> IA -> GLACIER)
            // - data location changes (different storage backend)
            // Here metadata is unchanged for the simulation
            applyLifecycleToSourceNullVersion: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: { versionId: 'null' },
                authCredentials: sourceAuthCredentials,
                requestBody: objMDUpdated,
            }, next),
            // === REPLICATION PHASE ===
            // Replication: GET lifecycled metadata from source for replication
            getSourceLifecycledNullVersionForReplication: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: { versionId: 'null' },
                authCredentials: sourceAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMDReplicated = objectMDWithUpdatedAccountInfo(data, src === dst ? null : dstAccountInfo);
                return next();
            }),
            // Replication: PUT lifecycled null version to destination
            replicateLifecycledNullVersionToDestination: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: { versionId: 'null' },
                authCredentials: destinationAuthCredentials,
                requestBody: objMDReplicated,
            }, next),
            // === VALIDATION PHASE ===
            headObjectByVersionId: next => dstS3.headObject({
                Bucket: bucketDestination,
                Key: keyName,
                VersionId: versionId,
            }, next),
            headObjectByNullVersion: next => dstS3.headObject({
                Bucket: bucketDestination,
                Key: keyName,
                VersionId: 'null',
            }, next),
            listObjectVersionsDestination: next => dstS3.listObjectVersions({
                Bucket: bucketDestination,
            }, next),
        }, (err, results) => {
            if (err) {
                return done(err);
            }

            const firstHeadObjectRes = results.headObjectByVersionId;
            assert.strictEqual(firstHeadObjectRes.VersionId, versionId);

            const secondHeadObjectRes = results.headObjectByNullVersion;
            assert.strictEqual(secondHeadObjectRes.VersionId, 'null');

            const listObjectVersionsRes = results.listObjectVersionsDestination;
            const { Versions } = listObjectVersionsRes;

            assert.strictEqual(Versions.length, 2);
            const [currentVersion, nonCurrentVersion] = Versions;

            assert.strictEqual(currentVersion.VersionId, 'null');
            assert.strictEqual(currentVersion.IsLatest, true);

            assert.strictEqual(nonCurrentVersion.VersionId, versionId);
            assert.strictEqual(nonCurrentVersion.IsLatest, false);

            return done();
        });
    });
});
});
