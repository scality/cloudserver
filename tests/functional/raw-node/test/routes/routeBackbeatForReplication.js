const assert = require('assert');
const async = require('async');
const { models } = require('arsenal');
const { ObjectMD } = models;

const { makeBackbeatRequest } = require('../../utils/makeRequest');
const BucketUtility = require('../../../aws-node-sdk/lib/utility/bucket-util');

const describeSkipIfAWS = process.env.AWS_ON_AIR ? describe.skip : describe;

const backbeatAuthCredentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

const testData = 'testkey data';

describeSkipIfAWS('backbeat routes for replication', () => {
    const bucketUtil = new BucketUtility(
        'default', { signatureVersion: 'v4' });
    const s3 = bucketUtil.s3;

    const bucketSource = 'backbeatbucket-replication-source';
    const bucketDestination = 'backbeatbucket-replication-destination';
    const keyName = 'key0';
    const storageClass = 'foo';

    beforeEach(done =>
        bucketUtil.emptyIfExists(bucketSource)
            .then(() => s3.createBucket({ Bucket: bucketSource }).promise())
            .then(() => bucketUtil.emptyIfExists(bucketDestination))
            .then(() => s3.createBucket({ Bucket: bucketDestination }).promise())
            .then(() => done(), err => done(err))
    );

    afterEach(done =>
        bucketUtil.empty(bucketSource)
            .then(() => s3.deleteBucket({ Bucket: bucketSource }).promise())
            .then(() => bucketUtil.empty(bucketDestination))
            .then(() => s3.deleteBucket({ Bucket: bucketDestination }).promise())
            .then(() => done(), err => done(err))
    );

    it('should successfully replicate a null version', done => {
        let objMD;

        async.series({
            putObject: next => s3.putObject({ Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, next),
            enableVersioningSource: next => s3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            enableVersioningDestination: next => s3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: backbeatAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = JSON.parse(data.body).Body;
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
                authCredentials: backbeatAuthCredentials,
                requestBody: objMD,
            }, next),
            headObject: next => s3.headObject({ Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => s3.listObjectVersions({ Bucket: bucketDestination }, next),
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
            suspendVersioningSource: next => s3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Suspended' } }, next),
            putObject: next => s3.putObject({ Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, next),
            enableVersioningSource: next => s3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            enableVersioningDestination: next => s3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: backbeatAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = JSON.parse(data.body).Body;
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
                authCredentials: backbeatAuthCredentials,
                requestBody: objMD,
            }, next),
            headObject: next => s3.headObject({ Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => s3.listObjectVersions({ Bucket: bucketDestination }, next),
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
            putObject: next => s3.putObject({ Bucket: bucketSource, Key: keyName, Body: Buffer.from(testData) }, next),
            enableVersioningSource: next => s3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            enableVersioningDestination: next => s3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: backbeatAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = JSON.parse(data.body).Body;
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
                authCredentials: backbeatAuthCredentials,
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
                    authCredentials: backbeatAuthCredentials,
                    requestBody: result.getSerialized(),
                }, next);
            },
            headObject: next => s3.headObject({ Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => s3.listObjectVersions({ Bucket: bucketDestination }, next),
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
            putObjectSource: next => s3.putObject(
                { Bucket: bucketSource, Key: keyName, Body: Buffer.from(testData) }, next),
            enableVersioningSource: next => s3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            enableVersioningDestination: next => s3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: backbeatAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = JSON.parse(data.body).Body;
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
                authCredentials: backbeatAuthCredentials,
                requestBody: objMD,
            }, next),
            putObjectDestination: next => s3.putObject(
            { Bucket: bucketDestination, Key: keyName, Body: Buffer.from(testData) }, (err, data) => {
                if (err) {
                    return next(err);
                }
                expectedVersionId = data.VersionId;
                return next();
            }),
            headObject: next => s3.headObject({ Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => s3.listObjectVersions({ Bucket: bucketDestination }, next),
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
            enableVersioningDestination: next => s3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObjectDestination: next => s3.putObject(
            { Bucket: bucketDestination, Key: keyName, Body: Buffer.from(testData) }, (err, data) => {
                if (err) {
                    return next(err);
                }
                firstVersionId = data.VersionId;
                return next();
            }),
            enableVersioningSource: next => s3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObjectSource: next => s3.putObject(
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
                authCredentials: backbeatAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = JSON.parse(data.body).Body;
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
                authCredentials: backbeatAuthCredentials,
                requestBody: objMD,
            }, next),
            headObjectFirstVersion: next => s3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: firstVersionId }, next),
            headObjectSecondVersion: next => s3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: secondVersionId }, next),
            listObjectVersions: next => s3.listObjectVersions({ Bucket: bucketDestination }, next),
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

    it('should replicate/put metadata to a destination that has a null version', done => {
        let objMD;
        let versionId;

        async.series({
            putObjectDestinationInitial: next => s3.putObject(
                { Bucket: bucketDestination, Key: keyName, Body: Buffer.from(testData) }, next),
            enableVersioningDestination: next => s3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            enableVersioningSource: next => s3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObjectSource: next => s3.putObject(
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
                authCredentials: backbeatAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = JSON.parse(data.body).Body;
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
                authCredentials: backbeatAuthCredentials,
                requestBody: objMD,
            }, next),
            headObjectNullVersion: next => s3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => s3.listObjectVersions(
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

    it('should replicate/put metadata to a destination that has a suspended null version', done => {
        let objMD;
        let versionId;

        async.series({
            suspendVersioningDestination: next => s3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Suspended' } }, next),
            putObjectDestinationInitial: next => s3.putObject(
                { Bucket: bucketDestination, Key: keyName, Body: Buffer.from(testData) }, next),
            enableVersioningDestination: next => s3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            enableVersioningSource: next => s3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObjectSource: next => s3.putObject(
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
                authCredentials: backbeatAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = JSON.parse(data.body).Body;
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
                authCredentials: backbeatAuthCredentials,
                requestBody: objMD,
            }, next),
            headObjectNullVersion: next => s3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => s3.listObjectVersions({ Bucket: bucketDestination }, next),
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

    it('should replicate/put metadata to a destination that has a previously updated null version', done => {
        let objMD;
        let objMDNull;
        let versionId;

        async.series({
            putObjectDestinationInitial: next => s3.putObject(
                { Bucket: bucketDestination, Key: keyName, Body: Buffer.from(testData) }, next),
            enableVersioningDestination: next => s3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            getMetadataNullVersion: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: backbeatAuthCredentials,
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
                authCredentials: backbeatAuthCredentials,
                requestBody: objMDNull,
            }, next),
            enableVersioningSource: next => s3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObjectSource: next => s3.putObject(
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
                authCredentials: backbeatAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = JSON.parse(data.body).Body;
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
                authCredentials: backbeatAuthCredentials,
                requestBody: objMD,
            }, next),
            headObjectNullVersion: next => s3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => s3.listObjectVersions({ Bucket: bucketDestination }, next),
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

    it('should replicate/put metadata to a destination that has a suspended null version with internal version',
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
            suspendVersioningDestination: next => s3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Suspended' } }, next),
            putObjectDestinationInitial: next => s3.putObject(
                { Bucket: bucketDestination, Key: keyName, Body: Buffer.from(testData) }, next),
            putObjectTagging: next => s3.putObjectTagging(
                { Bucket: bucketDestination, Key: keyName, Tagging: { TagSet: tagSet } }, next),
            enableVersioningDestination: next => s3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            enableVersioningSource: next => s3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            putObjectSource: next => s3.putObject(
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
                authCredentials: backbeatAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = JSON.parse(data.body).Body;
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
                authCredentials: backbeatAuthCredentials,
                requestBody: objMD,
            }, next),
            headObjectNullVersion: next => s3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            getObjectTaggingNullVersion: next => s3.getObjectTagging(
                { Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            listObjectVersions: next => s3.listObjectVersions({ Bucket: bucketDestination }, next),
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

    it('should mimic null version replication by crrExistingObjects, then replicate version', done => {
        let objMDNull;
        let objMDNullReplicated;
        let objMDVersion;
        let versionId;

        async.series({
            createNullSoloMasterKey: next => s3.putObject(
                { Bucket: bucketSource, Key: keyName, Body: Buffer.from(testData) }, next),
            enableVersioningSource: next => s3.putBucketVersioning(
                { Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } }, next),
            simulateCrrExistingObjectsGetMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: backbeatAuthCredentials,
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
                authCredentials: backbeatAuthCredentials,
                requestBody: objMDNull,
            }, next),
            enableVersioningDestination: next => s3.putBucketVersioning(
                { Bucket: bucketDestination, VersioningConfiguration: { Status: 'Enabled' } }, next),
            replicateNullVersion: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: backbeatAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMDNullReplicated = JSON.parse(data.body).Body;
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
                authCredentials: backbeatAuthCredentials,
                requestBody: objMDNullReplicated,
            }, next),
            putNewVersionSource: next => s3.putObject(
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
                authCredentials: backbeatAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMDVersion = JSON.parse(data.body).Body;
                return next();
            }),
            listObjectVersionsBeforeReplicate: next => s3.listObjectVersions({ Bucket: bucketDestination }, next),
            putReplicatedVersion: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId,
                },
                authCredentials: backbeatAuthCredentials,
                requestBody: objMDVersion,
            }, next),
            checkReplicatedNullVersion: next => s3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            checkReplicatedVersion: next => s3.headObject(
                { Bucket: bucketDestination, Key: keyName, VersionId: versionId }, next),
            listObjectVersionsAfterReplicate: next => s3.listObjectVersions({ Bucket: bucketDestination }, next),
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
            enableVersioningDestination: next => s3.putBucketVersioning({
                Bucket: bucketDestination,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            putObjectDestination: next => s3.putObject({
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
            putObjectSource: next => s3.putObject({
                Bucket: bucketSource,
                Key: keyName,
                Body: Buffer.from(testData),
            }, next),
            enableVersioningSource: next => s3.putBucketVersioning({
                Bucket: bucketSource,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            getMetadata: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: { versionId: 'null' },
                authCredentials: backbeatAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = JSON.parse(data.body).Body;
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: { versionId: 'null' },
                authCredentials: backbeatAuthCredentials,
                requestBody: objMD,
            }, next),
            headObjectByVersionId: next => s3.headObject({
                Bucket: bucketDestination,
                Key: keyName,
                VersionId: versionId,
            }, next),
            headObjectByNullVersionId: next => s3.headObject({
                Bucket: bucketDestination,
                Key: keyName,
                VersionId: 'null',
            }, next),
            listObjectVersions: next => s3.listObjectVersions({
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
            putObjectDestinationInitial: next => s3.putObject({
                Bucket: bucketDestination,
                Key: keyName,
                Body: Buffer.from(testData),
            }, next),
            enableVersioningDestination: next => s3.putBucketVersioning({
                Bucket: bucketDestination,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            putObjectSource: next => s3.putObject({
                Bucket: bucketSource,
                Key: keyName,
                Body: Buffer.from(testData),
            }, next),
            enableVersioningSource: next => s3.putBucketVersioning({
                Bucket: bucketSource,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            putObjectTaggingSource: next => s3.putObjectTagging({
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
                authCredentials: backbeatAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMD = JSON.parse(data.body).Body;
                return next();
            }),
            replicateMetadata: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: { versionId: 'null' },
                authCredentials: backbeatAuthCredentials,
                requestBody: objMD,
            }, next),
            headObjectNullVersion: next => s3.headObject({
                Bucket: bucketDestination,
                Key: keyName,
                VersionId: 'null',
            }, next),
            getObjectTaggingNullVersion: next => s3.getObjectTagging({
                Bucket: bucketDestination,
                Key: keyName,
                VersionId: 'null',
            }, next),
            listObjectVersions: next => s3.listObjectVersions({
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
            enableVersioningDestination: next => s3.putBucketVersioning({
                Bucket: bucketDestination,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            putObjectDestination: next => s3.putObject({
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
            putObjectSource: next => s3.putObject({
                Bucket: bucketSource,
                Key: keyName,
                Body: Buffer.from(testData),
            }, next),
            enableVersioningSource: next => s3.putBucketVersioning({
                Bucket: bucketSource,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            simulateLifecycleNullVersion: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: { versionId: 'null' },
                authCredentials: backbeatAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMDUpdated = JSON.parse(data.body).Body;
                return next();
            }),
            updateMetadataSource: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: { versionId: 'null' },
                authCredentials: backbeatAuthCredentials,
                requestBody: objMDUpdated,
            }, next),
            getReplicatedNullVersion: next => makeBackbeatRequest({
                method: 'GET',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: { versionId: 'null' },
                authCredentials: backbeatAuthCredentials,
            }, (err, data) => {
                if (err) {
                    return next(err);
                }
                objMDReplicated = JSON.parse(data.body).Body;
                return next();
            }),
            putReplicatedNullVersion: next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: { versionId: 'null' },
                authCredentials: backbeatAuthCredentials,
                requestBody: objMDReplicated,
            }, next),
            headObjectByVersionId: next => s3.headObject({
                Bucket: bucketDestination,
                Key: keyName,
                VersionId: versionId,
            }, next),
            headObjectByNullVersion: next => s3.headObject({
                Bucket: bucketDestination,
                Key: keyName,
                VersionId: 'null',
            }, next),
            listObjectVersionsDestination: next => s3.listObjectVersions({
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
