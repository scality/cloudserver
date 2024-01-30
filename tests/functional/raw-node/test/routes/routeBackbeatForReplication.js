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
        return async.series([
            next => s3.putObject({ Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, next),
            next => s3.putBucketVersioning({ Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } },
                next),
            next => s3.putBucketVersioning({ Bucket: bucketDestination, VersioningConfiguration:
                { Status: 'Enabled' } }, next),
            next => makeBackbeatRequest({
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
            next => makeBackbeatRequest({
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
            next => s3.headObject({ Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            next => s3.listObjectVersions({ Bucket: bucketDestination }, next),
        ], (err, data) => {
            if (err) {
                return done(err);
            }
            const headObjectRes = data[5];
            assert.strictEqual(headObjectRes.VersionId, 'null');

            const listObjectVersionsRes = data[6];
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
        return async.series([
            next => s3.putObject({ Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, next),
            next => s3.putBucketVersioning({ Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } },
                next),
            next => s3.putBucketVersioning({ Bucket: bucketDestination, VersioningConfiguration:
                { Status: 'Enabled' } }, next),
            next => makeBackbeatRequest({
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
            next => makeBackbeatRequest({
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
            next => {
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
            next => s3.headObject({ Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            next => s3.listObjectVersions({ Bucket: bucketDestination }, next),
        ], (err, data) => {
            if (err) {
                return done(err);
            }
            const headObjectRes = data[6];
            assert.strictEqual(headObjectRes.VersionId, 'null');
            assert.strictEqual(headObjectRes.StorageClass, storageClass);

            const listObjectVersionsRes = data[7];
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
        return async.series([
            next => s3.putObject({ Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, next),
            next => s3.putBucketVersioning({ Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } },
                next),
            next => s3.putBucketVersioning({ Bucket: bucketDestination, VersioningConfiguration:
                { Status: 'Enabled' } }, next),
            next => makeBackbeatRequest({
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
            next => makeBackbeatRequest({
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
            next => s3.putObject({ Bucket: bucketDestination, Key: keyName, Body: new Buffer(testData) },
            (err, data) => {
                if (err) {
                    return next(err);
                }
                expectedVersionId = data.VersionId;
                return next();
            }),
            next => s3.headObject({ Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            next => s3.listObjectVersions({ Bucket: bucketDestination }, next),
        ], (err, data) => {
            if (err) {
                return done(err);
            }
            const headObjectRes = data[6];
            assert.strictEqual(headObjectRes.VersionId, 'null');

            const listObjectVersionsRes = data[7];
            const { Versions } = listObjectVersionsRes;

            assert.strictEqual(Versions.length, 2);

            const [currentVersion, nonCurrentVersion] = Versions;
            assert.strictEqual(currentVersion.VersionId, expectedVersionId);
            assert.strictEqual(nonCurrentVersion.VersionId, 'null');
            return done();
        });
    });

    it('should replicate a null solo master version an then replicate another version', done => {
        let objMDNull;
        let objMDNullAfterMigration;
        let objMDVersion;
        let versionId;
        // Simulate a flow where a key, created before versioning, is replicated using the CRRexistingObjects script.
        // Then, replicate another version of the same key.
        return async.series([
            // 1. Create null solo master key
            next => s3.putObject({ Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, next),
            next => s3.putBucketVersioning({ Bucket: bucketSource, VersioningConfiguration: { Status: 'Enabled' } },
                next),
            // 2. Check that the version is a null solo master key
            next => makeBackbeatRequest({
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
            // 3. Simulate the putMetadata with x-scal-migrate-null-solo-master to generate an internal null version
            next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketSource,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                headers: {
                    'x-scal-migrate-null-solo-master': 'true',
                },
                authCredentials: backbeatAuthCredentials,
                requestBody: objMDNull,
            }, next),
            // 4. Simulate the put metadata logic in Replication Queue Processor.
            next => s3.putBucketVersioning({ Bucket: bucketDestination, VersioningConfiguration:
                { Status: 'Enabled' } }, next),
            next => makeBackbeatRequest({
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

                objMDNullAfterMigration = JSON.parse(data.body).Body;
                // 5. Check that the migration worked and that a versionId representing
                // the new internal version attached is set.
                assert.notEqual(JSON.parse(objMDNullAfterMigration).versionId, undefined);
                return next();
            }),
            next => makeBackbeatRequest({
                method: 'PUT',
                resourceType: 'metadata',
                bucket: bucketDestination,
                objectKey: keyName,
                queryObj: {
                    versionId: 'null',
                },
                authCredentials: backbeatAuthCredentials,
                requestBody: objMDNullAfterMigration,
            }, next),
            // 6. Put a new version in the source bucket to be replicated.
            next => s3.putObject({ Bucket: bucketSource, Key: keyName, Body: new Buffer(testData) }, (err, data) => {
                if (err) {
                    return next(err);
                }

                versionId = data.VersionId;
                return next();
            }),
            // 7. Simulate the metadata replication of the version.
            next => makeBackbeatRequest({
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
            next => makeBackbeatRequest({
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
            // 8. Check that the null version does not get overwritten.
            next => s3.headObject({ Bucket: bucketDestination, Key: keyName, VersionId: 'null' }, next),
            next => s3.headObject({ Bucket: bucketDestination, Key: keyName, VersionId: versionId }, next),
            next => s3.listObjectVersions({ Bucket: bucketDestination }, next),
        ], (err, data) => {
            if (err) {
                return done(err);
            }
            const headObjectNullRes = data[10];
            assert.strictEqual(headObjectNullRes.VersionId, 'null');

            const headObjectVersionRes = data[11];
            assert.strictEqual(headObjectVersionRes.VersionId, versionId);

            const listObjectVersionsRes = data[12];
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
});
