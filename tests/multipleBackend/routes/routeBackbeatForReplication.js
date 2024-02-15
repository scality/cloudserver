const assert = require('assert');
const async = require('async');
const { models } = require('arsenal');
const { ObjectMD } = models;

const { makeBackbeatRequest } = require('../../functional/raw-node/utils/makeRequest');
const BucketUtility = require('../../functional/aws-node-sdk/lib/utility/bucket-util');

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
});
