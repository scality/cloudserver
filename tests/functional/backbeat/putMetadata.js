const assert = require('assert');
const async = require('async');
const uuid = require('uuid');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');
const { removeAllVersions } = require('../aws-node-sdk/lib/utility/versioning-util');
const { updateMetadata, runIfMongo } = require('./utils');

const credentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

const destLocation = 'us-east-2';

const objectName = 'key';

runIfMongo('backbeat routes: putMetadata', () => {
    let bucketUtil;
    let s3;
    let bucketName;

    before(() => {
        bucketUtil = new BucketUtility('account1', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;
    });

    beforeEach(done => {
        bucketName = `put-metadata-bucket-${uuid.v4()}`;
        s3.createBucket({ Bucket: bucketName }, done);
    });

    afterEach(done => async.series([
        next => removeAllVersions({ Bucket: bucketName }, next),
        next => s3.deleteBucket({ Bucket: bucketName }, next),
    ], done));

    function updateMetadataAndAssertState(versionId, expectedObjectCount, cb) {
        async.series([
            next => updateMetadata(
                { bucket: bucketName, objectKey: objectName, versionId, authCredentials: credentials },
                { storageClass: destLocation },
                next),
            next => s3.headObject({ Bucket: bucketName, Key: objectName, VersionId: versionId }, (err, data) => {
                assert.ifError(err);
                assert(data.StorageClass, destLocation);
                return next();
            }),
            next => s3.listObjectVersions({ Bucket: bucketName }, (err, data) => {
                assert.ifError(err);
                assert.strictEqual(data.Versions.length, expectedObjectCount);
                assert.strictEqual(data.DeleteMarkers.length, 0);
                return next();
            }),
        ], cb);
    }

    it('should update the storage class of a non versioned object', done => {
        async.series([
            next => s3.putObject({ Bucket: bucketName, Key: objectName }, err => {
                assert.ifError(err);
                return next();
            }),
            next => updateMetadataAndAssertState(undefined, 1, next),
        ], done);
    });

    it('should update the storage class of a versioned object', done => {
        let versionId;
        async.series([
            next => s3.putBucketVersioning({
                Bucket: bucketName,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            next => s3.putObject({ Bucket: bucketName, Key: objectName }, (err, data) => {
                assert.ifError(err);
                versionId = data.VersionId;
                return next();
            }),
            next => updateMetadataAndAssertState(versionId, 1, next),
        ], done);
    });

    it('should update the storage class of a non last version object', done => {
        let versionId;
        async.series([
            next => s3.putBucketVersioning({
                Bucket: bucketName,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            next => s3.putObject({ Bucket: bucketName, Key: objectName }, (err, data) => {
                assert.ifError(err);
                versionId = data.VersionId;
                return next();
            }),
            next => s3.putObject({ Bucket: bucketName, Key: objectName }, err => {
                assert.ifError(err);
                return next();
            }),
            next => updateMetadataAndAssertState(versionId, 2, next),
        ], done);
    });

    it('should update the storage class of a non versioned object in a versioned bucket', done => {
        async.series([
            next => s3.putObject({ Bucket: bucketName, Key: objectName }, err => {
                assert.ifError(err);
                return next();
            }),
            next => s3.putBucketVersioning({
                Bucket: bucketName,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            next => updateMetadataAndAssertState('null', 1, next),
        ], done);
    });

    it('should update the storage class of a null version created from non versioned object '
        + 'in a versioned bucket', done => {
        async.series([
            next => s3.putObject({ Bucket: bucketName, Key: objectName }, err => {
                assert.ifError(err);
                return next();
            }),
            next => s3.putBucketVersioning({
                Bucket: bucketName,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            next => s3.putObject({ Bucket: bucketName, Key: objectName }, err => {
                assert.ifError(err);
                return next();
            }),
            next => updateMetadataAndAssertState('null', 2, next),
        ], done);
    });

    it('should update the storage class of a null version in a versioned bucket', done => {
        async.series([
            next => s3.putBucketVersioning({
                Bucket: bucketName,
                VersioningConfiguration: { Status: 'Suspended' },
            }, next),
            next => s3.putObject({ Bucket: bucketName, Key: objectName }, err => {
                assert.ifError(err);
                return next();
            }),
            next => s3.putBucketVersioning({
                Bucket: bucketName,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            next => updateMetadataAndAssertState('null', 1, next),
        ], done);
    });

    it('should update the storage class of a null non last version in a versioned bucket', done => {
        async.series([
            next => s3.putBucketVersioning({
                Bucket: bucketName,
                VersioningConfiguration: { Status: 'Suspended' },
            }, next),
            next => s3.putObject({ Bucket: bucketName, Key: objectName }, err => {
                assert.ifError(err);
                return next();
            }),
            next => s3.putBucketVersioning({
                Bucket: bucketName,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            next => s3.putObject({ Bucket: bucketName, Key: objectName }, err => {
                assert.ifError(err);
                return next();
            }),
            next => updateMetadataAndAssertState('null', 2, next),
        ], done);
    });

    it('should update the storage class of a null version in a versioning suspended bucket', done => {
        async.series([
            next => s3.putBucketVersioning({
                Bucket: bucketName,
                VersioningConfiguration: { Status: 'Suspended' },
            }, next),
            next => s3.putObject({ Bucket: bucketName, Key: objectName }, err => {
                assert.ifError(err);
                return next();
            }),
            next => updateMetadataAndAssertState('null', 1, next),
        ], done);
    });

    it('should update the storage class of a non versioned object in a versioning suspended bucket', done => {
        async.series([
            next => s3.putObject({ Bucket: bucketName, Key: objectName }, err => {
                assert.ifError(err);
                return next();
            }),
            next => s3.putBucketVersioning({
                Bucket: bucketName,
                VersioningConfiguration: { Status: 'Suspended' },
            }, next),
            next => updateMetadataAndAssertState('null', 1, next),
        ], done);
    });

    it('should update the storage class of a version in a versioning suspended bucket', done => {
        let versionId;
        async.series([
            next => s3.putBucketVersioning({
                Bucket: bucketName,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            next => s3.putObject({ Bucket: bucketName, Key: objectName }, (err, data) => {
                assert.ifError(err);
                versionId = data.VersionId;
                return next();
            }),
            next => s3.putBucketVersioning({
                Bucket: bucketName,
                VersioningConfiguration: { Status: 'Suspended' },
            }, next),
            next => updateMetadataAndAssertState(versionId, 1, next),
        ], done);
    });
});
