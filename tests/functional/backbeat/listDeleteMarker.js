const assert = require('assert');
const async = require('async');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');
const { removeAllVersions } = require('../aws-node-sdk/lib/utility/versioning-util');
const { makeBackbeatRequest } = require('./utils');

const credentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

describe('listLifecycle with non-current delete marker', () => {
    let bucketUtil;
    let s3;
    let expectedVersionId;
    let expectedDMVersionId;
    const testBucket = 'bucket-for-list-lifecycle-noncurrent-dm-tests';
    const keyName = 'key0';

    before(done => {
        bucketUtil = new BucketUtility('account1', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;

        return async.series([
            next => s3.createBucket({ Bucket: testBucket }, next),
            next => s3.putBucketVersioning({
                Bucket: testBucket,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            next => s3.deleteObject({ Bucket: testBucket, Key: keyName }, (err, data) => {
                if (err) {
                    return next(err);
                }
                expectedDMVersionId = data.VersionId;
                return next();
            }),
            next => s3.putObject({ Bucket: testBucket, Key: keyName }, (err, data) => {
                if (err) {
                    return next(err);
                }
                expectedVersionId = data.VersionId;
                return next();
            }),
        ], done);
    });

    after(done => async.series([
        next => removeAllVersions({ Bucket: testBucket }, next),
        next => s3.deleteBucket({ Bucket: testBucket }, next),
    ], done));

    it('should return the current version', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'current' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextKeyMarker);
            assert.strictEqual(data.MaxKeys, 1000);
            assert.strictEqual(data.Contents.length, 1);
            const key = data.Contents[0];
            assert.strictEqual(key.Key, keyName);
            assert.strictEqual(key.VersionId, expectedVersionId);
            return done();
        });
    });

    it('should return the non-current delete marker', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'noncurrent' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextKeyMarker);
            assert.strictEqual(data.MaxKeys, 1000);
            assert.strictEqual(data.Contents.length, 1);
            const key = data.Contents[0];
            assert.strictEqual(key.Key, keyName);
            assert.strictEqual(key.VersionId, expectedDMVersionId);
            return done();
        });
    });

    it('should return no orphan delete marker', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'orphan' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextKeyMarker);
            assert.strictEqual(data.MaxKeys, 1000);
            assert.strictEqual(data.Contents.length, 0);
            return done();
        });
    });
});

describe('listLifecycle with current delete marker version', () => {
    let bucketUtil;
    let s3;
    let expectedVersionId;
    const testBucket = 'bucket-for-list-lifecycle-current-dm-tests';
    const keyName = 'key0';

    before(done => {
        bucketUtil = new BucketUtility('account1', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;

        return async.series([
            next => s3.createBucket({ Bucket: testBucket }, next),
            next => s3.putBucketVersioning({
                Bucket: testBucket,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            next => s3.putObject({ Bucket: testBucket, Key: keyName }, (err, data) => {
                if (err) {
                    return next(err);
                }
                expectedVersionId = data.VersionId;
                return next();
            }),
            next => s3.deleteObject({ Bucket: testBucket, Key: keyName }, next),
        ], done);
    });

    after(done => async.series([
        next => removeAllVersions({ Bucket: testBucket }, next),
        next => s3.deleteBucket({ Bucket: testBucket }, next),
    ], done));

    it('should return no current object if current version is a delete marker', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'current' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextKeyMarker);
            assert.strictEqual(data.MaxKeys, 1000);
            assert.strictEqual(data.Contents.length, 0);
            return done();
        });
    });

    it('should return the non-current version', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'noncurrent' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextKeyMarker);
            assert.strictEqual(data.MaxKeys, 1000);
            assert.strictEqual(data.Contents.length, 1);
            const key = data.Contents[0];
            assert.strictEqual(key.Key, keyName);
            assert.strictEqual(key.VersionId, expectedVersionId);
            return done();
        });
    });

    it('should return no orphan delete marker', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'orphan' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextKeyMarker);
            assert.strictEqual(data.MaxKeys, 1000);
            assert.strictEqual(data.Contents.length, 0);
            return done();
        });
    });
});
