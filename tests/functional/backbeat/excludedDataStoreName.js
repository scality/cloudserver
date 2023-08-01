const assert = require('assert');
const async = require('async');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');
const { removeAllVersions } = require('../aws-node-sdk/lib/utility/versioning-util');
const { makeBackbeatRequest, updateMetadata } = require('./utils');

const testBucket = 'bucket-for-list-lifecycle-current-tests';
const location1 = 'us-east-1';
const location2 = 'us-east-2';

const credentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

describe('excludedDataStoreName', () => {
    let bucketUtil;
    let s3;
    const expectedVersions = [];

    before(done => {
        bucketUtil = new BucketUtility('account1', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;

        return async.series([
            next => s3.createBucket({ Bucket: testBucket }, next),
            next => s3.putBucketVersioning({
                Bucket: testBucket,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            next => s3.putObject({ Bucket: testBucket, Key: 'key0' }, (err, data) => {
                expectedVersions.push(data.VersionId);
                return next(err);
            }),
            next => s3.putObject({ Bucket: testBucket, Key: 'key0' }, (err, data) => {
                if (err) {
                    return next(err);
                }
                const versionId = data.VersionId;
                return updateMetadata(
                    { bucket: testBucket, objectKey: 'key0', versionId, authCredentials: credentials },
                    { dataStoreName: location2 },
                    next);
            }),
            next => s3.putObject({ Bucket: testBucket, Key: 'key0' }, (err, data) => {
                expectedVersions.push(data.VersionId);
                return next(err);
            }),
            next => s3.putObject({ Bucket: testBucket, Key: 'key0' }, next),
            next => s3.putObject({ Bucket: testBucket, Key: 'key1' }, (err, data) => {
                if (err) {
                    return next(err);
                }
                const versionId = data.VersionId;
                return updateMetadata(
                    { bucket: testBucket, objectKey: 'key1', versionId, authCredentials: credentials },
                    { dataStoreName: location2 },
                    next);
            }),
            next => s3.putObject({ Bucket: testBucket, Key: 'key2' }, next),
        ], done);
    });

    after(done => async.series([
        next => removeAllVersions({ Bucket: testBucket }, next),
        next => s3.deleteBucket({ Bucket: testBucket }, next),
    ], done));

    it('should return error when listing current versions if excluded-data-store-name is not in config', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'current', 'excluded-data-store-name': 'idonotexist' },
            authCredentials: credentials,
        }, err => {
            assert(err, 'Expected error but found none');
            assert.strictEqual(err.code, 'InvalidLocationConstraint');
            assert.strictEqual(err.statusCode, 400);
            assert.strictEqual(err.message, 'value of the location you are attempting to set' +
            ' - idonotexist - is not listed in the locationConstraint config');
            return done();
        });
    });

    it('should return error when listing non-current versions if excluded-data-store-name is not in config', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'noncurrent', 'excluded-data-store-name': 'idonotexist' },
            authCredentials: credentials,
        }, err => {
            assert(err, 'Expected error but found none');
            assert.strictEqual(err.code, 'InvalidLocationConstraint');
            assert.strictEqual(err.statusCode, 400);
            assert.strictEqual(err.message, 'value of the location you are attempting to set' +
            ' - idonotexist - is not listed in the locationConstraint config');
            return done();
        });
    });

    it('should exclude current versions stored in location2', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'current', 'excluded-data-store-name': location2 },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextMarker);
            assert.strictEqual(data.MaxKeys, 1000);

            const contents = data.Contents;
            assert.strictEqual(contents.length, 2);

            assert.strictEqual(contents[0].Key, 'key0');
            assert.strictEqual(contents[0].DataStoreName, location1);
            assert.strictEqual(contents[1].Key, 'key2');
            assert.strictEqual(contents[1].DataStoreName, location1);
            return done();
        });
    });

    it('should return trucated listing that excludes current versions stored in location2', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'current', 'excluded-data-store-name': location2, 'max-keys': '1' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, true);
            assert.strictEqual(data.NextMarker, 'key0');
            assert.strictEqual(data.MaxKeys, 1);

            const contents = data.Contents;
            assert.strictEqual(contents.length, 1);

            assert.strictEqual(contents[0].Key, 'key0');
            assert.strictEqual(contents[0].DataStoreName, location1);

            return  makeBackbeatRequest({
                method: 'GET',
                bucket: testBucket,
                queryObj: {
                    'list-type': 'current',
                    'excluded-data-store-name': location2,
                    'max-keys': '1',
                    'marker': 'key0',
                },
                authCredentials: credentials,
            }, (err, response) => {
                assert.ifError(err);
                assert.strictEqual(response.statusCode, 200);
                const data = JSON.parse(response.body);

                assert.strictEqual(data.IsTruncated, false);
                assert(!data.NextMarker);
                assert.strictEqual(data.MaxKeys, 1);

                const contents = data.Contents;
                assert.strictEqual(contents.length, 1);

                assert.strictEqual(contents[0].Key, 'key2');
                assert.strictEqual(contents[0].DataStoreName, location1);
                return done();
            });
        });
    });

    it('should exclude non-current versions stored in location2', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'noncurrent', 'excluded-data-store-name': location2 },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextKeyMarker);
            assert(!data.NextVersionIdMarker);
            assert.strictEqual(data.MaxKeys, 1000);

            const contents = data.Contents;
            assert.strictEqual(contents.length, 2);

            assert.strictEqual(contents[0].Key, 'key0');
            assert.strictEqual(contents[0].DataStoreName, location1);
            assert.strictEqual(contents[0].VersionId, expectedVersions[1]);
            assert.strictEqual(contents[1].Key, 'key0');
            assert.strictEqual(contents[1].DataStoreName, location1);
            assert.strictEqual(contents[1].VersionId, expectedVersions[0]);
            return done();
        });
    });

    it('should return trucated listing that excludes non-current versions stored in location2', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'noncurrent', 'excluded-data-store-name': location2, 'max-keys': '1' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, true);
            assert.strictEqual(data.NextKeyMarker, 'key0');
            assert.strictEqual(data.NextVersionIdMarker, expectedVersions[1]);
            assert.strictEqual(data.MaxKeys, 1);

            const contents = data.Contents;
            assert.strictEqual(contents.length, 1);

            assert.strictEqual(contents[0].Key, 'key0');
            assert.strictEqual(contents[0].DataStoreName, location1);
            assert.strictEqual(contents[0].VersionId, expectedVersions[1]);
            return makeBackbeatRequest({
                method: 'GET',
                bucket: testBucket,
                queryObj: {
                    'list-type': 'noncurrent',
                    'excluded-data-store-name': location2,
                    'key-marker': 'key0',
                    'version-id-marker': expectedVersions[1],
                    'max-keys': '1',
                },
                authCredentials: credentials,
            }, (err, response) => {
                assert.ifError(err);
                assert.strictEqual(response.statusCode, 200);
                const data = JSON.parse(response.body);

                assert.strictEqual(data.IsTruncated, true);
                assert.strictEqual(data.NextKeyMarker, 'key0');
                assert.strictEqual(data.NextVersionIdMarker, expectedVersions[0]);
                assert.strictEqual(data.MaxKeys, 1);

                const contents = data.Contents;
                assert.strictEqual(contents.length, 1);

                assert.strictEqual(contents[0].Key, 'key0');
                assert.strictEqual(contents[0].DataStoreName, location1);
                assert.strictEqual(contents[0].VersionId, expectedVersions[0]);
                return makeBackbeatRequest({
                    method: 'GET',
                    bucket: testBucket,
                    queryObj: {
                        'list-type': 'noncurrent',
                        'excluded-data-store-name': location2,
                        'key-marker': 'key0',
                        'version-id-marker': expectedVersions[0],
                        'max-keys': '1',
                    },
                    authCredentials: credentials,
                }, (err, response) => {
                    assert.ifError(err);
                    assert.strictEqual(response.statusCode, 200);
                    const data = JSON.parse(response.body);

                    assert.strictEqual(data.IsTruncated, false);
                    assert(!data.NextKeyMarker);
                    assert(!data.NextVersionIdMarker);
                    assert.strictEqual(data.MaxKeys, 1);

                    const contents = data.Contents;
                    assert.strictEqual(contents.length, 0);
                    return done();
                });
            });
        });
    });
});
