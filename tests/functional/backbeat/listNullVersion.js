const assert = require('assert');
const async = require('async');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');
const { removeAllVersions } = require('../aws-node-sdk/lib/utility/versioning-util');
const { makeBackbeatRequest, runIfMongoV1 } = require('./utils');

const testBucket = 'bucket-for-list-lifecycle-null-tests';

const credentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

runIfMongoV1('listLifecycle if null version', () => {
    let bucketUtil;
    let s3;
    let versionForKey2;

    before(done => {
        bucketUtil = new BucketUtility('account1', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;

        return async.series([
            next => s3.createBucket({ Bucket: testBucket }, next),
            next => s3.putObject({ Bucket: testBucket, Key: 'key1', Body: '123' }, next),
            next => s3.putObject({ Bucket: testBucket, Key: 'key2', Body: '123' }, next),
            next => s3.putBucketVersioning({
                Bucket: testBucket,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            next => s3.putObject({ Bucket: testBucket, Key: 'key1', Body: '123' }, (err, data) => {
                if (err) {
                    return next(err);
                }
                // delete version to create a null current version for key1.
                return s3.deleteObject({ Bucket: testBucket, Key: 'key1', VersionId: data.VersionId }, next);
            }),
            next => s3.putObject({ Bucket: testBucket, Key: 'key2', Body: '123' }, (err, data) => {
                if (err) {
                    return next(err);
                }
                versionForKey2 = data.VersionId;
                return next();
            }),
        ], done);
    });

    after(done => async.series([
        next => removeAllVersions({ Bucket: testBucket }, next),
        next => s3.deleteBucket({ Bucket: testBucket }, next),
    ], done));

    it('should return the null noncurrent versions', done => {
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

            const contents = data.Contents;
            assert.strictEqual(contents.length, 1);
            assert.strictEqual(contents[0].Key, 'key2');
            assert.strictEqual(contents[0].VersionId, 'null');
            return done();
        });
    });

    it('should return the null current versions', done => {
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

            const contents = data.Contents;
            assert.strictEqual(contents.length, 2);

            const firstKey = contents[0];
            assert.strictEqual(firstKey.Key, 'key1');
            assert.strictEqual(firstKey.VersionId, 'null');

            const secondKey = contents[1];
            assert.strictEqual(secondKey.Key, 'key2');
            assert.strictEqual(secondKey.VersionId, versionForKey2);
            return done();
        });
    });
});
