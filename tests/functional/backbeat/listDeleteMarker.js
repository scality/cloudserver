const assert = require('assert');
const async = require('async');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    DeleteObjectCommand,
} = require('@aws-sdk/client-s3');

const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');
const { removeAllVersions } = require('../aws-node-sdk/lib/utility/versioning-util');
const { makeBackbeatRequest } = require('./utils');

const bucketUtil = new BucketUtility('default', {});
const s3 = bucketUtil.s3;
let credentials = null;

async function getCredentials() {
    const creds = await s3.config.credentials();
    credentials = {
        accessKey: creds.accessKeyId,
        secretKey: creds.secretAccessKey,
    };
    return credentials;
}


describe('listLifecycle with non-current delete marker', () => {
    let expectedVersionId;
    let expectedDMVersionId;
    const testBucket = 'bucket-for-list-lifecycle-noncurrent-dm-tests';
    const keyName = 'key0';

    before(done => async.series([
            next => getCredentials().then(creds => {
                credentials = creds;
                next();
            }).catch(next),
            next => s3.send(new CreateBucketCommand({ Bucket: testBucket }))
                .then(() => next())
                .catch(next),
            next => s3.send(new PutBucketVersioningCommand({
                Bucket: testBucket,
                VersioningConfiguration: { Status: 'Enabled' },
            }))
                .then(() => next())
                .catch(next),
            next => s3.send(new DeleteObjectCommand({ Bucket: testBucket, Key: keyName }))
                .then(data => {
                    expectedDMVersionId = data.VersionId;
                    next();
                })
                .catch(next),
            next => s3.send(new PutObjectCommand({ Bucket: testBucket, Key: keyName }))
                .then(data => {
                    expectedVersionId = data.VersionId;
                    next();
                })
                .catch(next),
        ], done));


    after(async () => {
        await removeAllVersions({ Bucket: testBucket });
        await s3.send(new DeleteBucketCommand({ Bucket: testBucket }));
    });

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
    let expectedVersionId;
    const testBucket = 'bucket-for-list-lifecycle-current-dm-tests';
    const keyName = 'key0';

    before(done => async.series([
            next => s3.send(new CreateBucketCommand({ Bucket: testBucket }))
                .then(() => next())
                .catch(next),
            next => s3.send(new PutBucketVersioningCommand({
                Bucket: testBucket,
                VersioningConfiguration: { Status: 'Enabled' },
            }))
                .then(() => next())
                .catch(next),
            next => s3.send(new PutObjectCommand({ Bucket: testBucket, Key: keyName }))
                .then(data => {
                    expectedVersionId = data.VersionId;
                    next();
                })
                .catch(next),
            next => s3.send(new DeleteObjectCommand({ Bucket: testBucket, Key: keyName }))
                .then(() => next())
                .catch(next),
        ], done));

    after(async () => {
        await removeAllVersions({ Bucket: testBucket });
        await s3.send(new DeleteBucketCommand({ Bucket: testBucket }));
    });

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
