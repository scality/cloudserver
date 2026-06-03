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
const { promisify } = require('util');

const testBucket = 'bucket-for-list-lifecycle-null-tests';

const removeAllVersionsPromise = promisify(removeAllVersions);
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

describe('listLifecycle if null version', () => {
    let versionForKey2;

    before(done =>
        async.series(
            [
                next =>
                    getCredentials()
                        .then(creds => {
                            credentials = creds;
                            next();
                        })
                        .catch(next),
                next =>
                    s3
                        .send(new CreateBucketCommand({ Bucket: testBucket }))
                        .then(() => next())
                        .catch(next),
                next =>
                    s3
                        .send(new PutObjectCommand({ Bucket: testBucket, Key: 'key1', Body: '123' }))
                        .then(() => next())
                        .catch(next),
                next =>
                    s3
                        .send(new PutObjectCommand({ Bucket: testBucket, Key: 'key2', Body: '123' }))
                        .then(() => next())
                        .catch(next),
                next =>
                    s3
                        .send(
                            new PutBucketVersioningCommand({
                                Bucket: testBucket,
                                VersioningConfiguration: { Status: 'Enabled' },
                            }),
                        )
                        .then(() => next())
                        .catch(next),
                next =>
                    s3
                        .send(new PutObjectCommand({ Bucket: testBucket, Key: 'key1', Body: '123' }))
                        .then(data =>
                            // delete version to create a null current version for key1.
                            s3.send(
                                new DeleteObjectCommand({
                                    Bucket: testBucket,
                                    Key: 'key1',
                                    VersionId: data.VersionId,
                                }),
                            ),
                        )
                        .then(() => next())
                        .catch(next),
                next =>
                    s3
                        .send(new PutObjectCommand({ Bucket: testBucket, Key: 'key2', Body: '123' }))
                        .then(data => {
                            versionForKey2 = data.VersionId;
                            next();
                        })
                        .catch(next),
            ],
            done,
        ),
    );

    after(async () => {
        await removeAllVersionsPromise({ Bucket: testBucket });
        await s3.send(new DeleteBucketCommand({ Bucket: testBucket }));
    });

    it('should return the null noncurrent versions', done => {
        makeBackbeatRequest(
            {
                method: 'GET',
                bucket: testBucket,
                queryObj: { 'list-type': 'noncurrent' },
                authCredentials: credentials,
            },
            (err, response) => {
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
            },
        );
    });

    it('should return the null current versions', done => {
        makeBackbeatRequest(
            {
                method: 'GET',
                bucket: testBucket,
                queryObj: { 'list-type': 'current' },
                authCredentials: credentials,
            },
            (err, response) => {
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
            },
        );
    });
});

describe('listLifecycle with null current version after versioning suspended', () => {
    let expectedVersionId;
    const nullObjectBucket = 'bucket-for-list-lifecycle-current-null-tests';
    const keyName = 'key0';

    before(done =>
        async.series(
            [
                next =>
                    s3
                        .send(new CreateBucketCommand({ Bucket: nullObjectBucket }))
                        .then(() => next())
                        .catch(next),
                next =>
                    s3
                        .send(
                            new PutBucketVersioningCommand({
                                Bucket: nullObjectBucket,
                                VersioningConfiguration: { Status: 'Enabled' },
                            }),
                        )
                        .then(() => next())
                        .catch(next),
                next =>
                    s3
                        .send(new PutObjectCommand({ Bucket: nullObjectBucket, Key: keyName }))
                        .then(data => {
                            expectedVersionId = data.VersionId;
                            next();
                        })
                        .catch(next),
                next =>
                    s3
                        .send(
                            new PutBucketVersioningCommand({
                                Bucket: nullObjectBucket,
                                VersioningConfiguration: { Status: 'Suspended' },
                            }),
                        )
                        .then(() => next())
                        .catch(next),
                next =>
                    s3
                        .send(new PutObjectCommand({ Bucket: nullObjectBucket, Key: keyName }))
                        .then(() => next())
                        .catch(next),
            ],
            done,
        ),
    );

    after(async () => {
        await removeAllVersionsPromise({ Bucket: nullObjectBucket });
        await s3.send(new DeleteBucketCommand({ Bucket: nullObjectBucket }));
    });

    it('should return list of current versions when bucket has a null current version', done => {
        makeBackbeatRequest(
            {
                method: 'GET',
                bucket: nullObjectBucket,
                queryObj: { 'list-type': 'current' },
                authCredentials: credentials,
            },
            (err, response) => {
                assert.ifError(err);
                assert.strictEqual(response.statusCode, 200);
                const data = JSON.parse(response.body);

                assert.strictEqual(data.IsTruncated, false);
                assert(!data.NextKeyMarker);
                assert.strictEqual(data.MaxKeys, 1000);
                assert.strictEqual(data.Contents.length, 1);
                const key = data.Contents[0];
                assert.strictEqual(key.Key, keyName);
                assert.strictEqual(key.VersionId, 'null');
                return done();
            },
        );
    });

    it('should return list of non-current versions when bucket has a null current version', done => {
        makeBackbeatRequest(
            {
                method: 'GET',
                bucket: nullObjectBucket,
                queryObj: { 'list-type': 'noncurrent' },
                authCredentials: credentials,
            },
            (err, response) => {
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
            },
        );
    });
});
