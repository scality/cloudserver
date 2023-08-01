const assert = require('assert');
const async = require('async');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');
const { removeAllVersions } = require('../aws-node-sdk/lib/utility/versioning-util');
const { makeBackbeatRequest } = require('./utils');

const testBucket = 'bucket-for-list-lifecycle-orphans-tests';
const emptyBucket = 'empty-bucket-for-list-lifecycle-orphans-tests';
const nonVersionedBucket = 'non-versioned-bucket-for-list-lifecycle-orphans-tests';

const credentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

function checkContents(contents) {
    contents.forEach(d => {
        assert(d.Key);
        assert(d.LastModified);
        assert(d.VersionId);
        assert(d.Owner.DisplayName);
        assert(d.Owner.ID);
        assert.strictEqual(d.IsLatest, true);
        assert.strictEqual(d.ListType, 'orphan');
        assert(!d.ETag);
        assert(!d.Size);
        assert(!d.StorageClass);
        assert(!d.TagSet);
        assert(!d.DataStoreName);
    });
}

function createDeleteMarker(s3, bucketName, keyName, cb) {
    return async.series([
        next => s3.putObject({ Bucket: bucketName, Key: keyName, Body: '123', Tagging: 'mykey=myvalue' }, next),
        next => s3.deleteObject({ Bucket: bucketName, Key: keyName }, next),
    ], cb);
}

function createOrphanDeleteMarker(s3, bucketName, keyName, cb) {
    let versionId;
    return async.series([
        next => s3.putObject({ Bucket: bucketName, Key: keyName, Body: '123', Tagging: 'mykey=myvalue' },
            (err, data) => {
                if (err) {
                    return next(err);
                }
                versionId = data.VersionId;
                return next();
            }),
        next => s3.deleteObject({ Bucket: bucketName, Key: keyName }, next),
        next => s3.deleteObject({ Bucket: bucketName, Key: keyName, VersionId: versionId }, next),
    ], cb);
}

describe('listLifecycleOrphanDeleteMarkers', () => {
    let bucketUtil;
    let s3;
    let date;

    before(done => {
        bucketUtil = new BucketUtility('account1', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;

        return async.series([
            next => s3.createBucket({ Bucket: testBucket }, next),
            next => s3.createBucket({ Bucket: emptyBucket }, next),
            next => s3.createBucket({ Bucket: nonVersionedBucket }, next),
            next => s3.putBucketVersioning({
                Bucket: testBucket,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            next => s3.putBucketVersioning({
                Bucket: emptyBucket,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            next => async.times(3, (n, cb) => {
                createOrphanDeleteMarker(s3, testBucket, `key${n}old`, cb);
            }, next),
            next => createDeleteMarker(s3, testBucket, 'no-orphan-delete-marker', next),
            next => {
                date = new Date(Date.now()).toISOString();
                return async.times(5, (n, cb) => {
                    createOrphanDeleteMarker(s3, testBucket, `key${n}`, cb);
                }, next);
            },
        ], done);
    });

    after(done => async.series([
        next => removeAllVersions({ Bucket: testBucket }, next),
        next => s3.deleteBucket({ Bucket: testBucket }, next),
        next => s3.deleteBucket({ Bucket: emptyBucket }, next),
        next => s3.deleteBucket({ Bucket: nonVersionedBucket }, next),
    ], done));

    it('should return empty list of orphan delete markers if bucket is empty', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: emptyBucket,
            queryObj: { 'list-type': 'orphan' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);

            const data = JSON.parse(response.body);
            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextMarker);
            assert.strictEqual(data.MaxKeys, 1000);
            assert.strictEqual(data.Contents.length, 0);

            return done();
        });
    });

    it('should return empty list of orphan delete markers if prefix does not apply', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'orphan', prefix: 'unknown' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);

            const data = JSON.parse(response.body);
            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextMarker);
            assert.strictEqual(data.MaxKeys, 1000);
            assert.strictEqual(data.Contents.length, 0);

            return done();
        });
    });

    it('should return empty list if max-keys is set to 0', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'orphan', 'max-keys': '0' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);

            const data = JSON.parse(response.body);
            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextMarker);
            assert.strictEqual(data.MaxKeys, 0);
            assert.strictEqual(data.Contents.length, 0);

            return done();
        });
    });

    it('should return InvalidArgument error if max-keys is invalid', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'orphan', 'max-keys': 'a' },
            authCredentials: credentials,
        }, err => {
            assert.strictEqual(err.code, 'InvalidArgument');
            return done();
        });
    });

    it('should return error if bucket does not exist', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: 'idonotexist',
            queryObj: { 'list-type': 'orphan' },
            authCredentials: credentials,
        }, err => {
            assert.strictEqual(err.code, 'NoSuchBucket');
            return done();
        });
    });

    it('should return all the orphan delete markers', done => {
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
            assert(!data.NextMarker);
            assert.strictEqual(data.MaxKeys, 1000);

            const contents = data.Contents;
            assert.strictEqual(contents.length, 8);
            checkContents(contents);

            return done();
        });
    });

    it('should return all the orphan delete markers with prefix key1', done => {
        const prefix = 'key1';

        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'orphan', prefix },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);

            const data = JSON.parse(response.body);
            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextMarker);
            assert.strictEqual(data.MaxKeys, 1000);
            assert.strictEqual(data.Prefix, prefix);

            const contents = data.Contents;
            assert.strictEqual(contents.length, 2);
            checkContents(contents);
            assert.strictEqual(contents[0].Key, 'key1');
            assert.strictEqual(contents[1].Key, 'key1old');

            return done();
        });
    });

    it('should return the orphan delete markers before a defined date', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: {
                'list-type': 'orphan',
                'before-date': date,
            },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);

            const data = JSON.parse(response.body);
            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextMarker);
            assert.strictEqual(data.MaxKeys, 1000);
            assert.strictEqual(data.Contents.length, 3);
            assert.strictEqual(data.BeforeDate, date);

            const contents = data.Contents;
            checkContents(contents);
            assert.strictEqual(contents[0].Key, 'key0old');
            assert.strictEqual(contents[1].Key, 'key1old');
            assert.strictEqual(contents[2].Key, 'key2old');

            return done();
        });
    });

    it('should truncate list of orphan delete markers before a defined date', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: {
                'list-type': 'orphan',
                'before-date': date,
                'max-keys': '1',
            },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);

            const data = JSON.parse(response.body);
            assert.strictEqual(data.IsTruncated, true);
            assert.strictEqual(data.NextMarker, 'key0old');
            assert.strictEqual(data.MaxKeys, 1);
            assert.strictEqual(data.BeforeDate, date);
            assert.strictEqual(data.Contents.length, 1);

            const contents = data.Contents;
            checkContents(contents);
            assert.strictEqual(contents[0].Key, 'key0old');

            return done();
        });
    });

    it('should return the second truncate list of orphan delete markers before a defined date', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'orphan', 'before-date': date, 'max-keys': '1', 'marker': 'key0old' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);

            const data = JSON.parse(response.body);
            assert.strictEqual(data.IsTruncated, true);
            assert.strictEqual(data.Marker, 'key0old');
            assert.strictEqual(data.NextMarker, 'key1old');
            assert.strictEqual(data.MaxKeys, 1);
            assert.strictEqual(data.Contents.length, 1);

            const contents = data.Contents;
            checkContents(contents);
            assert.strictEqual(contents[0].Key, 'key1old');
            assert.strictEqual(data.BeforeDate, date);

            return done();
        });
    });

    it('should return the third truncate list of orphan delete markers before a defined date', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'orphan', 'before-date': date, 'max-keys': '1', 'marker': 'key1old' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);

            const data = JSON.parse(response.body);
            assert.strictEqual(data.IsTruncated, true);
            assert.strictEqual(data.MaxKeys, 1);
            assert.strictEqual(data.Marker, 'key1old');
            assert.strictEqual(data.BeforeDate, date);
            assert.strictEqual(data.NextMarker, 'key2old');

            const contents = data.Contents;
            assert.strictEqual(contents.length, 1);
            checkContents(contents);
            assert.strictEqual(contents[0].Key, 'key2old');

            return done();
        });
    });

    it('should return the fourth and last truncate list of orphan delete markers before a defined date', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'orphan', 'before-date': date, 'max-keys': '1', 'marker': 'key2old' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);

            const data = JSON.parse(response.body);
            assert.strictEqual(data.IsTruncated, false);
            assert.strictEqual(data.MaxKeys, 1);
            assert.strictEqual(data.Marker, 'key2old');
            assert.strictEqual(data.BeforeDate, date);

            const contents = data.Contents;
            assert.strictEqual(contents.length, 0);

            return done();
        });
    });
});
