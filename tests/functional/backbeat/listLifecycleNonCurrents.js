const assert = require('assert');
const async = require('async');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');
const { removeAllVersions } = require('../aws-node-sdk/lib/utility/versioning-util');
const { makeBackbeatRequest, runIfMongoV1 } = require('./utils');

const testBucket = 'bucket-for-list-lifecycle-noncurrent-tests';
const emptyBucket = 'empty-bucket-for-list-lifecycle-noncurrent-tests';
const nonVersionedBucket = 'non-versioned-bucket-for-list-lifecycle-noncurrent-tests';

const credentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

function checkContents(contents) {
    contents.forEach(d => {
        assert(d.Key);
        assert(d.LastModified);
        assert(d.ETag);
        assert(d.Owner.DisplayName);
        assert(d.Owner.ID);
        assert(d.StorageClass);
        assert.strictEqual(d.StorageClass, 'STANDARD');
        assert(d.VersionId);
        assert(d.staleDate);
        assert(!d.IsLatest);
        assert.deepStrictEqual(d.TagSet, [{
            Key: 'mykey',
            Value: 'myvalue',
        }]);
        assert.strictEqual(d.DataStoreName, 'us-east-1');
        assert.strictEqual(d.ListType, 'noncurrent');
        assert.strictEqual(d.Size, 3);
    });
}

runIfMongoV1('listLifecycleNonCurrents', () => {
    let bucketUtil;
    let s3;
    let date;
    let expectedKey1VersionIds = [];
    let expectedKey2VersionIds = [];

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
            next => async.timesSeries(3, (n, cb) => {
                s3.putObject({ Bucket: testBucket, Key: 'key1', Body: '123', Tagging: 'mykey=myvalue' }, cb);
            }, (err, res) => {
                // Only the two first ones are kept, since the stale date of the last one (3rd)
                // Will be the last-modified of the next one (4th) that is created after the "date".
                // The array is reverse since, for a specific key, we expect the listing to be ordered
                // by last-modified date in descending order due to the way version id is generated.
                expectedKey1VersionIds = res.map(r => r.VersionId).slice(0, 2).reverse();
                return next(err);
            }),
            next => async.timesSeries(3, (n, cb) => {
                s3.putObject({ Bucket: testBucket, Key: 'key2', Body: '123', Tagging: 'mykey=myvalue' }, cb);
            }, (err, res) => {
                // Only the two first ones are kept, since the stale date of the last one (3rd)
                // Will be the last-modified of the next one (4th) that is created after the "date".
                // The array is reverse since, for a specific key, we expect the listing to be ordered
                // by last-modified date in descending order due to the way version id is generated.
                expectedKey2VersionIds = res.map(r => r.VersionId).slice(0, 2).reverse();
                return next(err);
            }),
            next => {
                date = new Date(Date.now()).toISOString();
                return async.times(5, (n, cb) => {
                    s3.putObject({ Bucket: testBucket, Key: 'key1', Body: '123', Tagging: 'mykey=myvalue' }, cb);
                }, next);
            },
            next => async.times(5, (n, cb) => {
                s3.putObject({ Bucket: testBucket, Key: 'key2', Body: '123', Tagging: 'mykey=myvalue' }, cb);
            }, next),
        ], done);
    });

    after(done => async.series([
        next => removeAllVersions({ Bucket: testBucket }, next),
        next => s3.deleteBucket({ Bucket: testBucket }, next),
        next => s3.deleteBucket({ Bucket: emptyBucket }, next),
        next => s3.deleteBucket({ Bucket: nonVersionedBucket }, next),
    ], done));

    it('should return empty list of noncurrent versions if bucket is empty', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: emptyBucket,
            queryObj: { 'list-type': 'noncurrent' },
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

    it('should return empty list of noncurrent versions if prefix does not apply', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'noncurrent', prefix: 'unknown' },
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

    it('should return empty list if max-keys is set to 0', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'noncurrent', 'max-keys': '0' },
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

    it('should return error if bucket does not exist', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: 'idonotexist',
            queryObj: { 'list-type': 'noncurrent' },
            authCredentials: credentials,
        }, err => {
            assert.strictEqual(err.code, 'NoSuchBucket');
            return done();
        });
    });

    it('should return BadRequest error if list type is empty', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': '' },
            authCredentials: credentials,
        }, err => {
            assert.strictEqual(err.code, 'BadRequest');
            return done();
        });
    });

    it('should return BadRequest error if list type is invalid', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'invalid' },
            authCredentials: credentials,
        }, err => {
            assert.strictEqual(err.code, 'BadRequest');
            return done();
        });
    });

    it('should return InvalidArgument error if max-keys is invalid', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'noncurrent', 'max-keys': 'a' },
            authCredentials: credentials,
        }, err => {
            assert.strictEqual(err.code, 'InvalidArgument');
            return done();
        });
    });

    it('should return error if bucket not versioned', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: nonVersionedBucket,
            queryObj: { 'list-type': 'noncurrent' },
            authCredentials: credentials,
        }, err => {
            assert.strictEqual(err.code, 'InvalidRequest');
            return done();
        });
    });

    it('should return all the noncurrent versions', done => {
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
            assert.strictEqual(contents.length, 14);
            checkContents(contents);

            return done();
        });
    });

    it('should return all the noncurrent versions with prefix key1', done => {
        const prefix = 'key1';

        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'noncurrent', prefix },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextKeyMarker);
            assert.strictEqual(data.MaxKeys, 1000);
            assert.strictEqual(data.Prefix, prefix);

            const contents = data.Contents;
            assert.strictEqual(contents.length, 7);
            assert(contents.every(d => d.Key === 'key1'));
            checkContents(contents);

            return done();
        });
    });

    it('should return all the noncurrent versions with prefix key1 before a defined date', done => {
        const prefix = 'key1';

        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'noncurrent', prefix, 'before-date': date  },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextKeyMarker);
            assert.strictEqual(data.MaxKeys, 1000);
            assert.strictEqual(data.Prefix, prefix);

            const contents = data.Contents;
            assert.strictEqual(contents.length, 2);
            assert(contents.every(d => d.Key === 'key1'));

            assert.deepStrictEqual(contents.map(v => v.VersionId), expectedKey1VersionIds);

            checkContents(contents);

            return done();
        });
    });

    it('should return the noncurrent version with prefix key1, before a defined date, and after marker', done => {
        const prefix = 'key2';

        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: {
                'list-type': 'noncurrent',
                prefix,
                'before-date': date,
                'key-marker': 'key1',
            },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextKeyMarker);
            assert.strictEqual(data.MaxKeys, 1000);
            assert.strictEqual(data.Prefix, prefix);

            const contents = data.Contents;
            assert.strictEqual(contents.length, 2);
            assert(contents.every(d => d.Key === 'key2'));

            assert.deepStrictEqual(contents.map(v => v.VersionId), expectedKey2VersionIds);

            checkContents(contents);

            return done();
        });
    });

    it('should return the noncurrent version with prefix key1, before a defined date, and after marker', done => {
        const prefix = 'key2';

        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: {
                'list-type': 'noncurrent',
                prefix,
                'before-date': date,
                'key-marker': 'key2',
                'version-id-marker': expectedKey2VersionIds[0]
            },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextKeyMarker);
            assert.strictEqual(data.MaxKeys, 1000);
            assert.strictEqual(data.Prefix, prefix);

            const contents = data.Contents;
            assert.strictEqual(contents.length, 1);
            assert(contents.every(d => d.Key === 'key2'));
            contents[0].Key = 'key2';
            contents[0].VersionId = expectedKey2VersionIds[1];

            checkContents(contents);

            return done();
        });
    });

    it('should return the non current versions before a defined date', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'noncurrent', 'before-date': date },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextKeyMarker);
            assert.strictEqual(data.MaxKeys, 1000);
            assert.strictEqual(data.BeforeDate, date);

            const contents = data.Contents;
            assert.strictEqual(contents.length, 4);
            checkContents(contents);

            const key1Versions = contents.filter(c => c.Key === 'key1');
            assert.strictEqual(key1Versions.length, 2);

            const key2Versions = contents.filter(c => c.Key === 'key2');
            assert.strictEqual(key2Versions.length, 2);

            assert.deepStrictEqual(key1Versions.map(v => v.VersionId), expectedKey1VersionIds);
            assert.deepStrictEqual(key2Versions.map(v => v.VersionId), expectedKey2VersionIds);

            return done();
        });
    });

    it('should truncate list of non current versions before a defined date', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'noncurrent', 'before-date': date, 'max-keys': '1' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, true);
            assert.strictEqual(data.NextKeyMarker, 'key1');
            assert.strictEqual(data.NextVersionIdMarker, expectedKey1VersionIds[0]);
            assert.strictEqual(data.MaxKeys, 1);
            assert.strictEqual(data.BeforeDate, date);
            assert.strictEqual(data.Contents.length, 1);

            const contents = data.Contents;
            checkContents(contents);
            assert.strictEqual(contents[0].Key, 'key1');
            assert.strictEqual(contents[0].VersionId, expectedKey1VersionIds[0]);
            return done();
        });
    });

    it('should return the first following list of noncurrent versions before a defined date', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: {
                'list-type': 'noncurrent',
                'before-date': date,
                'max-keys': '1',
                'key-marker': 'key1',
                'version-id-marker': expectedKey1VersionIds[0]
            },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, true);
            assert.strictEqual(data.KeyMarker, 'key1');
            assert.strictEqual(data.VersionIdMarker, expectedKey1VersionIds[0]);
            assert.strictEqual(data.NextKeyMarker, 'key1');
            assert.strictEqual(data.NextVersionIdMarker, expectedKey1VersionIds[1]);
            assert.strictEqual(data.MaxKeys, 1);
            assert.strictEqual(data.BeforeDate, date);
            assert.strictEqual(data.Contents.length, 1);

            const contents = data.Contents;
            checkContents(contents);
            assert.strictEqual(contents[0].Key, 'key1');
            assert.strictEqual(contents[0].VersionId, expectedKey1VersionIds[1]);
            return done();
        });
    });

    it('should return the second following list of noncurrent versions before a defined date', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: {
                'list-type': 'noncurrent',
                'before-date': date,
                'max-keys': '1',
                'key-marker': 'key1',
                'version-id-marker': expectedKey1VersionIds[1]
            },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, true);
            assert.strictEqual(data.KeyMarker, 'key1');
            assert.strictEqual(data.VersionIdMarker, expectedKey1VersionIds[1]);
            assert.strictEqual(data.NextKeyMarker, 'key2');
            assert.strictEqual(data.NextVersionIdMarker, expectedKey2VersionIds[0]);
            assert.strictEqual(data.MaxKeys, 1);
            assert.strictEqual(data.BeforeDate, date);
            assert.strictEqual(data.Contents.length, 1);

            const contents = data.Contents;
            checkContents(contents);
            assert.strictEqual(contents[0].Key, 'key2');
            assert.strictEqual(contents[0].VersionId, expectedKey2VersionIds[0]);
            return done();
        });
    });

    it('should return the last and third following list of noncurrent versions before a defined date', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: {
                'list-type': 'noncurrent',
                'before-date': date,
                'max-keys': '1',
                'key-marker': 'key2',
                'version-id-marker': expectedKey2VersionIds[0]
            },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            assert.strictEqual(data.IsTruncated, false);
            assert.strictEqual(data.KeyMarker, 'key2');
            assert.strictEqual(data.VersionIdMarker, expectedKey2VersionIds[0]);
            assert(!data.NextKeyMarker);
            assert(!data.NextVersionIdMarker);
            assert.strictEqual(data.MaxKeys, 1);
            assert.strictEqual(data.BeforeDate, date);
            assert.strictEqual(data.Contents.length, 1);

            const contents = data.Contents;
            checkContents(contents);
            assert.strictEqual(contents[0].Key, 'key2');
            assert.strictEqual(contents[0].VersionId, expectedKey2VersionIds[1]);
            return done();
        });
    });
});
