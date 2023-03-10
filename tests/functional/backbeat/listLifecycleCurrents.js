const assert = require('assert');
const async = require('async');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');
const { removeAllVersions } = require('../aws-node-sdk/lib/utility/versioning-util');
const { makeBackbeatRequest, runIfMongoV1 } = require('./utils');

const testBucket = 'bucket-for-list-lifecycle-current-tests';
const emptyBucket = 'empty-bucket-for-list-lifecycle-current-tests';

const credentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

function checkContents(contents) {
    contents.forEach(d => {
        assert(d.Key);
        assert(d.LastModified);
        assert(d.Etag);
        assert(d.Owner.DisplayName);
        assert(d.Owner.ID);
        assert(d.StorageClass);
        assert.strictEqual(d.StorageClass, 'STANDARD');
        assert.deepStrictEqual(d.TagSet, [{
            Key: 'mykey',
            Value: 'myvalue',
        }]);
        assert.strictEqual(d.IsLatest, true);
        assert.strictEqual(d.DataStoreName, 'us-east-1');
        assert.strictEqual(d.ListType, 'current');
        assert.strictEqual(d.Size, 3);
    });
}

['Enabled', 'Disabled'].forEach(versioning => {
    runIfMongoV1(`listLifecycleCurrents with bucket versioning ${versioning}`, () => {
        let bucketUtil;
        let s3;
        let date;

        before(done => {
            bucketUtil = new BucketUtility('account1', { signatureVersion: 'v4' });
            s3 = bucketUtil.s3;

            return async.series([
                next => s3.createBucket({ Bucket: testBucket }, next),
                next => s3.createBucket({ Bucket: emptyBucket }, next),
                next => {
                    if (versioning !== 'Enabled') {
                        return process.nextTick(next);
                    }
                    return s3.putBucketVersioning({
                        Bucket: testBucket,
                        VersioningConfiguration: { Status: 'Enabled' },
                    }, next);
                },
                next => {
                    if (versioning !== 'Enabled') {
                        return process.nextTick(next);
                    }
                    return s3.putBucketVersioning({
                        Bucket: emptyBucket,
                        VersioningConfiguration: { Status: 'Enabled' },
                    }, next);
                },
                next => async.times(3, (n, cb) => {
                    s3.putObject({ Bucket: testBucket, Key: `oldkey${n}`, Body: '123', Tagging: 'mykey=myvalue' }, cb);
                }, next),
                next => {
                    date = new Date(Date.now()).toISOString();
                    return async.times(5, (n, cb) => {
                        s3.putObject({ Bucket: testBucket, Key: `key${n}`, Body: '123', Tagging: 'mykey=myvalue' }, cb);
                    }, next);
                },
            ], done);
        });

        after(done => async.series([
            next => removeAllVersions({ Bucket: testBucket }, next),
            next => s3.deleteBucket({ Bucket: testBucket }, next),
            next => s3.deleteBucket({ Bucket: emptyBucket }, next),
        ], done));

        it('should return empty list of current versions if bucket is empty', done => {
            makeBackbeatRequest({
                method: 'GET',
                bucket: emptyBucket,
                queryObj: { 'list-type': 'current' },
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

        it('should return empty list of current versions if prefix does not apply', done => {
            makeBackbeatRequest({
                method: 'GET',
                bucket: testBucket,
                queryObj: { 'list-type': 'current', prefix: 'unknown' },
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
                queryObj: { 'list-type': 'current', 'max-keys': '0' },
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

        it('should return NoSuchBucket error if bucket does not exist', done => {
            makeBackbeatRequest({
                method: 'GET',
                bucket: 'idonotexist',
                queryObj: { 'list-type': 'current' },
                authCredentials: credentials,
            }, err => {
                assert.strictEqual(err.code, 'NoSuchBucket');
                return done();
            });
        });

        it('should return InvalidArgument error if max-keys is invalid', done => {
            makeBackbeatRequest({
                method: 'GET',
                bucket: testBucket,
                queryObj: { 'list-type': 'current', 'max-keys': 'a' },
                authCredentials: credentials,
            }, err => {
                assert.strictEqual(err.code, 'InvalidArgument');
                return done();
            });
        });

        it('should return all the current versions', done => {
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
                assert(!data.NextMarker);
                assert.strictEqual(data.MaxKeys, 1000);

                const contents = data.Contents;
                assert.strictEqual(contents.length, 8);
                checkContents(contents);

                return done();
            });
        });

        it('should return all the current versions with prefix old', done => {
            const prefix = 'old';

            makeBackbeatRequest({
                method: 'GET',
                bucket: testBucket,
                queryObj: { 'list-type': 'current', prefix },
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
                assert.strictEqual(contents.length, 3);
                checkContents(contents);

                return done();
            });
        });

        it('should return the current versions before a defined date', done => {
            makeBackbeatRequest({
                method: 'GET',
                bucket: testBucket,
                queryObj: { 'list-type': 'current', 'before-date': date },
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
                assert.strictEqual(contents[0].Key, 'oldkey0');
                assert.strictEqual(contents[1].Key, 'oldkey1');
                assert.strictEqual(contents[2].Key, 'oldkey2');
                return done();
            });
        });

        it('should truncate list of current versions before a defined date', done => {
            makeBackbeatRequest({
                method: 'GET',
                bucket: testBucket,
                queryObj: { 'list-type': 'current', 'before-date': date, 'max-keys': '1' },
                authCredentials: credentials,
            }, (err, response) => {
                assert.ifError(err);
                assert.strictEqual(response.statusCode, 200);
                const data = JSON.parse(response.body);

                assert.strictEqual(data.IsTruncated, true);
                assert.strictEqual(data.NextMarker, 'oldkey0');
                assert.strictEqual(data.MaxKeys, 1);
                assert.strictEqual(data.BeforeDate, date);
                assert.strictEqual(data.Contents.length, 1);

                const contents = data.Contents;
                checkContents(contents);
                assert.strictEqual(contents[0].Key, 'oldkey0');
                return done();
            });
        });

        it('should return the next truncate list of current versions before a defined date', done => {
            makeBackbeatRequest({
                method: 'GET',
                bucket: testBucket,
                queryObj: { 'list-type': 'current', 'before-date': date, 'max-keys': '1', 'marker': 'oldkey0' },
                authCredentials: credentials,
            }, (err, response) => {
                assert.ifError(err);
                assert.strictEqual(response.statusCode, 200);
                const data = JSON.parse(response.body);

                assert.strictEqual(data.IsTruncated, true);
                assert.strictEqual(data.Marker, 'oldkey0');
                assert.strictEqual(data.NextMarker, 'oldkey1');
                assert.strictEqual(data.MaxKeys, 1);
                assert.strictEqual(data.Contents.length, 1);

                const contents = data.Contents;
                checkContents(contents);
                assert.strictEqual(contents[0].Key, 'oldkey1');
                assert.strictEqual(data.BeforeDate, date);
                return done();
            });
        });

        it('should return the last truncate list of current versions before a defined date', done => {
            makeBackbeatRequest({
                method: 'GET',
                bucket: testBucket,
                queryObj: { 'list-type': 'current', 'before-date': date, 'max-keys': '1', 'marker': 'oldkey1' },
                authCredentials: credentials,
            }, (err, response) => {
                assert.ifError(err);
                assert.strictEqual(response.statusCode, 200);
                const data = JSON.parse(response.body);

                assert.strictEqual(data.IsTruncated, false);
                assert.strictEqual(data.MaxKeys, 1);
                assert.strictEqual(data.Marker, 'oldkey1');
                assert.strictEqual(data.BeforeDate, date);

                const contents = data.Contents;
                assert.strictEqual(contents.length, 1);
                checkContents(contents);
                assert.strictEqual(contents[0].Key, 'oldkey2');
                return done();
            });
        });
    });
});
