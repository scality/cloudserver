const assert = require('assert');
const async = require('async');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');
const { removeAllVersions } = require('../aws-node-sdk/lib/utility/versioning-util');
const { makeBackbeatRequest } = require('./utils');

const testBucket = 'bucket-for-list-lifecycle-current-tests';
const emptyBucket = 'empty-bucket-for-list-lifecycle-current-tests';

const credentials = {
    accessKey: 'WLI8X7JGPU1AWQEQIKM5',
    secretKey: '0Src2X+kIrR1SUo/NhR5o1V4hqU1dtlePBHAcCbV',
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
    describe(`listLifecycleCurrents with bucket versioning ${versioning}`, () => {
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
                assert(!data.NextKeyMarker);
                assert.strictEqual(data.MaxKeys, 1000);
                assert.strictEqual(data.Contents.length, 0);
                return done();
            });
        });

        it('should return error if bucket does not exist', done => {
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
                assert(!data.NextKeyMarker);
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
                assert(!data.NextKeyMarker);
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
                assert(!data.NextKeyMarker);
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
                assert.strictEqual(data.NextKeyMarker, 'oldkey0');
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
                queryObj: { 'list-type': 'current', 'before-date': date, 'max-keys': '1', 'key-marker': 'oldkey0' },
                authCredentials: credentials,
            }, (err, response) => {
                assert.ifError(err);
                assert.strictEqual(response.statusCode, 200);
                const data = JSON.parse(response.body);

                assert.strictEqual(data.IsTruncated, true);
                assert.strictEqual(data.KeyMarker, 'oldkey0');
                assert.strictEqual(data.NextKeyMarker, 'oldkey1');
                assert.strictEqual(data.MaxKeys, 1);
                assert.strictEqual(data.Contents.length, 1);

                const contents = data.Contents;
                checkContents(contents);
                assert.strictEqual(contents[0].Key, 'oldkey1');
                assert.strictEqual(data.BeforeDate, date);
                return done();
            });
        });

        // it('should return the last truncate list of current versions before a defined date', done => {
        //     makeBackbeatRequest({
        //         method: 'GET',
        //         bucket: testBucket,
        //         queryObj: { 'list-type': 'current', 'before-date': date, 'max-keys': '1', 'key-marker': 'oldkey1' },
        //         authCredentials: credentials,
        //     }, (err, response) => {
        //         assert.ifError(err);
        //         assert.strictEqual(response.statusCode, 200);
        //         const data = JSON.parse(response.body);

        //         assert.strictEqual(data.IsTruncated, false);
        //         assert.strictEqual(data.MaxKeys, 1);
        //         assert.strictEqual(data.KeyMarker, 'oldkey1');
        //         assert.strictEqual(data.BeforeDate, date);

        //         const contents = data.Contents;
        //         assert.strictEqual(contents.length, 1);
        //         checkContents(contents);
        //         assert.strictEqual(contents[0].Key, 'oldkey2');
        //         return done();
        //     });
        // });
    });
});
