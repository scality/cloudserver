const assert = require('assert');
const async = require('async');
const { makeRequest } = require('../raw-node/utils/makeRequest');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');
const { removeAllVersions } = require('../aws-node-sdk/lib/utility/versioning-util');

const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';

const testBucket = 'bucket-for-list-lifecycle-noncurrent-tests';
const emptyBucket = 'empty-bucket-for-list-lifecycle-noncurrent-tests';
const nonVersionedBucket = 'non-versioned-bucket-for-list-lifecycle-noncurrent-tests';

/** makeBackbeatRequest - utility function to generate a request going
 * through backbeat route
 * @param {object} params - params for making request
 * @param {string} params.method - request method
 * @param {string} params.bucket - bucket name
 * @param {string} params.subCommand - subcommand to backbeat
 * @param {object} [params.headers] - headers and their string values
 * @param {object} [params.authCredentials] - authentication credentials
 * @param {object} params.authCredentials.accessKey - access key
 * @param {object} params.authCredentials.secretKey - secret key
 * @param {string} [params.requestBody] - request body contents
 * @param {function} callback - with error and response parameters
 * @return {undefined} - and call callback
 */
function makeBackbeatRequest(params, callback) {
    const { method, headers, bucket, authCredentials, queryObj } = params;
    const options = {
        hostname: ipAddress,
        port: 8000,
        method,
        headers,
        authCredentials,
        path: `/_/backbeat/lifecycle/${bucket}`,
        jsonResponse: true,
        queryObj,
    };
    makeRequest(options, callback);
}

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

describe.only('listLifecycleNonCurrents with bucket versioning', () => {
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
                // only the two first ones, since the stale date of the last one (3rd) will be the last-modified
                // of the next one (4th) that is created after the "date".
                expectedKey1VersionIds = res.map(r => r.VersionId).slice(0, 2);
                return next(err);
            }),
            next => async.timesSeries(3, (n, cb) => {
                s3.putObject({ Bucket: testBucket, Key: 'key2', Body: '123', Tagging: 'mykey=myvalue' }, cb);
            }, (err, res) => {
                // only the two first ones, since the stale date of the last one (3rd) will be the last-modified
                // of the next one (4th) that is created after the "date".
                expectedKey2VersionIds = res.map(r => r.VersionId).slice(0, 2);
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
        ], done)
    });

    after(done => {
        return async.series([
            next => removeAllVersions({ Bucket: testBucket }, next),
            next => s3.deleteBucket({ Bucket: testBucket }, next),
            next => s3.deleteBucket({ Bucket: emptyBucket }, next),
            next => s3.deleteBucket({ Bucket: nonVersionedBucket }, next),
        ], done);
    });

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

    it('should return error if bucket does not exist', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: 'idonotexist',
            queryObj: { 'list-type': 'noncurrent' },
            authCredentials: credentials,
        }, (err, response) => {
            assert.strictEqual(err.code, 'NoSuchBucket');
            return done();
        });
    });

    it('should return error if bucket not versioned', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: nonVersionedBucket,
            queryObj: { 'list-type': 'noncurrent' },
            authCredentials: credentials,
        }, (err, response) => {
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
            console.log('contents!!!', contents);
            console.log('date!!!', date);
            checkContents(contents);

            return done();
        });
    });

    it('should return the current versions before a defined date', done => {
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
            assert.strictEqual(data.Contents.length, 4);
            assert.strictEqual(data.BeforeDate, date);

            const contents = data.Contents;
            checkContents(contents);
            
            const key1Versions = contents.filter(c => c.Key === 'key1');
            assert.strictEqual(key1Versions.length, 2);

            const key2Versions = contents.filter(c => c.Key === 'key2');
            assert.strictEqual(key2Versions.length, 2);
    
            assert.deepStrictEqual(key1Versions.map(v => v.VersionId).sort(), expectedKey1VersionIds.sort())
            assert.deepStrictEqual(key2Versions.map(v => v.VersionId).sort(), expectedKey2VersionIds.sort())

            return done();
        });
    });

    // it('should truncate list of current versions before a defined date', done => {
    //     makeBackbeatRequest({
    //         method: 'GET',
    //         bucket: testBucket,
    //         queryObj: { 'list-type': 'noncurrent', 'before-date': date, 'max-keys': '1' },
    //         authCredentials: credentials,
    //     }, (err, response) => {
    //         assert.ifError(err);
    //         assert.strictEqual(response.statusCode, 200);
    //         const data = JSON.parse(response.body);

    //         assert.strictEqual(data.IsTruncated, true);
    //         assert.strictEqual(data.NextKeyMarker, 'oldkey0');
    //         assert.strictEqual(data.MaxKeys, 1);
    //         assert.strictEqual(data.BeforeDate, date);
    //         assert.strictEqual(data.Contents.length, 1);

    //         const contents = data.Contents;
    //         checkContents(contents);
    //         assert.strictEqual(contents[0].Key, 'oldkey0');
    //         return done();
    //     });
    // });

    // it('should get the next truncate list of current versions before a defined date', done => {
    //     makeBackbeatRequest({
    //         method: 'GET',
    //         bucket: testBucket,
    //         queryObj: { 'list-type': 'noncurrent', 'before-date': date, 'max-keys': '1', 'key-marker': 'oldkey0' },
    //         authCredentials: credentials,
    //     }, (err, response) => {
    //         assert.ifError(err);
    //         assert.strictEqual(response.statusCode, 200);
    //         const data = JSON.parse(response.body);

    //         assert.strictEqual(data.IsTruncated, true);
    //         assert.strictEqual(data.KeyMarker, 'oldkey0');
    //         assert.strictEqual(data.NextKeyMarker, 'oldkey1');
    //         assert.strictEqual(data.MaxKeys, 1);
    //         assert.strictEqual(data.Contents.length, 1);

    //         const contents = data.Contents;
    //         checkContents(contents);
    //         assert.strictEqual(contents[0].Key, 'oldkey1');
    //         assert.strictEqual(data.BeforeDate, date);
    //         return done();
    //     });
    // });

    // it('should get the last truncate list of current versions before a defined date', done => {
    //     makeBackbeatRequest({
    //         method: 'GET',
    //         bucket: testBucket,
    //         queryObj: { 'list-type': 'noncurrent', 'before-date': date, 'max-keys': '1', 'key-marker': 'oldkey1' },
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
