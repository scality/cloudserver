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

// runIfMongoV1('listLifecycle if null version', () => {
//     let bucketUtil;
//     let s3;
//     let versionForKey2;

//     before(done => {
//         bucketUtil = new BucketUtility('account1', { signatureVersion: 'v4' });
//         s3 = bucketUtil.s3;

//         return async.series([
//             next => s3.createBucket({ Bucket: testBucket }, next),
//             next => s3.putObject({ Bucket: testBucket, Key: 'key1', Body: '123' }, next),
//             next => s3.putObject({ Bucket: testBucket, Key: 'key2', Body: '123' }, next),
//             next => s3.putBucketVersioning({
//                 Bucket: testBucket,
//                 VersioningConfiguration: { Status: 'Enabled' },
//             }, next),
//             next => s3.putObject({ Bucket: testBucket, Key: 'key1', Body: '123' }, (err, data) => {
//                 if (err) {
//                     return next(err);
//                 }
//                 // delete version to create a null current version for key1.
//                 return s3.deleteObject({ Bucket: testBucket, Key: 'key1', VersionId: data.VersionId }, next);
//             }),
//             next => s3.putObject({ Bucket: testBucket, Key: 'key2', Body: '123' }, (err, data) => {
//                 if (err) {
//                     return next(err);
//                 }
//                 versionForKey2 = data.VersionId;
//                 return next();
//             }),
//         ], done);
//     });

//     after(done => async.series([
//         next => removeAllVersions({ Bucket: testBucket }, next),
//         next => s3.deleteBucket({ Bucket: testBucket }, next),
//     ], done));

//     it('should return the null noncurrent versions', done => {
//         makeBackbeatRequest({
//             method: 'GET',
//             bucket: testBucket,
//             queryObj: { 'list-type': 'noncurrent' },
//             authCredentials: credentials,
//         }, (err, response) => {
//             assert.ifError(err);
//             assert.strictEqual(response.statusCode, 200);
//             const data = JSON.parse(response.body);

//             assert.strictEqual(data.IsTruncated, false);
//             assert(!data.NextKeyMarker);
//             assert.strictEqual(data.MaxKeys, 1000);

//             const contents = data.Contents;
//             assert.strictEqual(contents.length, 1);
//             assert.strictEqual(contents[0].Key, 'key2');
//             assert.strictEqual(contents[0].VersionId, 'null');
//             return done();
//         });
//     });

//     it('should return the null current versions', done => {
//         makeBackbeatRequest({
//             method: 'GET',
//             bucket: testBucket,
//             queryObj: { 'list-type': 'current' },
//             authCredentials: credentials,
//         }, (err, response) => {
//             assert.ifError(err);
//             assert.strictEqual(response.statusCode, 200);
//             const data = JSON.parse(response.body);

//             assert.strictEqual(data.IsTruncated, false);
//             assert(!data.NextKeyMarker);
//             assert.strictEqual(data.MaxKeys, 1000);

//             const contents = data.Contents;
//             assert.strictEqual(contents.length, 2);

//             const firstKey = contents[0];
//             assert.strictEqual(firstKey.Key, 'key1');
//             assert.strictEqual(firstKey.VersionId, 'null');

//             const secondKey = contents[1];
//             assert.strictEqual(secondKey.Key, 'key2');
//             assert.strictEqual(secondKey.VersionId, versionForKey2);
//             return done();
//         });
//     });
// });

// runIfMongoV1('listLifecycle with null current version after versioning suspended', () => {
//     let bucketUtil;
//     let s3;
//     let expectedVersionId;
//     const nullObjectBucket = 'bucket-for-list-lifecycle-current-null-tests';
//     const keyName = 'key0';

//     before(done => {
//         bucketUtil = new BucketUtility('account1', { signatureVersion: 'v4' });
//         s3 = bucketUtil.s3;

//         return async.series([
//             next => s3.createBucket({ Bucket: nullObjectBucket }, next),
//             next => s3.putBucketVersioning({
//                 Bucket: nullObjectBucket,
//                 VersioningConfiguration: { Status: 'Enabled' },
//             }, next),
//             next => s3.putObject({ Bucket: nullObjectBucket, Key: keyName }, (err, data) => {
//                 if (err) {
//                     return next(err);
//                 }
//                 expectedVersionId = data.VersionId;
//                 return next();
//             }),
//             next => s3.putBucketVersioning({
//                 Bucket: nullObjectBucket,
//                 VersioningConfiguration: { Status: 'Suspended' },
//             }, next),
//             next => s3.putObject({ Bucket: nullObjectBucket, Key: keyName }, next),
//         ], done);
//     });

//     after(done => async.series([
//         next => removeAllVersions({ Bucket: nullObjectBucket }, next),
//         next => s3.deleteBucket({ Bucket: nullObjectBucket }, next),
//     ], done));

//     it('should return list of current versions when bucket has a null current version', done => {
//         makeBackbeatRequest({
//             method: 'GET',
//             bucket: nullObjectBucket,
//             queryObj: { 'list-type': 'current' },
//             authCredentials: credentials,
//         }, (err, response) => {
//             assert.ifError(err);
//             assert.strictEqual(response.statusCode, 200);
//             const data = JSON.parse(response.body);

//             assert.strictEqual(data.IsTruncated, false);
//             assert(!data.NextKeyMarker);
//             assert.strictEqual(data.MaxKeys, 1000);
//             assert.strictEqual(data.Contents.length, 1);
//             const key = data.Contents[0];
//             assert.strictEqual(key.Key, keyName);
//             assert.strictEqual(key.VersionId, 'null');
//             return done();
//         });
//     });

//     it('should return list of non-current versions when bucket has a null current version', done => {
//         makeBackbeatRequest({
//             method: 'GET',
//             bucket: nullObjectBucket,
//             queryObj: { 'list-type': 'noncurrent' },
//             authCredentials: credentials,
//         }, (err, response) => {
//             assert.ifError(err);
//             assert.strictEqual(response.statusCode, 200);
//             const data = JSON.parse(response.body);

//             assert.strictEqual(data.IsTruncated, false);
//             assert(!data.NextKeyMarker);
//             assert.strictEqual(data.MaxKeys, 1000);
//             assert.strictEqual(data.Contents.length, 1);
//             const key = data.Contents[0];
//             assert.strictEqual(key.Key, keyName);
//             assert.strictEqual(key.VersionId, expectedVersionId);
//             return done();
//         });
//     });
// });

// runIfMongoV1('listLifecycle with null current versions and version id marker', () => {
//     let bucketUtil;
//     let s3;
//     let expectedKey0VersionId;
//     let expectedKey1VersionId;
//     const bucketName = 'bucket-for-list-lifecycle-null-with-marker-tests';
//     const keyName0 = 'key0';
//     const keyName1 = 'key1';

//     before(done => {
//         bucketUtil = new BucketUtility('account1', { signatureVersion: 'v4' });
//         s3 = bucketUtil.s3;

//         return async.series([
//             next => s3.createBucket({ Bucket: bucketName }, next),
//             next => s3.putBucketVersioning({
//                 Bucket: bucketName,
//                 VersioningConfiguration: { Status: 'Enabled' },
//             }, next),
//             next => s3.putObject({ Bucket: bucketName, Key: keyName0 }, (err, data) => {
//                 if (err) {
//                     return next(err);
//                 }
//                 expectedKey0VersionId = data.VersionId;
//                 return next();
//             }),
//             next => s3.putObject({ Bucket: bucketName, Key: keyName1 }, (err, data) => {
//                 if (err) {
//                     return next(err);
//                 }
//                 expectedKey1VersionId = data.VersionId;
//                 return next();
//             }),
//             next => s3.putBucketVersioning({
//                 Bucket: bucketName,
//                 VersioningConfiguration: { Status: 'Suspended' },
//             }, next),
//             next => s3.putObject({ Bucket: bucketName, Key: keyName0 }, next),
//             next => s3.putObject({ Bucket: bucketName, Key: keyName1 }, next),
//         ], done);
//     });

//     after(done => async.series([
//         next => removeAllVersions({ Bucket: bucketName }, next),
//         next => s3.deleteBucket({ Bucket: bucketName }, next),
//     ], done));

//     it('should return the truncated list of noncurrent version - part 1', done => {
//         makeBackbeatRequest({
//             method: 'GET',
//             bucket: bucketName,
//             queryObj: { 'list-type': 'noncurrent', 'max-keys': '1' },
//             authCredentials: credentials,
//         }, (err, response) => {
//             assert.ifError(err);
//             assert.strictEqual(response.statusCode, 200);
//             const data = JSON.parse(response.body);

//             assert.strictEqual(data.IsTruncated, true);
//             assert.strictEqual(data.NextKeyMarker, keyName0);
//             assert.strictEqual(data.NextVersionIdMarker, expectedKey0VersionId);
//             assert.strictEqual(data.MaxKeys, 1);
//             assert.strictEqual(data.Contents.length, 1);
//             const key = data.Contents[0];
//             assert.strictEqual(key.Key, keyName0);
//             assert.strictEqual(key.VersionId, expectedKey0VersionId);
//             return done();
//         });
//     });

//     it('should return the truncated list of noncurrent version - part 2', done => {
//         makeBackbeatRequest({
//             method: 'GET',
//             bucket: bucketName,
//             queryObj: { 
//                 'list-type': 'noncurrent', 
//                 'max-keys': '1', 
//                 'key-marker': keyName0,
//                 'version-id-marker': expectedKey0VersionId,
//             },
//             authCredentials: credentials,
//         }, (err, response) => {
//             assert.ifError(err);
//             assert.strictEqual(response.statusCode, 200);
//             const data = JSON.parse(response.body);

//             assert.strictEqual(data.IsTruncated, false);
//             assert(!data.NextKeyMarker);
//             assert(!data.NextVersionIdMarker);
//             assert.strictEqual(data.MaxKeys, 1);
//             assert.strictEqual(data.Contents.length, 1);
//             const key = data.Contents[0];
//             assert.strictEqual(key.Key, keyName1);
//             assert.strictEqual(key.VersionId, expectedKey1VersionId);
//             return done();
//         });
//     });
// });

// runIfMongoV1('listLifecycle with null noncurrent versions and version id marker', () => {
//     let bucketUtil;
//     let s3;
//     let versionForKey0;
//     let versionForKey1;

//     before(done => {
//         bucketUtil = new BucketUtility('account1', { signatureVersion: 'v4' });
//         s3 = bucketUtil.s3;

//         return async.series([
//             next => s3.createBucket({ Bucket: testBucket }, next),
//             next => s3.putObject({ Bucket: testBucket, Key: 'key0', Body: '123' }, next),
//             next => s3.putObject({ Bucket: testBucket, Key: 'key1', Body: '123' }, next),
//             next => s3.putBucketVersioning({
//                 Bucket: testBucket,
//                 VersioningConfiguration: { Status: 'Enabled' },
//             }, next),
//             next => s3.putObject({ Bucket: testBucket, Key: 'key0', Body: '123' }, (err, data) => {
//                 if (err) {
//                     return next(err);
//                 }
//                 versionForKey0 = data.VersionId;
//                 return next();
//             }),
//             next => s3.putObject({ Bucket: testBucket, Key: 'key1', Body: '123' }, (err, data) => {
//                 if (err) {
//                     return next(err);
//                 }
//                 versionForKey1 = data.VersionId;
//                 return next();
//             }),
//         ], done);
//     });

//     after(done => async.series([
//         next => removeAllVersions({ Bucket: testBucket }, next),
//         next => s3.deleteBucket({ Bucket: testBucket }, next),
//     ], done));

//     // it('list versions', done => {
//     //     s3.listObjectVersions({ Bucket: testBucket, MaxKeys: 2, KeyMarker: 'key0', VersionIdMarker: 'null' }, (err, data) => {
//     //         console.log('data!!!', data);
//     //         done();
//     //     });
//     // });

//     // it('should return the truncated list of null noncurrent versions', done => {
//     //     makeBackbeatRequest({
//     //         method: 'GET',
//     //         bucket: testBucket,
//     //         queryObj: { 
//     //             'list-type': 'noncurrent', 
//     //             'max-keys': '1', 
//     //             'key-marker': 'key0',
//     //             'version-id-marker': 'ok',  
//     //         },
//     //         authCredentials: credentials,
//     //     }, (err, response) => {
//     //         assert.ifError(err);
//     //         assert.strictEqual(response.statusCode, 200);
//     //         const data = JSON.parse(response.body);

//     //         console.log('data!!!', data);
//     //         done();
//     //     });
//     // });


//     it('should return the truncated list of null noncurrent versions', done => {
//         makeBackbeatRequest({
//             method: 'GET',
//             bucket: testBucket,
//             queryObj: { 'list-type': 'noncurrent', 'max-keys': '1' },
//             authCredentials: credentials,
//         }, (err, response) => {
//             assert.ifError(err);
//             assert.strictEqual(response.statusCode, 200);
//             const data = JSON.parse(response.body);

//             console.log('data!!!', data);

//             assert.strictEqual(data.IsTruncated, true);
//             assert.strictEqual(data.NextKeyMarker, 'key0');
//             // assert.strictEqual(data.NextVersionIdMarker, versionForKey1);
//             assert.strictEqual(data.MaxKeys, 1);

//             const contents = data.Contents;
//             assert.strictEqual(contents.length, 1);
//             assert.strictEqual(contents[0].Key, 'key0');
//             assert.strictEqual(contents[0].VersionId, 'null');

//             const nextVersionIdMarker = data.NextVersionIdMarker;
//             makeBackbeatRequest({
//                 method: 'GET',
//                 bucket: testBucket,
//                 queryObj: { 
//                     'list-type': 'noncurrent',
//                     'max-keys': '1',
//                     'key-marker': 'key0',
//                     'version-id-marker': nextVersionIdMarker,
//                 },
//                 authCredentials: credentials,
//             }, (err, response) => {
//                 assert.ifError(err);
//                 assert.strictEqual(response.statusCode, 200);
//                 const data = JSON.parse(response.body);
    
//                 assert.strictEqual(data.IsTruncated, false);
//                 assert(!data.NextKeyMarker);
//                 assert(!data.NextVersionIdMarker);
//                 assert.strictEqual(data.MaxKeys, 1);
    
//                 const contents = data.Contents;
//                 assert.strictEqual(contents.length, 1);
//                 assert.strictEqual(contents[0].Key, 'key1');
//                 assert.strictEqual(contents[0].VersionId, 'null');
//                 return done();
//             });
//         });
//     });
// });


describe.only('listLifecycle with null noncurrent', () => {
    let bucketUtil;
    let s3;
    let versionForKey01;
    let versionForKey02;

    before(done => {
        bucketUtil = new BucketUtility('account1', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;

        return async.series([
            next => s3.createBucket({ Bucket: testBucket }, next),
            next => s3.putObject({ Bucket: testBucket, Key: 'key0', Body: '123' }, next),
            next => s3.putBucketVersioning({
                Bucket: testBucket,
                VersioningConfiguration: { Status: 'Enabled' },
            }, next),
            next => s3.putObject({ Bucket: testBucket, Key: 'key0', Body: '123' }, (err, data) => {
                if (err) {
                    return next(err);
                }
                versionForKey01 = data.VersionId;
                return next();
            }),
            next => s3.putObject({ Bucket: testBucket, Key: 'key0', Body: '123' }, (err, data) => {
                if (err) {
                    return next(err);
                }
                versionForKey02 = data.VersionId;
                return next();
            }),
        ], done);
    });

    after(done => async.series([
        next => removeAllVersions({ Bucket: testBucket }, next),
        next => s3.deleteBucket({ Bucket: testBucket }, next),
    ], done));


    // it('should return the truncated list of null noncurrent versions', done => {
    //     makeBackbeatRequest({
    //         method: 'GET',
    //         bucket: testBucket,
    //         queryObj: { 'list-type': 'noncurrent', 'max-keys': '1' },
    //         authCredentials: credentials,
    //     }, (err, response) => {
    //         assert.ifError(err);
    //         assert.strictEqual(response.statusCode, 200);
    //         const data = JSON.parse(response.body);

    //         console.log('data!!!', data);

    //         assert.strictEqual(data.IsTruncated, true);
    //         assert.strictEqual(data.NextKeyMarker, 'key0');
    //         assert.strictEqual(data.NextVersionIdMarker, versionForKey01);
    //         assert.strictEqual(data.MaxKeys, 1);

    //         const contents = data.Contents;
    //         assert.strictEqual(contents.length, 1);
    //         assert.strictEqual(contents[0].Key, 'key0');
    //         assert.strictEqual(contents[0].VersionId, versionForKey01);

    //         return done();
    //     });
    // });

    it('should return the truncated list of null noncurrent versions', done => {
        makeBackbeatRequest({
            method: 'GET',
            bucket: testBucket,
            queryObj: { 'list-type': 'noncurrent', 'max-keys': '1', 'key-marker': 'key0',
            'version-id-marker': versionForKey01, },
            authCredentials: credentials,
        }, (err, response) => {
            assert.ifError(err);
            assert.strictEqual(response.statusCode, 200);
            const data = JSON.parse(response.body);

            console.log('data!!!', data);

            assert.strictEqual(data.IsTruncated, false);
            assert(!data.NextKeyMarker);
            assert(!data.NextVersionIdMarker);
            assert.strictEqual(data.MaxKeys, 1);

            const contents = data.Contents;
            assert.strictEqual(contents.length, 1);
            assert.strictEqual(contents[0].Key, 'key0');
            assert.strictEqual(contents[0].VersionId, 'null');

            return done();
        });
    });
});
