const { lifecycleClient, s3Client } = require('./utils/sdks');
// const { runAndCheckSearch, runIfMongo } = require('./utils/helpers');
const { mongoClient } = require('../../../utilities/mongoClient');
const async = require('async');
const assert = require('assert');

const firstObjectKey = 'first';
const secondObjectKey = 'second';
const thirdObjectKey = 'third';

const tagKey = 'item-type';
const tagValue = 'main';
const objectTagData = `${tagKey}=${tagValue}`;

const userName = 'Bart';
const userCanonicalId = '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
const owner = {
    DisplayName: userName,
    ID: userCanonicalId,
};

const expectedFirstObject = {
    Key: firstObjectKey,
    Owner: owner,
    Size: 0,
    StorageClass: 'STANDARD',
    TagSet: [{
        Key: tagKey,
        Value: tagValue,
    }],
};

const expectedSecondObject = {
    Key: secondObjectKey,
    Owner: owner,
    Size: 0,
    StorageClass: 'STANDARD',
    TagSet: [],
};

const expectedThirdObject = {
    Key: thirdObjectKey,
    Owner: owner,
    Size: 0,
    StorageClass: 'STANDARD',
    TagSet: [],
};

const runIfMongo = process.env.S3METADATA === 'mongodb' ?
    describe : describe.skip;

function check(data, expected) {
    Object.keys(expected).forEach(
        r => {
            if (r === 'Contents') {
                assert.strictEqual(data.Contents.length, expected.Contents.length);
                expected.Contents.forEach((content, i) => {
                    Object.keys(content).forEach(k => {
                        assert.deepStrictEqual(data.Contents[i][k], content[k], `Contents[${i}].${k} value is invalid`);
                    })
                });
            } else {
                assert.strictEqual(data[r], expected[r], `${r} value is invalid`);
            }
        });
}

runIfMongo('Basic search', () => {
    const bucketName = `basicsearchmebucket${Date.now()}`;
    let startDate;
    let firstDoneDate;
    let secondDoneDate;
    let thirdDoneDate;
    before(done =>
        async.series([
            next => s3Client.createBucket({ Bucket: bucketName }, err => { 
                startDate = new Date().toISOString();
                return next(err); 
            }),
            next => s3Client.putObject({ Bucket: bucketName, Key: firstObjectKey, Tagging: objectTagData }, err => {
                firstDoneDate = new Date().toISOString();
                return next(err);
            }),
            next => s3Client.putObject({ Bucket: bucketName, Key: secondObjectKey}, err => {
                secondDoneDate = new Date().toISOString();
                return next(err);
            }),
            next => s3Client.putObject({ Bucket: bucketName, Key: thirdObjectKey}, err => {
                thirdDoneDate = new Date().toISOString();
                return next(err);
            }),
        ], done));

    after(done => {
        s3Client.deleteObjects({ Bucket: bucketName, Delete: { Objects: [
            { Key: firstObjectKey },
            { Key: secondObjectKey },
            { Key: thirdObjectKey }],
        } },
            err => {
                if (err) {
                    return done(err);
                }
                return s3Client.deleteBucket({ Bucket: bucketName }, done);
            });
    });

    // TODO test keyMarker != key
    // TODO test so NextkeyMarker === last key

    it('should return all three objects', done => {
        return lifecycleClient.listLifecycleObjects({ 
            Bucket: bucketName, 
        }, (err, data) => {
            if (err) {
                return done(err);
            }
            const expected = {
                IsTruncated: false,
                Contents: [
                    expectedFirstObject, expectedSecondObject, expectedThirdObject
                ],
                Name: bucketName,
                MaxKeys: 1000,
            }
            check(data, expected);
            return done();
        });
    });

    it('beforeDate: should return empty list', done => {
        return lifecycleClient.listLifecycleObjects({ 
            Bucket: bucketName, 
            BeforeDate: startDate,
        }, (err, data) => {
            if (err) {
                return done(err);
            }
            const expected = {
                IsTruncated: false,
                Contents: [],
                Name: bucketName,
                MaxKeys: 1000,
            }
            check(data, expected);
            return done();
        });
    });

    it('beforeDate: should return the first object', done => {
        return lifecycleClient.listLifecycleObjects({ 
            Bucket: bucketName, 
            BeforeDate: firstDoneDate,
        }, (err, data) => {
            if (err) {
                return done(err);
            }
            const expected = {
                IsTruncated: false,
                Contents: [expectedFirstObject],
                Name: bucketName,
                MaxKeys: 1000,
            }
            check(data, expected);
            return done();
        });
    });

    it('beforeDate: should return the first and second object', done => {
        return lifecycleClient.listLifecycleObjects({ 
            Bucket: bucketName, 
            BeforeDate: secondDoneDate,
        }, (err, data) => {
            if (err) {
                return done(err);
            }
            const expected = {
                IsTruncated: false,
                Contents: [expectedFirstObject, expectedSecondObject],
                Name: bucketName,
                MaxKeys: 1000,
            }
            check(data, expected);
            return done();
        });
    });

});

// runIfMongo('Basic search 2', () => {
//     const bucketName = `basicsearchmebucket${Date.now()}`;
//     let startDate;
//     let afterFirstDate;
//     let afterSecondDate;
//     let afterThirdDate;
//     before(done => {
//         mongoClient.connectClient(err => {
//             s3Client.createBucket({ Bucket: bucketName }, err => {
//                 if (err) {
//                     return done(err);
//                 }
//                 startDate = new Date().toISOString();
//                 async.each([firstObjectKey, secondObjectKey, thirdObjectKey], (objectName, cb) => {
//                     return s3Client.putObject({ Bucket: bucketName, Key: objectName}, cb);
//                 }, err => {
//                     if (err) {
//                         return done(err);
//                     }
//                     return mongoClient.matchObjectsLastModified(bucketName, '2022-12-24T16:55:36.762Z', err => {
//                         if (err) {
//                             return done(err);
//                         }
//                         return s3Client.putObject({ Bucket: bucketName, Key: 'four'}, done);
//                     });
//                 });
//             });
//         })
//     });

//     after(done => {
//         s3Client.deleteObjects({ Bucket: bucketName, Delete: { Objects: [
//             { Key: firstObjectKey },
//             { Key: secondObjectKey },
//             { Key: thirdObjectKey },
//             { Key: 'four' },
//         ],
//         } },
//             err => {
//                 if (err) {
//                     return done(err);
//                 }
//                 return s3Client.deleteBucket({ Bucket: bucketName }, err => {
//                     if (err) {
//                         return done(err);
//                     }
//                     mongoClient.disconnectClient(done);
//                 });
//             });
//     });

//     it('should list lifecycle objects', done => {
//         return lifecycleClient.listLifecycleObjects({ 
//             Bucket: bucketName,
//         }, (err, data) => {
//             console.log('TOTAL err!!!', err);
//             console.log('TOTAL data!!!', data);
//             return lifecycleClient.listLifecycleObjects({ 
//                 Bucket: bucketName,
//                 MaxKeys: 1,
//             }, (err, data) => {
//                 console.log('1 err!!!', err);
//                 console.log('1 data!!!', data);
//                 return lifecycleClient.listLifecycleObjects({ 
//                     Bucket: bucketName,
//                     DateMarker: data.NextDateMarker,
//                     KeyMarker: data.NextKeyMarker,
//                     MaxKeys: 1,
//                 }, (err, data) => {
//                     console.log('2 err!!!', err);
//                     console.log('2 data!!!', data);
//                     return lifecycleClient.listLifecycleObjects({ 
//                         Bucket: bucketName,
//                         DateMarker: data.NextDateMarker,
//                         KeyMarker: data.NextKeyMarker,
//                         MaxKeys: 1,
//                     }, (err, data) => {
//                         console.log('3 err!!!', err);
//                         console.log('3 data!!!', data);
//                         return lifecycleClient.listLifecycleObjects({ 
//                             Bucket: bucketName,
//                             DateMarker: data.NextDateMarker,
//                             KeyMarker: data.NextKeyMarker,
//                             MaxKeys: 1,
//                         }, (err, data) => {
//                             console.log('4 err!!!', err);
//                             console.log('4 data!!!', data);
//                         });
//                     });
//                 });
//             });
//         });
//     });
// });

// runIfMongo('Search when no objects in bucket', () => {
//     const bucketName = `noobjectbucket${Date.now()}`;
//     before(done => {
//         s3Client.createBucket({ Bucket: bucketName }, done);
//     });

//     after(done => {
//         s3Client.deleteBucket({ Bucket: bucketName }, done);
//     });

//     it('should return empty listing when no objects in bucket', done => {
//         const encodedSearch = encodeURIComponent(`key="${objectKey}"`);
//         return runAndCheckSearch(lifecycleClient, bucketName,
//             encodedSearch, false, null, done);
//     });
// });

// runIfMongo('Invalid regular expression searches', () => {
//     const bucketName = `badregex-${Date.now()}`;
//     before(done => {
//         s3Client.createBucket({ Bucket: bucketName }, done);
//     });

//     after(done => {
//         s3Client.deleteBucket({ Bucket: bucketName }, done);
//     });

//     it('should return error if pattern is invalid', done => {
//         const encodedSearch = encodeURIComponent('key LIKE "/((helloworld/"');
//         const testError = {
//             code: 'InvalidArgument',
//             message: 'Invalid sql where clause sent as search query',
//         };
//         return runAndCheckSearch(lifecycleClient, bucketName,
//             encodedSearch, false, testError, done);
//     });
// });
