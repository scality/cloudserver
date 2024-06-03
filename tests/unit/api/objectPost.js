const assert = require('assert');
const async = require('async');
const moment = require('moment');
const { errors, s3middleware } = require('arsenal');
const sinon = require('sinon');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutObjectLock = require('../../../lib/api/bucketPutObjectLock');
const bucketPutACL = require('../../../lib/api/bucketPutACL');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const { parseTagFromQuery } = s3middleware.tagging;
const { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils }
    = require('../helpers');
const { ds } = require('arsenal').storage.data.inMemory.datastore;
const metadata = require('../metadataswitch');
const objectPost = require('../../../lib/api/objectPost');
const { objectLockTestUtils } = require('../helpers');
const DummyRequest = require('../DummyRequest');
const mpuUtils = require('../utils/mpuUtils');
const { lastModifiedHeader } = require('../../../constants');
const { createPresignedPost } = require('@aws-sdk/s3-presigned-post');

const any = sinon.match.any;

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
// const postBody = Buffer.from('I am a body', 'utf8');
// const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
// const mockDate = new Date(2050, 10, 12);
const testPutBucketRequest = new DummyRequest({
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
});
// const testPutBucketRequestLock = new DummyRequest({
//     bucketName,
//     namespace,
//     headers: {
//         'host': `${bucketName}.s3.amazonaws.com`,
//         'x-amz-bucket-object-lock-enabled': 'true',
//     },
//     url: '/',
// });

// const originalputObjectMD = metadata.putObjectMD;
const objectName = 'objectName';

// let testPutObjectRequest;
// const enableVersioningRequest =
//     versioningTestUtils.createBucketPutVersioningReq(bucketName, 'Enabled');
// const suspendVersioningRequest =
//     versioningTestUtils.createBucketPutVersioningReq(bucketName, 'Suspended');

// function testAuth(bucketOwner, authUser, bucketPutReq, log, cb) {
//     bucketPut(bucketOwner, bucketPutReq, log, () => {
//         bucketPutACL(bucketOwner, testPutBucketRequest, log, err => {
//             assert.strictEqual(err, undefined);
//             objectPut(authUser, testPutObjectRequest, undefined,
//                 log, (err, resHeaders) => {
//                     assert.strictEqual(err, null);
//                     assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
//                     cb();
//                 });
//         });
//     });
// }

// describe('parseTagFromQuery', () => {
//     const invalidArgument = { status: 'InvalidArgument', statusCode: 400 };
//     const invalidTag = { status: 'InvalidTag', statusCode: 400 };
//     const allowedChar = '+- =._:/';
//     const tests = [
//         { tagging: 'key1=value1', result: { key1: 'value1' } },
//         { tagging: `key1=${encodeURIComponent(allowedChar)}`,
//             result: { key1: allowedChar } },
//         { tagging: 'key1=value1=value2', error: invalidArgument },
//         { tagging: '=value1', error: invalidArgument },
//         { tagging: 'key1%=value1', error: invalidArgument },
//         { tagging: `${'w'.repeat(129)}=value1`, error: invalidTag },
//         { tagging: `key1=${'w'.repeat(257)}`, error: invalidTag },
//         { tagging: `${'w'.repeat(129)}=value1`, error: invalidTag },
//         { tagging: `key1=${'w'.repeat(257)}`, error: invalidTag },
//     ];
//     tests.forEach(test => {
//         const behavior = test.error ? 'fail' : 'pass';
//         it(`should ${behavior} if tag set: "${test.tagging}"`, done => {
//             const result = parseTagFromQuery(test.tagging);
//             if (test.error) {
//                 assert(result.is[test.error.status]);
//                 assert.strictEqual(result.code, test.error.statusCode);
//             } else {
//                 assert.deepStrictEqual(result, test.result);
//             }
//             done();
//         });
//     });
// });

async function generatePresignedPost(options, conditions) {
    // Define the conditions for the presigned POST
    // const tagging = "<Tagging><TagSet><Tag><Key>Tag Name</Key><Value>Tag Value</Value></Tag></TagSet></Tagging>";
    // const conditions = [
    //     {"bucket": bucketName},
    //     { key: key },            // Ensure the key matches
    //     { "Content-Type": contentType },  // Enforce specific content type
    // ];

    // const fields = {
    //     "Content-Type": contentType, // Pre-filled field for content type
    //     "tagging": tagging
    // };

    // const options = {
    //     Bucket: bucketName,
    //     Fields: fields,
    //     Expires: 300,  // Time in seconds before the presigned POST expires
    //     Conditions: conditions,
    //     Key: key,
    // };
    try {
        // Generate the presigned POST
        // The presigned POST contains the URL and fields to upload the object
        // It will generate the Policy, x-amz-signature, x-amz-credential and x-amz-date for you
        const presignedPost = await createPresignedPost(s3, options);
        return presignedPost;
    } catch (error) {
        console.error("Error creating presigned post:", error);
        throw error;
    }
}

async function uploadFileUsingPresignedPost(presignedPostData, fileContent) {
    const formData = new FormData();
    Object.entries(presignedPostData.fields).forEach(([key, value]) => {
        formData.append(key, value);
    });
    formData.append('file', fileContent, 'filename.txt');
    console.log('formdata: ', formData)
    const response = 
    await axios({
        method: 'post',
        url: presignedPostData.url,
        data: formData,
        headers: {
            'Content-Type': `multipart/form-data; boundary=${formData._boundary}`,
        },
    });

    return response;
}

describe('objectPost API', () => {
    // beforeEach(() => {
    //     cleanup();
    //     sinon.spy(metadata, 'putObjectMD');
    //     testPutObjectRequest = new DummyRequest({
    //         bucketName,
    //         namespace,
    //         objectKey: objectName,
    //         headers: { host: `${bucketName}.s3.amazonaws.com` },
    //         url: '/',
    //     }, postBody);
    // });

    // afterEach(() => {
    //     sinon.restore();
    //     metadata.putObjectMD = originalputObjectMD;
    // });

// it('should return an error if the bucket does not exist', done => {
//     objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
//         assert.deepStrictEqual(err, errors.NoSuchBucket);
//         done();
//     });
// });

// it('should return an error if user is not authorized', done => {
//     const putAuthInfo = makeAuthInfo('accessKey2');
//     bucketPut(putAuthInfo, testPutBucketRequest,
//         log, () => {
//             objectPut(authInfo, testPutObjectRequest,
//                 undefined, log, err => {
//                     assert.deepStrictEqual(err, errors.AccessDenied);
//                     done();
//                 });
//         });
// });

                    // NOTE: likely not feasible on POST
                    // it('should put object if user has FULL_CONTROL grant on bucket', done => {
                    //     const bucketOwner = makeAuthInfo('accessKey2');
                    //     const authUser = makeAuthInfo('accessKey3');
                    //     testPutBucketRequest.headers['x-amz-grant-full-control'] =
                    //         `id=${authUser.getCanonicalID()}`;
                    //     testAuth(bucketOwner, authUser, testPutBucketRequest, log, done);
                    // });

                    // it('should put object if user has WRITE grant on bucket', done => {
                    //     const bucketOwner = makeAuthInfo('accessKey2');
                    //     const authUser = makeAuthInfo('accessKey3');
                    //     testPutBucketRequest.headers['x-amz-grant-write'] =
                    //         `id=${authUser.getCanonicalID()}`;

                    //     testAuth(bucketOwner, authUser, testPutBucketRequest, log, done);
                    // });

                    // it('should put object in bucket with public-read-write acl', done => {
                    //     const bucketOwner = makeAuthInfo('accessKey2');
                    //     const authUser = makeAuthInfo('accessKey3');
                    //     testPutBucketRequest.headers['x-amz-acl'] = 'public-read-write';

                    //     testAuth(bucketOwner, authUser, testPutBucketRequest, log, done);
                    // });

    it('should successfully post an object', done => {
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
            calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
        }, postBody);

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPost(authInfo, testPostObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    metadata.getObjectMD(bucketName, objectName,
                        {}, log, (err, md) => {
                            assert(md);
                            assert
                            .strictEqual(md['content-md5'], correctMD5);
                            done();
                        });
                });
        });
    });

    // const mockModes = ['GOVERNANCE', 'COMPLIANCE'];
    // mockModes.forEach(mockMode => {
    //     it(`should put an object with valid date & ${mockMode} mode`, done => {
    //         const testPutObjectRequest = new DummyRequest({
    //             bucketName,
    //             namespace,
    //             objectKey: objectName,
    //             headers: {
    //                 'x-amz-object-lock-retain-until-date': mockDate,
    //                 'x-amz-object-lock-mode': mockMode,
    //             },
    //             url: `/${bucketName}/${objectName}`,
    //             calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
    //         }, postBody);
    //         bucketPut(authInfo, testPutBucketRequestLock, log, () => {
    //             objectPut(authInfo, testPutObjectRequest, undefined, log,
    //                 (err, headers) => {
    //                     assert.ifError(err);
    //                     assert.strictEqual(headers.ETag, `"${correctMD5}"`);
    //                     metadata.getObjectMD(bucketName, objectName, {}, log,
    //                         (err, md) => {
    //                             const mode = md.retentionMode;
    //                             const retainUntilDate = md.retentionDate;
    //                             assert.ifError(err);
    //                             assert(md);
    //                             assert.strictEqual(mode, mockMode);
    //                             assert.strictEqual(retainUntilDate, mockDate);
    //                             done();
    //                         });
    //                 });
    //         });
    //     });
    // });

    // const formatTime = time => time.slice(0, 20);

    // const testObjectLockConfigs = [
    //     {
    //         testMode: 'COMPLIANCE',
    //         val: 30,
    //         type: 'Days',
    //     },
    //     {
    //         testMode: 'GOVERNANCE',
    //         val: 5,
    //         type: 'Years',
    //     },
    // ];
    // testObjectLockConfigs.forEach(config => {
    //     const { testMode, type, val } = config;
    //     it('should put an object with default retention if object does not ' +
    //         'have retention configuration but bucket has', done => {
    //         const testPutObjectRequest = new DummyRequest({
    //             bucketName,
    //             namespace,
    //             objectKey: objectName,
    //             headers: {},
    //             url: `/${bucketName}/${objectName}`,
    //             calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
    //         }, postBody);

    //         const testObjLockRequest = {
    //             bucketName,
    //             headers: { host: `${bucketName}.s3.amazonaws.com` },
    //             post: objectLockTestUtils.generateXml(testMode, val, type),
    //         };

    //         bucketPut(authInfo, testPutBucketRequestLock, log, () => {
    //             bucketPutObjectLock(authInfo, testObjLockRequest, log, () => {
    //                 objectPut(authInfo, testPutObjectRequest, undefined, log,
    //                     (err, headers) => {
    //                         assert.ifError(err);
    //                         assert.strictEqual(headers.ETag, `"${correctMD5}"`);
    //                         metadata.getObjectMD(bucketName, objectName, {},
    //                             log, (err, md) => {
    //                                 const mode = md.retentionMode;
    //                                 const retainDate = md.retentionDate;
    //                                 const date = moment();
    //                                 const days
    //                                     = type === 'Days' ? val : val * 365;
    //                                 const expectedDate
    //                                     = date.add(days, 'days');
    //                                 assert.ifError(err);
    //                                 assert.strictEqual(mode, testMode);
    //                                 assert.strictEqual(formatTime(retainDate),
    //                                     formatTime(expectedDate.toISOString()));
    //                                 done();
    //                             });
    //                     });
    //             });
    //         });
    //     });
    // });


    // it('should successfully put an object with legal hold ON', done => {
    //     const request = new DummyRequest({
    //         bucketName,
    //         namespace,
    //         objectKey: objectName,
    //         headers: {
    //             'x-amz-object-lock-legal-hold': 'ON',
    //         },
    //         url: `/${bucketName}/${objectName}`,
    //         calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
    //     }, postBody);

    //     bucketPut(authInfo, testPutBucketRequestLock, log, () => {
    //         objectPut(authInfo, request, undefined, log, (err, headers) => {
    //             assert.ifError(err);
    //             assert.strictEqual(headers.ETag, `"${correctMD5}"`);
    //             metadata.getObjectMD(bucketName, objectName, {}, log,
    //                 (err, md) => {
    //                     assert.ifError(err);
    //                     assert.strictEqual(md.legalHold, true);
    //                     done();
    //                 });
    //         });
    //     });
    // });

    // it('should successfully put an object with legal hold OFF', done => {
    //     const request = new DummyRequest({
    //         bucketName,
    //         namespace,
    //         objectKey: objectName,
    //         headers: {
    //             'x-amz-object-lock-legal-hold': 'OFF',
    //         },
    //         url: `/${bucketName}/${objectName}`,
    //         calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
    //     }, postBody);

    //     bucketPut(authInfo, testPutBucketRequestLock, log, () => {
    //         objectPut(authInfo, request, undefined, log, (err, headers) => {
    //             assert.ifError(err);
    //             assert.strictEqual(headers.ETag, `"${correctMD5}"`);
    //             metadata.getObjectMD(bucketName, objectName, {}, log,
    //                 (err, md) => {
    //                     assert.ifError(err);
    //                     assert(md);
    //                     assert.strictEqual(md.legalHold, false);
    //                     done();
    //                 });
    //         });
    //     });
    // });

    // it('should successfully put an object with user metadata', done => {
    //     const testPutObjectRequest = new DummyRequest({
    //         bucketName,
    //         namespace,
    //         objectKey: objectName,
    //         headers: {
    //             // Note that Node will collapse common headers into one
    //             // (e.g. "x-amz-meta-test: hi" and "x-amz-meta-test:
    //             // there" becomes "x-amz-meta-test: hi, there")
    //             // Here we are not going through an actual http
    //             // request so will not collapse properly.
    //             'x-amz-meta-test': 'some metadata',
    //             'x-amz-meta-test2': 'some more metadata',
    //             'x-amz-meta-test3': 'even more metadata',
    //         },
    //         url: `/${bucketName}/${objectName}`,
    //         calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
    //     }, postBody);

    //     bucketPut(authInfo, testPutBucketRequest, log, () => {
    //         objectPut(authInfo, testPutObjectRequest, undefined, log,
    //             (err, resHeaders) => {
    //                 assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
    //                 metadata.getObjectMD(bucketName, objectName, {}, log,
    //                     (err, md) => {
    //                         assert(md);
    //                         assert.strictEqual(md['x-amz-meta-test'],
    //                                     'some metadata');
    //                         assert.strictEqual(md['x-amz-meta-test2'],
    //                                     'some more metadata');
    //                         assert.strictEqual(md['x-amz-meta-test3'],
    //                                     'even more metadata');
    //                         done();
    //                     });
    //             });
    //     });
    // });

    // it('If testingMode=true and the last-modified header is given, should set last-modified accordingly', done => {
    //     const imposedLastModified = '2024-07-19';
    //     const testPutObjectRequest = new DummyRequest({
    //         bucketName,
    //         namespace,
    //         objectKey: objectName,
    //         headers: {
    //             [lastModifiedHeader]: imposedLastModified,
    //         },
    //         url: `/${bucketName}/${objectName}`,
    //         calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
    //     }, postBody);

    //     bucketPut(authInfo, testPutBucketRequest, log, () => {
    //         const config = require('../../../lib/Config');
    //         config.config.testingMode = true;
    //         objectPut(authInfo, testPutObjectRequest, undefined, log,
    //             (err, resHeaders) => {
    //                 assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
    //                 metadata.getObjectMD(bucketName, objectName, {}, log,
    //                     (err, md) => {
    //                         assert(md);

    //                         const lastModified = md['last-modified'];
    //                         const lastModifiedDate = lastModified.split('T')[0];
    //                         // last-modified date should be the one set by the last-modified header
    //                         assert.strictEqual(lastModifiedDate, imposedLastModified);

    //                         // The header should be removed after being treated.
    //                         assert(md[lastModifiedHeader] === undefined);

    //                         config.config.testingMode = false;
    //                         done();
    //                     });
    //             });
    //     });
    // });

    // it('should not take into acccount the last-modified header when testingMode=false', done => {
    //     const imposedLastModified = '2024-07-19';

    //     const testPutObjectRequest = new DummyRequest({
    //         bucketName,
    //         namespace,
    //         objectKey: objectName,
    //         headers: {
    //             'x-amz-meta-x-scal-last-modified': imposedLastModified,
    //         },
    //         url: `/${bucketName}/${objectName}`,
    //         calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
    //     }, postBody);

    //     bucketPut(authInfo, testPutBucketRequest, log, () => {
    //         const config = require('../../../lib/Config');
    //         config.config.testingMode = false;
    //         objectPut(authInfo, testPutObjectRequest, undefined, log,
    //             (err, resHeaders) => {
    //                 assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
    //                 metadata.getObjectMD(bucketName, objectName, {}, log,
    //                     (err, md) => {
    //                         assert(md);
    //                         assert.strictEqual(md['x-amz-meta-x-scal-last-modified'],
    //                                     imposedLastModified);
    //                         const lastModified = md['last-modified'];
    //                         const lastModifiedDate = lastModified.split('T')[0];
    //                         const currentTs = new Date().toJSON();
    //                         const currentDate = currentTs.split('T')[0];
    //                         assert.strictEqual(lastModifiedDate, currentDate);
    //                         done();
    //                     });
    //             });
    //     });
    // });

    // it('should put an object with user metadata but no data', done => {
    //     const postBody = '';
    //     const correctMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
    //     const testPutObjectRequest = new DummyRequest({
    //         bucketName,
    //         namespace,
    //         objectKey: objectName,
    //         headers: {
    //             'content-length': '0',
    //             'x-amz-meta-test': 'some metadata',
    //             'x-amz-meta-test2': 'some more metadata',
    //             'x-amz-meta-test3': 'even more metadata',
    //         },
    //         parsedContentLength: 0,
    //         url: `/${bucketName}/${objectName}`,
    //         calculatedHash: 'd41d8cd98f00b204e9800998ecf8427e',
    //     }, postBody);

    //     bucketPut(authInfo, testPutBucketRequest, log, () => {
    //         objectPut(authInfo, testPutObjectRequest, undefined, log,
    //             (err, resHeaders) => {
    //                 assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
    //                 assert.deepStrictEqual(ds, []);
    //                 metadata.getObjectMD(bucketName, objectName, {}, log,
    //                     (err, md) => {
    //                         assert(md);
    //                         assert.strictEqual(md.location, null);
    //                         assert.strictEqual(md['x-amz-meta-test'],
    //                                     'some metadata');
    //                         assert.strictEqual(md['x-amz-meta-test2'],
    //                                    'some more metadata');
    //                         assert.strictEqual(md['x-amz-meta-test3'],
    //                                    'even more metadata');
    //                         done();
    //                     });
    //             });
    //     });
    // });

    // it('should not leave orphans in data when overwriting an object', done => {
    //     const testPutObjectRequest2 = new DummyRequest({
    //         bucketName,
    //         namespace,
    //         objectKey: objectName,
    //         headers: {},
    //         url: `/${bucketName}/${objectName}`,
    //     }, Buffer.from('I am another body', 'utf8'));

    //     bucketPut(authInfo, testPutBucketRequest, log, () => {
    //         objectPut(authInfo, testPutObjectRequest,
    //             undefined, log, () => {
    //                 objectPut(authInfo, testPutObjectRequest2, undefined,
    //                     log,
    //                 () => {
    //                     // orphan objects don't get deleted
    //                     // until the next tick
    //                     // in memory
    //                     setImmediate(() => {
    //                         // Data store starts at index 1
    //                         assert.strictEqual(ds[0], undefined);
    //                         assert.strictEqual(ds[1], undefined);
    //                         assert.deepStrictEqual(ds[2].value,
    //                             Buffer.from('I am another body', 'utf8'));
    //                         done();
    //                     });
    //                 });
    //             });
    //     });
    // });

    // it('should not leave orphans in data when overwriting an multipart upload object', done => {
    //     bucketPut(authInfo, testPutBucketRequest, log, () => {
    //         mpuUtils.createMPU(namespace, bucketName, objectName, log,
    //             (err, testUploadId) => {
    //                 objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
    //                     assert.ifError(err);
    //                     sinon.assert.calledWith(metadata.putObjectMD,
    //                         any, any, any, sinon.match({ oldReplayId: testUploadId }), any, any);
    //                     done();
    //                 });
    //             });
    //     });
    // });

    // it('should not put object with retention configuration if object lock ' +
    //     'is not enabled on the bucket', done => {
    //     const testPutObjectRequest = new DummyRequest({
    //         bucketName,
    //         namespace,
    //         objectKey: objectName,
    //         headers: {
    //             'x-amz-object-lock-retain-until-date': mockDate,
    //             'x-amz-object-lock-mode': 'GOVERNANCE',
    //         },
    //         url: `/${bucketName}/${objectName}`,
    //         calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
    //     }, postBody);

    //     bucketPut(authInfo, testPutBucketRequest, log, () => {
    //         objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
    //             assert.deepStrictEqual(err, errors.InvalidRequest
    //                 .customizeDescription(
    //                     'Bucket is missing ObjectLockConfiguration'));
    //             done();
    //         });
    //     });
    // });
    // it('should forward a 400 back to client on metadata 408 response', () => {
    //     metadata.putObjectMD =
    //         (bucketName, objName, objVal, params, log, cb) =>
    //             cb({ httpCode: 408 });

    //     bucketPut(authInfo, testPutBucketRequest, log, () => {
    //         objectPut(authInfo, testPutObjectRequest, undefined, log,
    //             err => {
    //                 assert.strictEqual(err.code, 400);
    //             });
    //     });
    // });

    // it('should forward a 502 to the client for 4xx != 408', () => {
    //     metadata.putObjectMD =
    //         (bucketName, objName, objVal, params, log, cb) =>
    //             cb({ httpCode: 412 });

    //     bucketPut(authInfo, testPutBucketRequest, log, () => {
    //         objectPut(authInfo, testPutObjectRequest, undefined, log,
    //             err => {
    //                 assert.strictEqual(err.code, 502);
    //             });
    //     });
    // });


    // it('should pass overheadField to metadata.putObjectMD for a non-versioned request', done => {
    //     const testPutObjectRequest = new DummyRequest({
    //         bucketName,
    //         namespace,
    //         objectKey: objectName,
    //         headers: {},
    //         url: `/${bucketName}/${objectName}`,
    //         contentMD5: correctMD5,
    //     }, postBody);

    //     bucketPut(authInfo, testPutBucketRequest, log, () => {
    //         objectPut(authInfo, testPutObjectRequest, undefined, log,
    //             err => {
    //                 assert.ifError(err);
    //                 sinon.assert.calledWith(metadata.putObjectMD.lastCall,
    //                     bucketName, objectName, any, sinon.match({ overheadField: sinon.match.array }), any, any);
    //                 done();
    //             });
    //     });
    // });

    // it('should pass overheadField to metadata.putObjectMD for a versioned request', done => {
    //     const testPutObjectRequest = versioningTestUtils
    //         .createPutObjectRequest(bucketName, objectName, Buffer.from('I am another body', 'utf8'));
    //     bucketPut(authInfo, testPutBucketRequest, log, () => {
    //         bucketPutVersioning(authInfo, enableVersioningRequest, log, () => {
    //             objectPut(authInfo, testPutObjectRequest, undefined, log,
    //                 err => {
    //                     assert.ifError(err);
    //                     sinon.assert.calledWith(metadata.putObjectMD.lastCall,
    //                         bucketName, objectName, any, sinon.match({ overheadField: sinon.match.array }), any, any);
    //                     done();
    //                 }
    //             );
    //         });
    //     });
    // });

    // it('should pass overheadField to metadata.putObjectMD for a version-suspended request', done => {
    //     const testPutObjectRequest = versioningTestUtils
    //         .createPutObjectRequest(bucketName, objectName, Buffer.from('I am another body', 'utf8'));
    //     bucketPut(authInfo, testPutBucketRequest, log, () => {
    //         bucketPutVersioning(authInfo, suspendVersioningRequest, log, () => {
    //             objectPut(authInfo, testPutObjectRequest, undefined, log,
    //                 err => {
    //                     assert.ifError(err);
    //                     sinon.assert.calledWith(metadata.putObjectMD.lastCall,
    //                         bucketName, objectName, any, sinon.match({ overheadField: sinon.match.array }), any, any);
    //                     done();
    //                 }
    //             );
    //         });
    //     });
    // });
});




// describe('objectPut API with versioning', () => {
//     beforeEach(() => {
//         cleanup();
//     });

//     const objData = ['foo0', 'foo1', 'foo2'].map(str =>
//         Buffer.from(str, 'utf8'));
//     const testPutObjectRequests = objData.map(data => versioningTestUtils
//         .createPutObjectRequest(bucketName, objectName, data));

//     it('should delete latest version when creating new null version ' +
//     'if latest version is null version', done => {
//         async.series([
//             callback => bucketPut(authInfo, testPutBucketRequest, log,
//                 callback),
//             // putting null version by putting obj before versioning configured
//             callback => objectPut(authInfo, testPutObjectRequests[0], undefined,
//                 log, err => {
//                     versioningTestUtils.assertDataStoreValues(ds, [objData[0]]);
//                     callback(err);
//                 }),
//             callback => bucketPutVersioning(authInfo, suspendVersioningRequest,
//                 log, callback),
//             // creating new null version by putting obj after ver suspended
//             callback => objectPut(authInfo, testPutObjectRequests[1],
//                 undefined, log, err => {
//                     // wait until next tick since mem backend executes
//                     // deletes in the next tick
//                     setImmediate(() => {
//                         // old null version should be deleted
//                         versioningTestUtils.assertDataStoreValues(ds,
//                             [undefined, objData[1]]);
//                         callback(err);
//                     });
//                 }),
//             // create another null version
//             callback => objectPut(authInfo, testPutObjectRequests[2],
//                 undefined, log, err => {
//                     setImmediate(() => {
//                         // old null version should be deleted
//                         versioningTestUtils.assertDataStoreValues(ds,
//                             [undefined, undefined, objData[2]]);
//                         callback(err);
//                     });
//                 }),
//         ], done);
//     });

//     describe('when null version is not the latest version', () => {
//         const objData = ['foo0', 'foo1', 'foo2'].map(str =>
//             Buffer.from(str, 'utf8'));
//         const testPutObjectRequests = objData.map(data => versioningTestUtils
//             .createPutObjectRequest(bucketName, objectName, data));
//         beforeEach(done => {
//             async.series([
//                 callback => bucketPut(authInfo, testPutBucketRequest, log,
//                     callback),
//                 // putting null version: put obj before versioning configured
//                 callback => objectPut(authInfo, testPutObjectRequests[0],
//                     undefined, log, callback),
//                 callback => bucketPutVersioning(authInfo,
//                     enableVersioningRequest, log, callback),
//                 // put another version:
//                 callback => objectPut(authInfo, testPutObjectRequests[1],
//                     undefined, log, callback),
//                 callback => bucketPutVersioning(authInfo,
//                     suspendVersioningRequest, log, callback),
//             ], err => {
//                 if (err) {
//                     return done(err);
//                 }
//                 versioningTestUtils.assertDataStoreValues(ds,
//                     objData.slice(0, 2));
//                 return done();
//             });
//         });

//         it('should still delete null version when creating new null version',
//         done => {
//             objectPut(authInfo, testPutObjectRequests[2], undefined,
//                 log, err => {
//                     assert.ifError(err, `Unexpected err: ${err}`);
//                     setImmediate(() => {
//                         // old null version should be deleted after putting
//                         // new null version
//                         versioningTestUtils.assertDataStoreValues(ds,
//                             [undefined, objData[1], objData[2]]);
//                         done(err);
//                     });
//                 });
//         });
//     });

//     it('should return BadDigest error and not leave orphans in data when ' +
//     'contentMD5 and completedHash do not match', done => {
//         const testPutObjectRequest = new DummyRequest({
//             bucketName,
//             namespace,
//             objectKey: objectName,
//             headers: {},
//             url: `/${bucketName}/${objectName}`,
//             contentMD5: 'vnR+tLdVF79rPPfF+7YvOg==',
//         }, Buffer.from('I am another body', 'utf8'));

//         bucketPut(authInfo, testPutBucketRequest, log, () => {
//             objectPut(authInfo, testPutObjectRequest, undefined, log,
//             err => {
//                 assert.deepStrictEqual(err, errors.BadDigest);
//                 // orphan objects don't get deleted
//                 // until the next tick
//                 // in memory
//                 setImmediate(() => {
//                     // Data store starts at index 1
//                     assert.strictEqual(ds[0], undefined);
//                     assert.strictEqual(ds[1], undefined);
//                     done();
//                 });
//             });
//         });
//     });
// });
