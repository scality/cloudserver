const { errors, storage } = require('arsenal');

const assert = require('assert');
const crypto = require('crypto');

const async = require('async');
const { parseString } = require('xml2js');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const objectPut = require('../../../lib/api/objectPut');
const completeMultipartUpload
    = require('../../../lib/api/completeMultipartUpload');
const constants = require('../../../constants');
const { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils }
    = require('../helpers');
const initiateMultipartUpload
    = require('../../../lib/api/initiateMultipartUpload');
const multipartDelete = require('../../../lib/api/multipartDelete');
const objectPutPart = require('../../../lib/api/objectPutPart');
const DummyRequest = require('../DummyRequest');

const { metadata } = storage.metadata.inMemory.metadata;
const { ds } = storage.data.inMemory.datastore;

const log = new DummyRequestLogger();

const splitter = constants.splitter;
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;
const postBody = Buffer.from('I am a body', 'utf8');
const bucketPutRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    post: '<CreateBucketConfiguration ' +
    'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
    '<LocationConstraint>scality-internal-mem</LocationConstraint>' +
    '</CreateBucketConfiguration >',
};
const objectKey = 'testObject';
const initiateRequest = {
    bucketName,
    namespace,
    objectKey,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: `/${objectKey}?uploads`,
};

function _createPutPartRequest(uploadId, partNumber, partBody) {
    const md5Hash = crypto.createHash('md5').update(partBody);
    const calculatedHash = md5Hash.digest('hex');
    return new DummyRequest({
        bucketName,
        namespace,
        objectKey,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: `/${objectKey}?partNumber=${partNumber}&uploadId=${uploadId}`,
        query: {
            partNumber,
            uploadId,
        },
        calculatedHash,
    }, partBody);
}

function _createCompleteMpuRequest(uploadId, parts) {
    const completeBody = [];
    completeBody.push('<CompleteMultipartUpload>');
    parts.forEach(part => {
        completeBody.push('<Part>' +
            `<PartNumber>${part.partNumber}</PartNumber>` +
            `<ETag>"${part.eTag}"</ETag>` +
            '</Part>');
    });
    completeBody.push('</CompleteMultipartUpload>');
    return {
        bucketName,
        namespace,
        objectKey,
        parsedHost: 's3.amazonaws.com',
        url: `/${objectKey}?uploadId=${uploadId}`,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        query: { uploadId },
        post: completeBody,
    };
}


describe('Multipart Upload API', () => {
    beforeEach(() => {
        cleanup();
    });

    test('mpuBucketPrefix should be a defined constant', () => {
        expect(constants.mpuBucketPrefix).toBeTruthy();
    });

    test('should initiate a multipart upload', done => {
        bucketPut(authInfo, bucketPutRequest, log, err => {
            expect(err).toBe(null);
            initiateMultipartUpload(authInfo, initiateRequest,
                log, (err, result) => {
                    expect(err).toBe(null);
                    parseString(result, (err, json) => {
                        expect(json.InitiateMultipartUploadResult
                            .Bucket[0]).toBe(bucketName);
                        expect(json.InitiateMultipartUploadResult
                            .Key[0]).toBe(objectKey);
                        expect(json.InitiateMultipartUploadResult.UploadId[0]).toBeTruthy();
                        expect(metadata.buckets.get(mpuBucket)._name).toBeTruthy();
                        const mpuKeys = metadata.keyMaps.get(mpuBucket);
                        expect(mpuKeys.size).toBe(1);
                        expect(mpuKeys.keys().next().value
                            .startsWith(`overview${splitter}${objectKey}`)).toBeTruthy();
                        done();
                    });
                });
        });
    });

    test('should return an error on an initiate multipart upload call if ' +
        'no destination bucket', done => {
        initiateMultipartUpload(authInfo, initiateRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
    });

    test('should upload a part', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                expect(mpuKeys.size).toBe(1);
                expect(mpuKeys.keys().next().value
                    .startsWith(`overview${splitter}${objectKey}`)).toBeTruthy();
                parseString(result, next);
            },
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                objectKey,
                namespace,
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, err => {
                expect(err).toBe(null);
                const keysInMPUkeyMap = [];
                metadata.keyMaps.get(mpuBucket).forEach((val, key) => {
                    keysInMPUkeyMap.push(key);
                });
                const sortedKeyMap = keysInMPUkeyMap.sort(a => {
                    if (a.slice(0, 8) === 'overview') {
                        return -1;
                    }
                    return 0;
                });
                const overviewEntry = sortedKeyMap[0];
                const partKey = sortedKeyMap[1];
                const partEntryArray = partKey.split(splitter);
                const partUploadId = partEntryArray[0];
                const firstPartNumber = partEntryArray[1];
                const partETag = metadata.keyMaps.get(mpuBucket)
                                                 .get(partKey)['content-md5'];
                expect(keysInMPUkeyMap.length).toBe(2);
                expect(metadata.keyMaps.get(mpuBucket)
                                                   .get(overviewEntry).key).toBe(objectKey);
                expect(partUploadId).toBe(testUploadId);
                expect(firstPartNumber).toBe('00001');
                expect(partETag).toBe(calculatedHash);
                done();
            });
        });
    });

    test('should upload a part even if the client sent a base 64 ETag ' +
    '(and the stored ETag in metadata should be hex)', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            const calculatedHash = md5Hash.update(bufferBody).digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, err => {
                expect(err).toBe(null);
                const keysInMPUkeyMap = [];
                metadata.keyMaps.get(mpuBucket).forEach((val, key) => {
                    keysInMPUkeyMap.push(key);
                });
                const sortedKeyMap = keysInMPUkeyMap.sort(a => {
                    if (a.slice(0, 8) === 'overview') {
                        return -1;
                    }
                    return 0;
                });
                const partKey = sortedKeyMap[1];
                const partETag = metadata.keyMaps.get(mpuBucket)
                                                 .get(partKey)['content-md5'];
                expect(keysInMPUkeyMap.length).toBe(2);
                expect(partETag).toBe(calculatedHash);
                done();
            });
        });
    });

    test('should return an error if too many parts', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '10001',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log,
                (err, result) => {
                    assert.deepStrictEqual(err, errors.TooManyParts);
                    expect(result).toBe(undefined);
                    done();
                });
        });
    });

    test('should return an error if part number is not an integer', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                objectKey,
                namespace,
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                query: {
                    partNumber: 'I am not an integer',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log,
                (err, result) => {
                    assert.deepStrictEqual(err, errors.InvalidArgument);
                    expect(result).toBe(undefined);
                    done();
                });
        });
    });

    test('should return an error if content-length is too large', done => {
        // Note this is only faking a large file
        // by setting a large content-length.  It is not actually putting a
        // large file.  Functional tests will test actual large data.
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'content-length': '5368709121',
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
                parsedContentLength: 5368709121,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined,
                log, (err, result) => {
                    assert.deepStrictEqual(err, errors.EntityTooLarge);
                    expect(result).toBe(undefined);
                    done();
                });
        });
    });

    test('should upload two parts', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest1 = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest1, undefined, log, () => {
                const postBody2 = Buffer.from('I am a second part', 'utf8');
                const md5Hash2 = crypto.createHash('md5');
                const bufferBody2 = Buffer.from(postBody2);
                md5Hash2.update(bufferBody2);
                const secondCalculatedMD5 = md5Hash2.digest('hex');
                const partRequest2 = new DummyRequest({
                    bucketName,
                    namespace,
                    objectKey,
                    url: `/${objectKey}?partNumber=` +
                        `1&uploadId=${testUploadId}`,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: {
                        partNumber: '2',
                        uploadId: testUploadId,
                    },
                    calculatedHash: secondCalculatedMD5,
                }, postBody2);
                objectPutPart(authInfo, partRequest2, undefined, log, err => {
                    expect(err).toBe(null);

                    const keysInMPUkeyMap = [];
                    metadata.keyMaps.get(mpuBucket).forEach((val, key) => {
                        keysInMPUkeyMap.push(key);
                    });
                    const sortedKeyMap = keysInMPUkeyMap.sort(a => {
                        if (a.slice(0, 8) === 'overview') {
                            return -1;
                        }
                        return 0;
                    });
                    const overviewEntry = sortedKeyMap[0];
                    const partKey = sortedKeyMap[2];
                    const secondPartEntryArray = partKey.split(splitter);
                    const partUploadId = secondPartEntryArray[0];
                    const secondPartETag = metadata.keyMaps.get(mpuBucket)
                                                   .get(partKey)['content-md5'];
                    const secondPartNumber = secondPartEntryArray[1];
                    expect(keysInMPUkeyMap.length).toBe(3);
                    expect(metadata
                        .keyMaps.get(mpuBucket).get(overviewEntry).key).toBe(objectKey);
                    expect(partUploadId).toBe(testUploadId);
                    expect(secondPartNumber).toBe('00002');
                    expect(secondPartETag).toBe(secondCalculatedMD5);
                    done();
                });
            });
        });
    });

    test('should complete a multipart upload', done => {
        const partBody = Buffer.from('I am a part\n', 'utf8');
        initiateRequest.headers['x-amz-meta-stuff'] =
            'I am some user metadata';
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5').update(partBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                // Note that the body of the post set in the request here does
                // not really matter in this test.
                // The put is not going through the route so the md5 is being
                // calculated above and manually being set in the request below.
                // What is being tested is that the calculatedHash being sent
                // to the API for the part is stored and then used to
                // calculate the final ETag upon completion
                // of the multipart upload.
                calculatedHash,
            }, partBody);
            objectPutPart(authInfo, partRequest, undefined, log, () => {
                const completeBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>1</PartNumber>' +
                    `<ETag>"${calculatedHash}"</ETag>` +
                    '</Part>' +
                    '</CompleteMultipartUpload>';
                const completeRequest = {
                    bucketName,
                    namespace,
                    objectKey,
                    parsedHost: 's3.amazonaws.com',
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: { uploadId: testUploadId },
                    post: completeBody,
                };
                const awsVerifiedETag =
                    '"953e9e776f285afc0bfcf1ab4668299d-1"';
                completeMultipartUpload(authInfo,
                    completeRequest, log, (err, result) => {
                        parseString(result, (err, json) => {
                            expect(json.CompleteMultipartUploadResult.Location[0]).toBe(`http://${bucketName}.s3.amazonaws.com`
                            + `/${objectKey}`);
                            expect(json.CompleteMultipartUploadResult.Bucket[0]).toBe(bucketName);
                            expect(json.CompleteMultipartUploadResult.Key[0]).toBe(objectKey);
                            expect(json.CompleteMultipartUploadResult.ETag[0]).toBe(awsVerifiedETag);
                            const MD = metadata.keyMaps.get(bucketName)
                                                       .get(objectKey);
                            expect(MD).toBeTruthy();
                            expect(MD['x-amz-meta-stuff']).toBe('I am some user metadata');
                            done();
                        });
                    });
            });
        });
    });

    test('should complete a multipart upload even if etag is sent ' +
        'in post body without quotes (a la Cyberduck)', done => {
        const partBody = Buffer.from('I am a part\n', 'utf8');
        initiateRequest.headers['x-amz-meta-stuff'] =
            'I am some user metadata';
        async.waterfall([
            function waterfall1(next) {
                bucketPut(authInfo, bucketPutRequest, log, next);
            },
            function waterfall2(corsHeaders, next) {
                initiateMultipartUpload(
                    authInfo, initiateRequest, log, next);
            },
            function waterfall3(result, corsHeaders, next) {
                parseString(result, next);
            },
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5').update(partBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, partBody);
            objectPutPart(authInfo, partRequest, undefined, log, () => {
                const completeBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>1</PartNumber>' +
                    // ETag without quotes
                    `<ETag>${calculatedHash}</ETag>` +
                    '</Part>' +
                    '</CompleteMultipartUpload>';
                const completeRequest = {
                    bucketName,
                    namespace,
                    objectKey,
                    parsedHost: 's3.amazonaws.com',
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: { uploadId: testUploadId },
                    post: completeBody,
                };
                const awsVerifiedETag =
                    '"953e9e776f285afc0bfcf1ab4668299d-1"';
                completeMultipartUpload(authInfo,
                    completeRequest, log, (err, result) => {
                        parseString(result, (err, json) => {
                            expect(json.CompleteMultipartUploadResult.Location[0]).toBe(`http://${bucketName}.s3.amazonaws.com`
                            + `/${objectKey}`);
                            expect(json.CompleteMultipartUploadResult.Bucket[0]).toBe(bucketName);
                            expect(json.CompleteMultipartUploadResult.Key[0]).toBe(objectKey);
                            expect(json.CompleteMultipartUploadResult.ETag[0]).toBe(awsVerifiedETag);
                            const MD = metadata.keyMaps.get(bucketName)
                                                       .get(objectKey);
                            expect(MD).toBeTruthy();
                            expect(MD['x-amz-meta-stuff']).toBe('I am some user metadata');
                            done();
                        });
                    });
            });
        });
    });

    test('should return an error if a complete multipart upload' +
    ' request contains malformed xml', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, () => {
                const completeBody = 'Malformed xml';
                const completeRequest = {
                    bucketName,
                    objectKey,
                    namespace,
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: { uploadId: testUploadId },
                    post: completeBody,
                    calculatedHash,
                };
                completeMultipartUpload(authInfo,
                    completeRequest, log, err => {
                        assert.deepStrictEqual(err, errors.MalformedXML);
                        expect(metadata.keyMaps.get(mpuBucket).size).toBe(2);
                        done();
                    });
            });
        });
    });

    test('should return an error if the complete ' +
    'multipart upload request contains xml that ' +
    'does not conform to the AWS spec', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, () => {
                // XML is missing any part listing so does
                // not conform to the AWS spec
                const completeBody = '<CompleteMultipartUpload>' +
                    '</CompleteMultipartUpload>';
                const completeRequest = {
                    bucketName,
                    namespace,
                    objectKey,
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: { uploadId: testUploadId },
                    post: completeBody,
                    calculatedHash,
                };
                completeMultipartUpload(authInfo, completeRequest, log, err => {
                    assert.deepStrictEqual(err, errors.MalformedXML);
                    done();
                });
            });
        });
    });

    test('should return an error if the complete ' +
    'multipart upload request contains xml with ' +
    'a part list that is not in numerical order', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const fullSizedPart = crypto.randomBytes(5 * 1024 * 1024);
            const bufferBody = Buffer.from(fullSizedPart);
            md5Hash.update(bufferBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest1 = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, fullSizedPart);
            const partRequest2 = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, fullSizedPart);
            objectPutPart(authInfo, partRequest1, undefined, log, () => {
                objectPutPart(authInfo, partRequest2, undefined, log, () => {
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>2</PartNumber>' +
                        `<ETag>"${calculatedHash}"</ETag>` +
                        '</Part>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${calculatedHash}"</ETag>` +
                        '</Part>' +
                        '</CompleteMultipartUpload>';
                    const completeRequest = {
                        bucketName,
                        namespace,
                        objectKey,
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        query: { uploadId: testUploadId },
                        post: completeBody,
                        calculatedHash,
                    };
                    completeMultipartUpload(authInfo,
                        completeRequest, log, err => {
                            assert.deepStrictEqual(err,
                                errors.InvalidPartOrder);
                            expect(metadata.keyMaps
                                                       .get(mpuBucket).size).toBe(3);
                            done();
                        });
                });
            });
        });
    });

    test('should return InvalidPart error if the complete ' +
    'multipart upload request contains xml with a missing part', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const fullSizedPart = crypto.randomBytes(5 * 1024 * 1024);
            const bufferBody = Buffer.from(fullSizedPart);
            md5Hash.update(bufferBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, fullSizedPart);
            objectPutPart(authInfo, partRequest, undefined, log, () => {
                const completeBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>99999</PartNumber>' +
                    `<ETag>"${calculatedHash}"</ETag>` +
                    '</Part>' +
                    '</CompleteMultipartUpload>';
                const completeRequest = {
                    bucketName,
                    namespace,
                    objectKey,
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: { uploadId: testUploadId },
                    post: completeBody,
                    calculatedHash,
                };
                completeMultipartUpload(authInfo, completeRequest, log, err => {
                    assert.deepStrictEqual(err,
                        errors.InvalidPart);
                    expect(metadata.keyMaps.get(mpuBucket).size).toBe(2);
                    done();
                });
            });
        });
    });

    test('should return an error if the complete multipart upload request '
    + 'contains xml with a part ETag that does not match the md5 for '
    + 'the part that was actually sent', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const wrongMD5 = '3858f62230ac3c915f300c664312c11f-9';
            const fullSizedPart = crypto.randomBytes(5 * 1024 * 1024);
            const partRequest1 = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
            }, fullSizedPart);
            const partRequest2 = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
            }, postBody);
            objectPutPart(authInfo, partRequest1, undefined, log, err => {
                assert.deepStrictEqual(err, null);
                const calculatedHash = partRequest1.calculatedHash;
                objectPutPart(authInfo, partRequest2, undefined, log, err => {
                    assert.deepStrictEqual(err, null);
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${calculatedHash}"</ETag>` +
                        '</Part>' +
                        '<Part>' +
                        '<PartNumber>2</PartNumber>' +
                        `<ETag>${wrongMD5}</ETag>` +
                        '</Part>' +
                        '</CompleteMultipartUpload>';
                    const completeRequest = {
                        bucketName,
                        namespace,
                        objectKey,
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        query: { uploadId: testUploadId },
                        post: completeBody,
                        calculatedHash,
                    };
                    expect(metadata.keyMaps.get(mpuBucket).size).toBe(3);
                    completeMultipartUpload(authInfo,
                        completeRequest, log, err => {
                            assert.deepStrictEqual(err, errors.InvalidPart);
                            done();
                        });
                });
            });
        });
    });

    test('should return an error if there is a part ' +
    'other than the last part that is less than 5MB ' +
    'in size', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest1 = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'content-length': '100',
                },
                parsedContentLength: 100,
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            const partRequest2 = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'content-length': '200',
                },
                parsedContentLength: 200,
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest1, undefined, log, () => {
                objectPutPart(authInfo, partRequest2, undefined, log, () => {
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${calculatedHash}"</ETag>` +
                        '</Part>' +
                        '<Part>' +
                        '<PartNumber>2</PartNumber>' +
                        `<ETag>"${calculatedHash}"</ETag>` +
                        '</Part>' +
                        '</CompleteMultipartUpload>';
                    const completeRequest = {
                        bucketName,
                        namespace,
                        objectKey,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        query: { uploadId: testUploadId },
                        post: completeBody,
                        calculatedHash,
                    };
                    expect(metadata.keyMaps.get(mpuBucket).size).toBe(3);
                    completeMultipartUpload(authInfo,
                        completeRequest, log, err => {
                            assert.deepStrictEqual(err,
                                                   errors.EntityTooSmall);
                            done();
                        });
                });
            });
        });
    });

    test('should aggregate the sizes of the parts', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until her
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest1 = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'content-length': '6000000',
                },
                parsedContentLength: 6000000,
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            const partRequest2 = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'content-length': '100',
                },
                parsedContentLength: 100,
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest1, undefined, log, () => {
                objectPutPart(authInfo, partRequest2, undefined, log, () => {
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${calculatedHash}"</ETag>` +
                        '</Part>' +
                        '<Part>' +
                        '<PartNumber>2</PartNumber>' +
                        `<ETag>"${calculatedHash}"</ETag>` +
                        '</Part>' +
                        '</CompleteMultipartUpload>';
                    const completeRequest = {
                        bucketName,
                        namespace,
                        objectKey,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        query: { uploadId: testUploadId },
                        post: completeBody,
                        calculatedHash,
                    };
                    completeMultipartUpload(authInfo,
                        completeRequest, log, (err, result) => {
                            expect(err).toBe(null);
                            parseString(result, err => {
                                expect(err).toBe(null);
                                const MD = metadata.keyMaps
                                                   .get(bucketName)
                                                   .get(objectKey);
                                expect(MD).toBeTruthy();
                                expect(MD['content-length']).toBe(6000100);
                                done();
                            });
                        });
                });
            });
        });
    });

    test('should set a canned ACL for a multipart upload', done => {
        const initiateRequest = {
            bucketName,
            namespace,
            objectKey,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
                'x-amz-acl': 'authenticated-read',
            },
            url: `/${objectKey}?uploads`,
        };

        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest1 = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'content-length': 6000000,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            const partRequest2 = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'content-length': 100,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest1, undefined, log, () => {
                objectPutPart(authInfo, partRequest2, undefined, log, () => {
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${calculatedHash}"</ETag>` +
                        '</Part>' +
                        '<Part>' +
                        '<PartNumber>2</PartNumber>' +
                        `<ETag>"${calculatedHash}"</ETag>` +
                        '</Part>' +
                        '</CompleteMultipartUpload>';
                    const completeRequest = {
                        bucketName,
                        namespace,
                        objectKey,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        query: { uploadId: testUploadId },
                        post: completeBody,
                        calculatedHash,
                    };
                    completeMultipartUpload(authInfo,
                        completeRequest, log, (err, result) => {
                            expect(err).toBe(null);
                            parseString(result, err => {
                                expect(err).toBe(null);
                                const MD = metadata.keyMaps
                                                   .get(bucketName)
                                                   .get(objectKey);
                                expect(MD).toBeTruthy();
                                expect(MD.acl.Canned).toBe('authenticated-read');
                                done();
                            });
                        });
                });
            });
        });
    });

    test('should set specific ACL grants for a multipart upload', done => {
        const granteeId = '79a59df900b949e55d96a1e698fbace' +
            'dfd6e09d98eacf8f8d5218e7cd47ef2be';
        const granteeEmail = 'sampleAccount1@sampling.com';
        const initiateRequest = {
            bucketName,
            namespace,
            objectKey,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-stuff': 'I am some user metadata',
                'x-amz-grant-read': `emailAddress="${granteeEmail}"`,
            },
            url: `/${objectKey}?uploads`,
        };

        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const calculatedHash = md5Hash.digest('hex');
            const partRequest1 = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'content-length': 6000000,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            const partRequest2 = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'content-length': 100,
                },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
                post: postBody,
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest1, undefined, log, () => {
                objectPutPart(authInfo, partRequest2, undefined, log, () => {
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${calculatedHash}"</ETag>` +
                        '</Part>' +
                        '<Part>' +
                        '<PartNumber>2</PartNumber>' +
                        `<ETag>"${calculatedHash}"</ETag>` +
                        '</Part>' +
                        '</CompleteMultipartUpload>';
                    const completeRequest = {
                        bucketName,
                        namespace,
                        objectKey,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        query: { uploadId: testUploadId },
                        post: completeBody,
                        calculatedHash,
                    };
                    completeMultipartUpload(authInfo,
                        completeRequest, log, (err, result) => {
                            expect(err).toBe(null);
                            parseString(result, err => {
                                expect(err).toBe(null);
                                const MD = metadata.keyMaps
                                                   .get(bucketName)
                                                   .get(objectKey);
                                expect(MD).toBeTruthy();
                                expect(MD.acl.READ[0]).toBe(granteeId);
                                done();
                            });
                        });
                });
            });
        });
    });

    test('should abort/delete a multipart upload', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const bufferMD5 = Buffer.from(postBody, 'base64');
            const calculatedHash = bufferMD5.toString('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, () => {
                const deleteRequest = {
                    bucketName,
                    namespace,
                    objectKey,
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: { uploadId: testUploadId },
                };
                expect(metadata.keyMaps.get(mpuBucket).size).toBe(2);
                multipartDelete(authInfo, deleteRequest, log, err => {
                    expect(err).toBe(null);
                    expect(metadata.keyMaps.get(mpuBucket).size).toBe(0);
                    done();
                });
            });
        });
    });

    test('should return no error if attempt to abort/delete ' +
        'a multipart upload that does not exist and not using' +
        'legacyAWSBehavior', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                expect(mpuKeys.size).toBe(1);
                parseString(result, next);
            },
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const bufferMD5 = Buffer.from(postBody, 'base64');
            const calculatedHash = bufferMD5.toString('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, () => {
                const deleteRequest = {
                    bucketName,
                    namespace,
                    objectKey,
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: { uploadId: 'non-existent-upload-id' },
                };
                expect(metadata.keyMaps.get(mpuBucket).size).toBe(2);
                multipartDelete(authInfo, deleteRequest, log, err => {
                    expect(err).toBe(null);
                    done();
                });
            });
        });
    });

    test(
        'should not leave orphans in data when overwriting an object with a MPU',
        done => {
            const fullSizedPart = crypto.randomBytes(5 * 1024 * 1024);
            const partBody = Buffer.from('I am a part\n', 'utf8');
            async.waterfall([
                next => bucketPut(authInfo, bucketPutRequest, log, next),
                (corsHeaders, next) => initiateMultipartUpload(authInfo,
                    initiateRequest, log, next),
                (result, corsHeaders, next) => parseString(result, next),
                (json, next) => {
                    const testUploadId =
                              json.InitiateMultipartUploadResult.UploadId[0];
                    const partRequest = new DummyRequest({
                        bucketName,
                        namespace,
                        objectKey,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                        query: {
                            partNumber: '1',
                            uploadId: testUploadId,
                        },
                    }, fullSizedPart);
                    objectPutPart(authInfo, partRequest, undefined, log, (err,
                        partCalculatedHash) => {
                        assert.deepStrictEqual(err, null);
                        next(null, testUploadId, partCalculatedHash);
                    });
                },
                (testUploadId, part1CalculatedHash, next) => {
                    const part2Request = new DummyRequest({
                        bucketName,
                        namespace,
                        objectKey,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                        query: {
                            partNumber: '2',
                            uploadId: testUploadId,
                        },
                    }, partBody);
                    objectPutPart(authInfo, part2Request, undefined, log, (err,
                        part2CalculatedHash) => {
                        assert.deepStrictEqual(err, null);
                        next(null, testUploadId, part1CalculatedHash,
                             part2CalculatedHash);
                    });
                },
                (testUploadId, part1CalculatedHash, part2CalculatedHash, next) => {
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${part1CalculatedHash}"</ETag>` +
                        '</Part>' +
                        '<Part>' +
                        '<PartNumber>2</PartNumber>' +
                        `<ETag>"${part2CalculatedHash}"</ETag>` +
                        '</Part>' +
                        '</CompleteMultipartUpload>';
                    const completeRequest = {
                        bucketName,
                        namespace,
                        objectKey,
                        parsedHost: 's3.amazonaws.com',
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        query: { uploadId: testUploadId },
                        post: completeBody,
                    };
                    completeMultipartUpload(authInfo, completeRequest, log,
                                            (err, result) => {
                                                assert.deepStrictEqual(err, null);
                                                next(null, result);
                                            });
                },
                (result, next) => {
                    expect(ds[0]).toBe(undefined);
                    assert.deepStrictEqual(ds[1].value, fullSizedPart);
                    assert.deepStrictEqual(ds[2].value, partBody);
                    initiateMultipartUpload(authInfo, initiateRequest, log, next);
                },
                (result, corsHeaders, next) => parseString(result, next),
                (json, next) => {
                    const testUploadId =
                        json.InitiateMultipartUploadResult.UploadId[0];
                    const overwritePartBody =
                        Buffer.from('I am an overwrite part\n', 'utf8');
                    const md5Hash = crypto.createHash('md5')
                        .update(overwritePartBody);
                    const calculatedHash = md5Hash.digest('hex');
                    const partRequest = new DummyRequest({
                        bucketName,
                        namespace,
                        objectKey,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                        query: {
                            partNumber: '1',
                            uploadId: testUploadId,
                        },
                        calculatedHash,
                    }, overwritePartBody);
                    objectPutPart(authInfo, partRequest, undefined, log, () =>
                        next(null, testUploadId, calculatedHash));
                },
                (testUploadId, calculatedHash, next) => {
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${calculatedHash}"</ETag>` +
                        '</Part>' +
                        '</CompleteMultipartUpload>';
                    const completeRequest = {
                        bucketName,
                        namespace,
                        objectKey,
                        parsedHost: 's3.amazonaws.com',
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        query: { uploadId: testUploadId },
                        post: completeBody,
                    };
                    completeMultipartUpload(authInfo, completeRequest, log, next);
                },
            ],
            err => {
                assert.deepStrictEqual(err, null);
                expect(ds[0]).toBe(undefined);
                expect(ds[1]).toBe(undefined);
                expect(ds[2]).toBe(undefined);
                assert.deepStrictEqual(ds[3].value,
                    Buffer.from('I am an overwrite part\n', 'utf8'));
                done();
            });
        }
    );

    test(
        'should not leave orphans in data when overwriting an object part',
        done => {
            const fullSizedPart = crypto.randomBytes(5 * 1024 * 1024);
            const overWritePart = Buffer.from('Overwrite content', 'utf8');
            let uploadId;

            async.waterfall([
                next => bucketPut(authInfo, bucketPutRequest, log, next),
                (corsHeaders, next) => initiateMultipartUpload(authInfo,
                    initiateRequest, log, next),
                (result, corsHeaders, next) => parseString(result, next),
                (json, next) => {
                    uploadId = json.InitiateMultipartUploadResult.UploadId[0];
                    const requestObj = {
                        bucketName,
                        namespace,
                        objectKey,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        url: `/${objectKey}?partNumber=1&uploadId=${uploadId}`,
                        query: {
                            partNumber: '1',
                            uploadId,
                        },
                    };
                    const partRequest = new DummyRequest(requestObj, fullSizedPart);
                    objectPutPart(authInfo, partRequest, undefined, log, err => {
                        assert.deepStrictEqual(err, null);
                        next(null, requestObj);
                    });
                },
                (requestObj, next) => {
                    assert.deepStrictEqual(ds[1].value, fullSizedPart);
                    const partRequest = new DummyRequest(requestObj, overWritePart);
                    objectPutPart(authInfo, partRequest, undefined, log,
                        (err, partCalculatedHash) => {
                            assert.deepStrictEqual(err, null);
                            next(null, partCalculatedHash);
                        });
                },
                (partCalculatedHash, next) => {
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${partCalculatedHash}"</ETag>` +
                        '</Part>' +
                        '</CompleteMultipartUpload>';

                    const completeRequest = {
                        bucketName,
                        namespace,
                        objectKey,
                        parsedHost: 's3.amazonaws.com',
                        url: `/${objectKey}?uploadId=${uploadId}`,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        query: { uploadId },
                        post: completeBody,
                    };
                    completeMultipartUpload(authInfo, completeRequest, log, next);
                },
            ],
            err => {
                assert.deepStrictEqual(err, null);
                expect(ds[0]).toBe(undefined);
                assert.deepStrictEqual(ds[1], undefined);
                assert.deepStrictEqual(ds[2].value, overWritePart);
                done();
            });
        }
    );

    test('should throw an error on put of an object part with an invalid' +
    'uploadId', done => {
        const testUploadId = 'invalidUploadID';
        const partRequest = new DummyRequest({
            bucketName,
            url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
            query: {
                partNumber: '1',
                uploadId: testUploadId,
            },
        }, postBody);

        bucketPut(authInfo, bucketPutRequest, log, () =>
          objectPutPart(authInfo, partRequest, undefined, log, err => {
              expect(err).toBe(errors.NoSuchUpload);
              done();
          })
        );
    });

    test('should complete an MPU with fewer parts than were originally ' +
        'put and delete data from left out parts', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const fullSizedPart = crypto.randomBytes(5 * 1024 * 1024);
            const partRequest1 = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
            }, fullSizedPart);
            const partRequest2 = new DummyRequest({
                bucketName,
                namespace,
                objectKey,
                url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                query: {
                    partNumber: '2',
                    uploadId: testUploadId,
                },
            }, postBody);
            objectPutPart(authInfo, partRequest1, undefined, log, err => {
                assert.deepStrictEqual(err, null);
                const md5Hash = crypto.createHash('md5').update(fullSizedPart);
                const calculatedHash = md5Hash.digest('hex');
                objectPutPart(authInfo, partRequest2, undefined, log, err => {
                    assert.deepStrictEqual(err, null);
                    const completeBody = '<CompleteMultipartUpload>' +
                            '<Part>' +
                            '<PartNumber>1</PartNumber>' +
                            `<ETag>"${calculatedHash}"</ETag>` +
                            '</Part>' +
                            '</CompleteMultipartUpload>';
                    const completeRequest = {
                        bucketName,
                        namespace,
                        objectKey,
                        url: `/${objectKey}?uploadId=${testUploadId}`,
                        headers: { host: `${bucketName}.s3.amazonaws.com` },
                        query: { uploadId: testUploadId },
                        post: completeBody,
                        calculatedHash,
                    };
                    // show that second part data is there
                    expect(ds[2]).toBeTruthy();
                    completeMultipartUpload(authInfo,
                        completeRequest, log, err => {
                            expect(err).toBe(null);
                            process.nextTick(() => {
                                // data has been deleted
                                expect(ds[2]).toBe(undefined);
                                done();
                            });
                        });
                });
            });
        });
    });
});

describe('complete mpu with versioning', () => {
    const objData = ['foo0', 'foo1', 'foo2'].map(str =>
        Buffer.from(str, 'utf8'));

    const enableVersioningRequest =
        versioningTestUtils.createBucketPutVersioningReq(bucketName, 'Enabled');
    const suspendVersioningRequest = versioningTestUtils
        .createBucketPutVersioningReq(bucketName, 'Suspended');
    const testPutObjectRequests = objData.slice(0, 2).map(data =>
        versioningTestUtils.createPutObjectRequest(bucketName, objectKey,
            data));

    beforeAll(done => {
        cleanup();
        async.series([
            callback => bucketPut(authInfo, bucketPutRequest, log,
                callback),
            // putting null version: put obj before versioning configured
            callback => objectPut(authInfo, testPutObjectRequests[0],
                undefined, log, callback),
            callback => bucketPutVersioning(authInfo,
                enableVersioningRequest, log, callback),
            // put another version:
            callback => objectPut(authInfo, testPutObjectRequests[1],
                undefined, log, callback),
            callback => bucketPutVersioning(authInfo,
                suspendVersioningRequest, log, callback),
        ], err => {
            if (err) {
                return done(err);
            }
            versioningTestUtils.assertDataStoreValues(ds, objData.slice(0, 2));
            return done();
        });
    });

    afterAll(done => {
        cleanup();
        done();
    });

    test('should delete null version when creating new null version, ' +
    'even when null version is not the latest version', done => {
        async.waterfall([
            next =>
                initiateMultipartUpload(authInfo, initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
            (json, next) => {
                const partBody = objData[2];
                const testUploadId =
                    json.InitiateMultipartUploadResult.UploadId[0];
                const partRequest = _createPutPartRequest(testUploadId, 1,
                    partBody);
                objectPutPart(authInfo, partRequest, undefined, log,
                    (err, eTag) => next(err, eTag, testUploadId));
            },
            (eTag, testUploadId, next) => {
                const parts = [{ partNumber: 1, eTag }];
                const completeRequest = _createCompleteMpuRequest(testUploadId,
                    parts);
                completeMultipartUpload(authInfo, completeRequest, log, next);
            },
        ], err => {
            assert.ifError(err, `Unexpected err: ${err}`);
            process.nextTick(() => {
                versioningTestUtils.assertDataStoreValues(ds, [undefined,
                    objData[1], objData[2]]);
                done(err);
            });
        });
    });
});
