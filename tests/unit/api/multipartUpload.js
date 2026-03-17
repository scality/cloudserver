const { errors, storage, versioning } = require('arsenal');

const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const moment = require('moment');
const sinon = require('sinon');
const util = require('util');
const { parseString, parseStringPromise } = require('xml2js');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutPolicy = require('../../../lib/api/bucketPutPolicy');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const objectPut = require('../../../lib/api/objectPut');
const completeMultipartUpload
    = require('../../../lib/api/completeMultipartUpload');
const constants = require('../../../constants');
const { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils }
    = require('../helpers');
const getObjectLegalHold = require('../../../lib/api/objectGetLegalHold');
const getObjectRetention = require('../../../lib/api/objectGetRetention');
const initiateMultipartUpload
    = require('../../../lib/api/initiateMultipartUpload');
const multipartDelete = require('../../../lib/api/multipartDelete');
const objectPutPart = require('../../../lib/api/objectPutPart');
const DummyRequest = require('../DummyRequest');
const changeObjectLock = require('../../utilities/objectLock-util');
const metadataswitch = require('../metadataswitch');
const { fakeMetadataArchive } = require('../../functional/aws-node-sdk/test/utils/init');
const { config } = require('../../../lib/Config');

const {
    LOCATION_NAME_CRR,
} = require('../../constants');

const { data } = require('../../../lib/data/wrapper');
const { metadata } = storage.metadata.inMemory.metadata;
const metadataBackend = storage.metadata.inMemory.metastore;
const { ds } = storage.data.inMemory.datastore;

const log = new DummyRequestLogger();

const splitter = constants.splitter;
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const authInfoOtherAcc = makeAuthInfo('accessKey2');
const namespace = 'default';
const bucketName = 'bucketname';
const lockedBucket = 'objectlockenabledbucket';
const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;
const postBody = Buffer.from('I am a body', 'utf8');
const futureDate = moment().add(1, 'Days').toISOString();
const objectKey = 'testObject';
const bucketPutRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    post: '<CreateBucketConfiguration ' +
    'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
    '<LocationConstraint>scality-internal-mem</LocationConstraint>' +
    '</CreateBucketConfiguration >',
    actionImplicitDenies: false,
};
const lockEnabledBucketRequest = Object.assign({}, bucketPutRequest);
lockEnabledBucketRequest.bucketName = lockedBucket;
lockEnabledBucketRequest.headers = {
    'host': `${lockedBucket}.s3.amazonaws.com`,
    'x-amz-bucket-object-lock-enabled': 'true',
};
const initiateRequest = {
    socket: {
        remoteAddress: '1.1.1.1',
    },
    bucketName,
    namespace,
    objectKey,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: `/${objectKey}?uploads`,
    actionImplicitDenies: false,
};
const retentionInitiateRequest = Object.assign({}, initiateRequest);
retentionInitiateRequest.bucketName = lockedBucket;
retentionInitiateRequest.headers = {
    'x-amz-object-lock-mode': 'GOVERNANCE',
    'x-amz-object-lock-retain-until-date': futureDate,
    'host': `${lockedBucket}.s3.amazonaws.com`,
};
const legalHoldInitiateRequest = Object.assign({}, initiateRequest);
legalHoldInitiateRequest.bucketName = lockedBucket;
legalHoldInitiateRequest.headers = {
    'x-amz-object-lock-legal-hold': 'ON',
    'host': `${lockedBucket}.s3.amazonaws.com`,
};

const getObjectLockInfoRequest = {
    bucketName: lockedBucket,
    namespace,
    objectKey,
    headers: { host: `${lockedBucket}.s3.amazonaws.com` },
    actionImplicitDenies: false,
};
const expectedRetentionConfig = {
    $: { xmlns: 'http://s3.amazonaws.com/doc/2006-03-01/' },
    Mode: ['GOVERNANCE'],
    RetainUntilDate: [futureDate],
};
const expectedLegalHold = {
    Status: ['ON'],
};

function _createPutPartRequest(uploadId, partNumber, partBody) {
    const md5Hash = crypto.createHash('md5').update(partBody);
    const partHash = md5Hash.digest('hex');
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
        partHash,
        actionImplicitDenies: false,
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
        actionImplicitDenies: false,
    };
}

const _bucketPut = util.promisify(bucketPut);

async function _uploadMpuObject(params = {}) {
    const _initiateMultipartUpload = async (...params) => {
        const result = await util.promisify(initiateMultipartUpload)(...params);
        const json = await parseStringPromise(result);
        return json.InitiateMultipartUploadResult.UploadId[0];
    };
    const _objectPutPart = util.promisify(objectPutPart);
    const _completeMultipartUpload = (...params) => util.promisify(cb =>
        completeMultipartUpload(...params, (err, xml, headers) => cb(err, { xml, headers })))();

    const headers = { ...initiateRequest.headers };
    if (params.location) {
        headers[constants.objectLocationConstraintHeader] = params.location;
    }
    if (params.versionID) {
        headers['x-scal-s3-version-id'] = params.versionID;
    }

    const uploadId = await _initiateMultipartUpload(authInfo, { ...initiateRequest, headers }, log);

    const partRequest = _createPutPartRequest(uploadId, 1, Buffer.from('I am a part\n', 'utf8'));
    partRequest.headers = headers;
    const eTag = await _objectPutPart(authInfo, partRequest, undefined, log);

    const completeRequest = _createCompleteMpuRequest(uploadId, [{ partNumber: 1, eTag }]);
    const resp = await _completeMultipartUpload(authInfo, { ...completeRequest, headers }, log);

    return resp.headers;
}

describe('Multipart Upload API', () => {
    beforeEach(() => {
        cleanup();

        sinon.spy(metadataswitch, 'putObjectMD');
    });

    afterEach(() => {
        sinon.restore();
    });

    it('mpuBucketPrefix should be a defined constant', () => {
        assert(constants.mpuBucketPrefix,
            'Expected mpuBucketPrefix to be defined');
    });

    it('should initiate a multipart upload', done => {
        bucketPut(authInfo, bucketPutRequest, log, err => {
            assert.ifError(err);
            initiateMultipartUpload(authInfo, initiateRequest,
                log, (err, result) => {
                    assert.ifError(err);
                    parseString(result, (err, json) => {
                        assert.strictEqual(json.InitiateMultipartUploadResult
                            .Bucket[0], bucketName);
                        assert.strictEqual(json.InitiateMultipartUploadResult
                            .Key[0], objectKey);
                        assert(json.InitiateMultipartUploadResult.UploadId[0]);
                        assert(metadata.buckets.get(mpuBucket)._name,
                            mpuBucket);
                        const mpuKeys = metadata.keyMaps.get(mpuBucket);
                        assert.strictEqual(mpuKeys.size, 1);
                        assert(mpuKeys.keys().next().value
                            .startsWith(`overview${splitter}${objectKey}`));
                        done();
                    });
                });
        });
    });

    it('should return an error on an initiate multipart upload call if ' +
        'no destination bucket', done => {
        initiateMultipartUpload(authInfo, initiateRequest,
            log, err => {
                assert(err.is.NoSuchBucket);
                done();
            });
    });

    it('should not mpu with storage-class header not equal to STANDARD', done => {
        const initiateRequestCold = {
            bucketName,
            namespace,
            objectKey,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-storage-class': 'COLD',
            },
            url: `/${objectKey}?uploads`,
        };
        initiateMultipartUpload(authInfo, initiateRequestCold,
            log, err => {
                assert.strictEqual(err.is.InvalidStorageClass, true);
                done();
            });
    });

    it('should upload a part', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                assert.strictEqual(mpuKeys.size, 1);
                assert(mpuKeys.keys().next().value
                    .startsWith(`overview${splitter}${objectKey}`));
                parseString(result, next);
            },
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            assert.ifError(err);
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const partHash = md5Hash.digest('hex');
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
                partHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, err => {
                assert.ifError(err);
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
                assert.strictEqual(keysInMPUkeyMap.length, 2);
                assert.strictEqual(metadata.keyMaps.get(mpuBucket)
                                                   .get(overviewEntry).key,
                                   objectKey);
                assert.strictEqual(partUploadId, testUploadId);
                assert.strictEqual(firstPartNumber, '00001');
                assert.strictEqual(partETag, partHash);
                done();
            });
        });
    });

    it('should upload a part even if the client sent a base 64 ETag ' +
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
            assert.ifError(err);
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            const partHash = md5Hash.update(bufferBody).digest('hex');
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
                partHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, err => {
                assert.ifError(err);
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
                assert.strictEqual(keysInMPUkeyMap.length, 2);
                assert.strictEqual(partETag, partHash);
                done();
            });
        });
    });

    it('should return an error if too many parts', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            assert.ifError(err);
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const partHash = md5Hash.digest('hex');
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
                partHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log,
                (err, result) => {
                    assert(err.is.TooManyParts);
                    assert.strictEqual(result, undefined);
                    done();
                });
        });
    });

    it('should return an error if part number is not an integer', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            assert.ifError(err);
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const partHash = md5Hash.digest('hex');
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
                partHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log,
                (err, result) => {
                    assert(err.is.InvalidArgument);
                    assert.strictEqual(result, undefined);
                    done();
                });
        });
    });

    it('should return an error if content-length is too large', done => {
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
            assert.ifError(err);
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const partHash = md5Hash.digest('hex');
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
                partHash,
                parsedContentLength: 5368709121,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined,
                log, (err, result) => {
                    assert(err.is.EntityTooLarge);
                    assert.strictEqual(result, undefined);
                    done();
                });
        });
    });

    it('should upload two parts', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            assert.ifError(err);
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const partHash = md5Hash.digest('hex');
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
                partHash,
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
                    partHash: secondCalculatedMD5,
                }, postBody2);
                objectPutPart(authInfo, partRequest2, undefined, log, err => {
                    assert.ifError(err);

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
                    assert.strictEqual(keysInMPUkeyMap.length, 3);
                    assert.strictEqual(metadata
                        .keyMaps.get(mpuBucket).get(overviewEntry).key,
                        objectKey);
                    assert.strictEqual(partUploadId, testUploadId);
                    assert.strictEqual(secondPartNumber, '00002');
                    assert.strictEqual(secondPartETag, secondCalculatedMD5);
                    done();
                });
            });
        });
    });

    it('should complete a multipart upload', done => {
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
            assert.ifError(err);
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const partHash = crypto.createHash('md5').update(partBody).digest('hex');
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
                // What is being tested is that the partHash being sent
                // to the API for the part is stored and then used to
                // calculate the final ETag upon completion
                // of the multipart upload.
                partHash,
            }, partBody);
            objectPutPart(authInfo, partRequest, undefined, log, () => {
                const completeBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>1</PartNumber>' +
                    `<ETag>"${partHash}"</ETag>` +
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
                    actionImplicitDenies: false,
                };
                const awsVerifiedETag =
                    '"953e9e776f285afc0bfcf1ab4668299d-1"';
                completeMultipartUpload(authInfo,
                    completeRequest, log, (err, result) => {
                        assert.ifError(err);
                        parseString(result, (err, json) => {
                            assert.ifError(err);
                            assert.strictEqual(
                                json.CompleteMultipartUploadResult.Location[0],
                                `http://${bucketName}.s3.amazonaws.com`
                                + `/${objectKey}`);
                            assert.strictEqual(
                                json.CompleteMultipartUploadResult.Bucket[0],
                                bucketName);
                            assert.strictEqual(
                                json.CompleteMultipartUploadResult.Key[0],
                                objectKey);
                            assert.strictEqual(
                                json.CompleteMultipartUploadResult.ETag[0],
                                awsVerifiedETag);
                            const MD = metadata.keyMaps.get(bucketName)
                                                       .get(objectKey);
                            assert(MD);
                            assert.strictEqual(MD['x-amz-meta-stuff'],
                                'I am some user metadata');
                            assert.strictEqual(MD.uploadId, testUploadId);
                            done();
                        });
                    });
            });
        });
    });

    it('should complete a multipart upload even if etag is sent ' +
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
            assert.ifError(err);
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const partHash = crypto.createHash('md5').update(partBody).digest('hex');
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
                partHash,
            }, partBody);
            objectPutPart(authInfo, partRequest, undefined, log, () => {
                const completeBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>1</PartNumber>' +
                    // ETag without quotes
                    `<ETag>${partHash}</ETag>` +
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
                    actionImplicitDenies: false,
                };
                const awsVerifiedETag =
                    '"953e9e776f285afc0bfcf1ab4668299d-1"';
                completeMultipartUpload(authInfo,
                    completeRequest, log, (err, result) => {
                        assert.ifError(err);
                        parseString(result, (err, json) => {
                            assert.ifError(err);
                            assert.strictEqual(
                                json.CompleteMultipartUploadResult.Location[0],
                                `http://${bucketName}.s3.amazonaws.com`
                                + `/${objectKey}`);
                            assert.strictEqual(
                                json.CompleteMultipartUploadResult.Bucket[0],
                                bucketName);
                            assert.strictEqual(
                                json.CompleteMultipartUploadResult.Key[0],
                                objectKey);
                            assert.strictEqual(
                                json.CompleteMultipartUploadResult.ETag[0],
                                awsVerifiedETag);
                            const MD = metadata.keyMaps.get(bucketName)
                                                       .get(objectKey);
                            assert(MD);
                            assert.strictEqual(MD['x-amz-meta-stuff'],
                                               'I am some user metadata');
                            done();
                        });
                    });
            });
        });
    });

    it('should return an error if a complete multipart upload' +
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
            const partHash = md5Hash.digest('hex');
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
                partHash,
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
                    partHash,
                    actionImplicitDenies: false,
                };
                completeMultipartUpload(authInfo,
                    completeRequest, log, err => {
                        assert.strictEqual(err.is.MalformedXML, true);
                        assert.strictEqual(metadata.keyMaps.get(mpuBucket).size,
                                           2);
                        done();
                    });
            });
        });
    });

    it('should return an error if the complete ' +
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
            const partHash = md5Hash.digest('hex');
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
                partHash,
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
                    partHash,
                    actionImplicitDenies: false,
                };
                completeMultipartUpload(authInfo, completeRequest, log, err => {
                    assert(err.is.MalformedXML);
                    done();
                });
            });
        });
    });

    it('should return an error if the complete ' +
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
            const partHash = md5Hash.digest('hex');
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
                partHash,
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
                partHash,
            }, fullSizedPart);
            objectPutPart(authInfo, partRequest1, undefined, log, () => {
                objectPutPart(authInfo, partRequest2, undefined, log, () => {
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>2</PartNumber>' +
                        `<ETag>"${partHash}"</ETag>` +
                        '</Part>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${partHash}"</ETag>` +
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
                        partHash,
                        actionImplicitDenies: false,
                    };
                    completeMultipartUpload(authInfo,
                        completeRequest, log, err => {
                            assert(err.is.InvalidPartOrder);
                            assert.strictEqual(metadata.keyMaps
                                                       .get(mpuBucket).size, 3);
                            done();
                        });
                });
            });
        });
    });

    it('should return InvalidPart error if the complete ' +
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
            const partHash = md5Hash.digest('hex');
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
                partHash,
            }, fullSizedPart);
            objectPutPart(authInfo, partRequest, undefined, log, () => {
                const completeBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>99999</PartNumber>' +
                    `<ETag>"${partHash}"</ETag>` +
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
                    partHash,
                    actionImplicitDenies: false,
                };
                completeMultipartUpload(authInfo, completeRequest, log, err => {
                    assert(err.is.InvalidPart);
                    assert.strictEqual(metadata.keyMaps.get(mpuBucket).size, 2);
                    done();
                });
            });
        });
    });

    it('should return an error if the complete multipart upload request '
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
                const partHash = partRequest1.partHash;
                objectPutPart(authInfo, partRequest2, undefined, log, err => {
                    assert.deepStrictEqual(err, null);
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${partHash}"</ETag>` +
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
                        partHash,
                        actionImplicitDenies: false,
                    };
                    assert.strictEqual(metadata.keyMaps.get(mpuBucket).size, 3);
                    completeMultipartUpload(authInfo,
                        completeRequest, log, err => {
                            assert(err.is.InvalidPart);
                            done();
                        });
                });
            });
        });
    });

    it('should return an error if there is a part ' +
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
            const partHash = md5Hash.digest('hex');
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
                partHash,
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
                partHash,
            }, postBody);
            objectPutPart(authInfo, partRequest1, undefined, log, () => {
                objectPutPart(authInfo, partRequest2, undefined, log, () => {
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${partHash}"</ETag>` +
                        '</Part>' +
                        '<Part>' +
                        '<PartNumber>2</PartNumber>' +
                        `<ETag>"${partHash}"</ETag>` +
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
                        partHash,
                        actionImplicitDenies: false,
                    };
                    assert.strictEqual(metadata.keyMaps.get(mpuBucket).size, 3);
                    completeMultipartUpload(authInfo,
                        completeRequest, log, err => {
                            assert(err.is.EntityTooSmall);
                            done();
                        });
                });
            });
        });
    });

    it('should aggregate the sizes of the parts', done => {
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
            const partHash = md5Hash.digest('hex');
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
                partHash,
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
                partHash,
            }, postBody);
            objectPutPart(authInfo, partRequest1, undefined, log, () => {
                objectPutPart(authInfo, partRequest2, undefined, log, () => {
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${partHash}"</ETag>` +
                        '</Part>' +
                        '<Part>' +
                        '<PartNumber>2</PartNumber>' +
                        `<ETag>"${partHash}"</ETag>` +
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
                        partHash,
                        actionImplicitDenies: false,
                    };
                    completeMultipartUpload(authInfo,
                        completeRequest, log, (err, result) => {
                            assert.strictEqual(err, null);
                            parseString(result, err => {
                                assert.strictEqual(err, null);
                                const MD = metadata.keyMaps
                                                   .get(bucketName)
                                                   .get(objectKey);
                                assert(MD);
                                assert.strictEqual(MD['content-length'],
                                                   6000100);
                                done();
                            });
                        });
                });
            });
        });
    });

    it('should set a canned ACL for a multipart upload', done => {
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
            actionImplicitDenies: false,
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
            const partHash = md5Hash.digest('hex');
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
                partHash,
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
                partHash,
            }, postBody);
            objectPutPart(authInfo, partRequest1, undefined, log, () => {
                objectPutPart(authInfo, partRequest2, undefined, log, () => {
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${partHash}"</ETag>` +
                        '</Part>' +
                        '<Part>' +
                        '<PartNumber>2</PartNumber>' +
                        `<ETag>"${partHash}"</ETag>` +
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
                        partHash,
                        actionImplicitDenies: false,
                    };
                    completeMultipartUpload(authInfo,
                        completeRequest, log, (err, result) => {
                            assert.strictEqual(err, null);
                            parseString(result, err => {
                                assert.strictEqual(err, null);
                                const MD = metadata.keyMaps
                                                   .get(bucketName)
                                                   .get(objectKey);
                                assert(MD);
                                assert.strictEqual(MD.acl.Canned,
                                                   'authenticated-read');
                                done();
                            });
                        });
                });
            });
        });
    });

    it('should set specific ACL grants for a multipart upload', done => {
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
            actionImplicitDenies: false,
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
            const partHash = md5Hash.digest('hex');
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
                partHash,
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
                partHash,
            }, postBody);
            objectPutPart(authInfo, partRequest1, undefined, log, () => {
                objectPutPart(authInfo, partRequest2, undefined, log, () => {
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${partHash}"</ETag>` +
                        '</Part>' +
                        '<Part>' +
                        '<PartNumber>2</PartNumber>' +
                        `<ETag>"${partHash}"</ETag>` +
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
                        partHash,
                        actionImplicitDenies: false,
                    };
                    completeMultipartUpload(authInfo,
                        completeRequest, log, (err, result) => {
                            assert.strictEqual(err, null);
                            parseString(result, err => {
                                assert.strictEqual(err, null);
                                const MD = metadata.keyMaps
                                                   .get(bucketName)
                                                   .get(objectKey);
                                assert(MD);
                                assert.strictEqual(MD.acl.READ[0], granteeId);
                                done();
                            });
                        });
                });
            });
        });
    });

    it('should abort/delete a multipart upload', done => {
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
            const partHash = bufferMD5.toString('hex');
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
                partHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, () => {
                const deleteRequest = {
                    bucketName,
                    namespace,
                    objectKey,
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: { uploadId: testUploadId },
                    actionImplicitDenies: false,
                };
                assert.strictEqual(metadata.keyMaps.get(mpuBucket).size, 2);
                multipartDelete(authInfo, deleteRequest, log, err => {
                    assert.strictEqual(err, null);
                    assert.strictEqual(metadata.keyMaps.get(mpuBucket).size, 0);
                    done();
                });
            });
        });
    });

    it('should return no error if attempt to abort/delete ' +
        'a multipart upload that does not exist and not using ' +
        'legacyAWSBehavior', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                assert.strictEqual(mpuKeys.size, 1);
                parseString(result, next);
            },
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const bufferMD5 = Buffer.from(postBody, 'base64');
            const partHash = bufferMD5.toString('hex');
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
                partHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, () => {
                const deleteRequest = {
                    bucketName,
                    namespace,
                    objectKey,
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: { uploadId: 'non-existent-upload-id' },
                    actionImplicitDenies: false,
                };
                assert.strictEqual(metadata.keyMaps.get(mpuBucket).size, 2);
                multipartDelete(authInfo, deleteRequest, log, err => {
                    assert.strictEqual(err, null,
                        `Expected no err but got ${err}`);
                    done();
                });
            });
        });
    });

    it('should not leave orphans in data when overwriting an object with a MPU',
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
                    partpartHash) => {
                    assert.deepStrictEqual(err, null);
                    next(null, testUploadId, partpartHash);
                });
            },
            (testUploadId, part1partHash, next) => {
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
                    part2partHash) => {
                    assert.deepStrictEqual(err, null);
                    next(null, testUploadId, part1partHash,
                         part2partHash);
                });
            },
            (testUploadId, part1partHash, part2partHash, next) => {
                const completeBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>1</PartNumber>' +
                    `<ETag>"${part1partHash}"</ETag>` +
                    '</Part>' +
                    '<Part>' +
                    '<PartNumber>2</PartNumber>' +
                    `<ETag>"${part2partHash}"</ETag>` +
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
                    actionImplicitDenies: false,
                };
                completeMultipartUpload(authInfo, completeRequest, log,
                                        (err, result) => {
                                            assert.deepStrictEqual(err, null);
                                            next(null, result);
                                        });
            },
            (result, next) => {
                assert.strictEqual(ds[0], undefined);
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
                const partHash = md5Hash.digest('hex');
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
                    partHash,
                }, overwritePartBody);
                objectPutPart(authInfo, partRequest, undefined, log, () =>
                    next(null, testUploadId, partHash));
            },
            (testUploadId, partHash, next) => {
                const completeBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>1</PartNumber>' +
                    `<ETag>"${partHash}"</ETag>` +
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
                    actionImplicitDenies: false,
                };
                completeMultipartUpload(authInfo, completeRequest, log, next);
            },
        ],
        err => {
            assert.deepStrictEqual(err, null);
            assert.strictEqual(ds[0], undefined);
            assert.strictEqual(ds[1], undefined);
            assert.strictEqual(ds[2], undefined);
            assert.deepStrictEqual(ds[3].value,
                Buffer.from('I am an overwrite part\n', 'utf8'));
            done();
        });
    });

    it('should not leave orphans in data when overwriting an object part',
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
                    (err, partpartHash) => {
                        assert.deepStrictEqual(err, null);
                        next(null, partpartHash);
                    });
            },
            (partpartHash, next) => {
                const completeBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>1</PartNumber>' +
                    `<ETag>"${partpartHash}"</ETag>` +
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
                    actionImplicitDenies: false,
                };
                completeMultipartUpload(authInfo, completeRequest, log, next);
            },
        ],
        err => {
            assert.deepStrictEqual(err, null);
            assert.strictEqual(ds[0], undefined);
            assert.deepStrictEqual(ds[1], undefined);
            assert.deepStrictEqual(ds[2].value, overWritePart);
            done();
        });
    });

    it('should leave orphaned data when overwriting an object part during completeMPU',
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
                objectPutPart(authInfo, partRequest, undefined, log, (err, partpartHash) => {
                    assert.deepStrictEqual(err, null);
                    next(null, requestObj, partpartHash);
                });
            },
            (requestObj, partpartHash, next) => {
                assert.deepStrictEqual(ds[1].value, fullSizedPart);
                async.parallel([
                    done => {
                        const partRequest = new DummyRequest(requestObj, overWritePart);
                        objectPutPart(authInfo, partRequest, undefined, log, err => {
                            assert.deepStrictEqual(err, null);
                            done();
                        });
                    },
                    done => {
                        const completeBody = '<CompleteMultipartUpload>' +
                              '<Part>' +
                              '<PartNumber>1</PartNumber>' +
                              `<ETag>"${partpartHash}"</ETag>` +
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
                            actionImplicitDenies: false,
                        };
                        completeMultipartUpload(authInfo, completeRequest, log, done);
                    },
                ], err => next(err));
            },
        ],
        err => {
            assert.deepStrictEqual(err, null);
            assert.strictEqual(ds[0], undefined);
            assert.deepStrictEqual(ds[1].value, fullSizedPart);
            assert.deepStrictEqual(ds[2].value, overWritePart);
            done();
        });
    });

    it('should throw an error on put of an object part with an invalid ' +
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
              assert(err.is.NoSuchUpload);
              done();
          })
        );
    });

    it('should complete an MPU with fewer parts than were originally ' +
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
            assert.ifError(err);
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
                const partHash = md5Hash.digest('hex');
                objectPutPart(authInfo, partRequest2, undefined, log, err => {
                    assert.deepStrictEqual(err, null);
                    const completeBody = '<CompleteMultipartUpload>' +
                            '<Part>' +
                            '<PartNumber>1</PartNumber>' +
                            `<ETag>"${partHash}"</ETag>` +
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
                        partHash,
                        actionImplicitDenies: false,
                    };
                    // show that second part data is there
                    assert(ds[2]);
                    completeMultipartUpload(authInfo,
                        completeRequest, log, err => {
                            assert.strictEqual(err, null);
                            process.nextTick(() => {
                                // data has been deleted
                                assert.strictEqual(ds[2], undefined);
                                done();
                            });
                        });
                });
            });
        });
    });

    it('should not delete data locations on completeMultipartUpload retry',
    done => {
        const partBody = Buffer.from('foo', 'utf8');
        let origDeleteObject;
        async.waterfall([
            next =>
                bucketPut(authInfo, bucketPutRequest, log, err => next(err)),
            next =>
                initiateMultipartUpload(authInfo, initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
            (json, next) => {
                const testUploadId =
                    json.InitiateMultipartUploadResult.UploadId[0];
                const partRequest = _createPutPartRequest(testUploadId, 1,
                    partBody);
                objectPutPart(authInfo, partRequest, undefined, log,
                    (err, eTag) => next(err, eTag, testUploadId));
            },
            (eTag, testUploadId, next) => {
                origDeleteObject = metadataBackend.deleteObject;
                metadataBackend.deleteObject = (
                    bucketName, objName, params, log, cb) => {
                        // prevent deletions from MPU bucket only
                    if (bucketName === mpuBucket) {
                        return process.nextTick(
                            () => cb(errors.InternalError));
                    }
                    return origDeleteObject(
                        bucketName, objName, params, log, cb);
                };
                const parts = [{ partNumber: 1, eTag }];
                const completeRequest = _createCompleteMpuRequest(
                    testUploadId, parts);
                completeMultipartUpload(authInfo, completeRequest, log, err => {
                    // expect a failure here because we could not
                    // remove the overview key
                    assert(err.is.InternalError);
                    next(null, eTag, testUploadId);
                });
            },
            (eTag, testUploadId, next) => {
                // allow MPU bucket metadata deletions to happen again
                metadataBackend.deleteObject = origDeleteObject;
                // retry the completeMultipartUpload with the same
                // metadata, as an application would normally do after
                // a failure
                const parts = [{ partNumber: 1, eTag }];
                const completeRequest = _createCompleteMpuRequest(
                    testUploadId, parts);
                completeMultipartUpload(authInfo, completeRequest, log, next);
            },
        ], err => {
            assert.ifError(err);
            // check that the original data has not been deleted
            // during the replay
            assert.strictEqual(ds[0], undefined);
            assert.notStrictEqual(ds[1], undefined);
            assert.deepStrictEqual(ds[1].value, partBody);
            done();
        });
    });

    it('should abort an MPU and delete its MD if it has been created by a failed complete before', done => {
        const delMeta = metadataBackend.deleteObject;
        metadataBackend.deleteObject = (bucketName, objName, params, log, cb) => cb(errors.InternalError);
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
            assert.ifError(err);
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const partHash = crypto.createHash('md5').update(partBody).digest('hex');
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
                partHash,
            }, partBody);
            objectPutPart(authInfo, partRequest, undefined, log, () => {
                const completeBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>1</PartNumber>' +
                    `<ETag>"${partHash}"</ETag>` +
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
                    actionImplicitDenies: false,
                };
                completeMultipartUpload(authInfo,
                    completeRequest, log, err => {
                        assert(err.is.InternalError);
                        const MD = metadata.keyMaps.get(bucketName)
                                                    .get(objectKey);
                        assert(MD);
                        assert.strictEqual(MD['x-amz-meta-stuff'],
                            'I am some user metadata');
                        assert.strictEqual(MD.uploadId, testUploadId);

                        metadataBackend.deleteObject = delMeta;
                        const deleteRequest = {
                            bucketName,
                            namespace,
                            objectKey,
                            url: `/${objectKey}?uploadId=${testUploadId}`,
                            headers: { host: `${bucketName}.s3.amazonaws.com` },
                            query: { uploadId: testUploadId },
                            actionImplicitDenies: false,
                        };
                        assert.strictEqual(metadata.keyMaps.get(mpuBucket).size, 2);
                        multipartDelete(authInfo, deleteRequest, log, err => {
                            assert.strictEqual(err, null);
                            assert.strictEqual(metadata.keyMaps.get(mpuBucket).size, 0);
                            done();
                        });
                    });
            });
        });
    });

    it('should complete an MPU and promote its MD if it has been created by a failed complete before', done => {
        const delMeta = metadataBackend.deleteObject;
        metadataBackend.deleteObject = (bucketName, objName, params, log, cb) => cb(errors.InternalError);
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
            assert.ifError(err);
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const partHash = crypto.createHash('md5').update(partBody).digest('hex');
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
                partHash,
            }, partBody);
            objectPutPart(authInfo, partRequest, undefined, log, () => {
                const completeBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>1</PartNumber>' +
                    `<ETag>"${partHash}"</ETag>` +
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
                    actionImplicitDenies: false,
                };
                completeMultipartUpload(authInfo,
                    completeRequest, log, err => {
                        assert(err.is.InternalError);
                        const MD = metadata.keyMaps.get(bucketName).get(objectKey);
                        assert(MD);
                        assert.strictEqual(MD['x-amz-meta-stuff'],
                            'I am some user metadata');
                        assert.strictEqual(MD.uploadId, testUploadId);
                        metadataBackend.deleteObject = delMeta;
                        assert.strictEqual(metadata.keyMaps.get(mpuBucket).size, 2);
                        completeMultipartUpload(authInfo,
                            completeRequest, log, err => {
                                assert.ifError(err);
                                const MD = metadata.keyMaps.get(bucketName)
                                                    .get(objectKey);
                                assert(MD);
                                assert.strictEqual(MD['x-amz-meta-stuff'],
                                    'I am some user metadata');
                                assert.strictEqual(metadata.keyMaps.get(mpuBucket).size, 0);
                                done();
                            });
                    });
            });
        });
    });

    it('should not pass needOplogUpdate when writing new object', done => {
        async.series([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            async () => _uploadMpuObject(),
            async () => {
                const options = metadataswitch.putObjectMD.lastCall.args[3];
                assert.strictEqual(options.needOplogUpdate, undefined);
                assert.strictEqual(options.originOp, undefined);
            },
        ], done);
    });

    it('should not pass needOplogUpdate when replacing object', done => {
        async.series([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            async () => _uploadMpuObject(),
            async () => _uploadMpuObject(),
            async () => {
                const options = metadataswitch.putObjectMD.lastCall.args[3];
                assert.strictEqual(options.needOplogUpdate, undefined);
                assert.strictEqual(options.originOp, undefined);
            },
        ], done);
    });

    it('should pass needOplogUpdate to metadata when replacing archived object', done => {
        const archived = {
            archiveInfo: { foo: 0, bar: 'stuff' }
        };

        async.series([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            async () => _uploadMpuObject(),
            next => fakeMetadataArchive(bucketName, objectKey, undefined, archived, next),
            async () => _uploadMpuObject(),
            async () => {
                const options = metadataswitch.putObjectMD.lastCall.args[3];
                assert.strictEqual(options.needOplogUpdate, true);
                assert.strictEqual(options.originOp, 's3:ReplaceArchivedObject');
            },
        ], done);
    });

    it('should pass needOplogUpdate to metadata when replacing archived object in version suspended bucket', done => {
        const archived = {
            archiveInfo: { foo: 0, bar: 'stuff' }
        };

        const suspendVersioningRequest = versioningTestUtils
            .createBucketPutVersioningReq(bucketName, 'Suspended');
        async.series([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            next => bucketPutVersioning(authInfo, suspendVersioningRequest, log, next),
            async () => _uploadMpuObject(),
            next => fakeMetadataArchive(bucketName, objectKey, undefined, archived, next),
            async () => _uploadMpuObject(),
            async () => {
                const options = metadataswitch.putObjectMD.lastCall.args[3];
                assert.strictEqual(options.needOplogUpdate, true);
                assert.strictEqual(options.originOp, 's3:ReplaceArchivedObject');
            },
        ], done);
    });

    it('should fail to initiate a multipart upload if location constraint is crr', done => {
        const initiateRequest = {
            socket: {
                remoteAddress: '1.1.1.1',
            },
            bucketName,
            namespace,
            objectKey,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                [constants.objectLocationConstraintHeader]: LOCATION_NAME_CRR,
            },
            url: `/${objectKey}?uploads`,
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, bucketPutRequest, log, err => {
            assert.ifError(err);
            initiateMultipartUpload(authInfo, initiateRequest,
                log, err => {
                    assert(err.is.InvalidArgument);
                    done();
                });
        });
    });

    it('should return a retryable error if deletePartsMetadata fails', done => {
        const partBody = Buffer.from('I am a part\n', 'utf8');
        let batchDeleteStub;

        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            assert.ifError(err);
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const partHash = crypto.createHash('md5').update(partBody).digest('hex');
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
                partHash,
            }, partBody);

            objectPutPart(authInfo, partRequest, undefined, log, () => {
                // Mock batchDeleteObjectMetadata to fail with non-retryable error
                const services = require('../../../lib/services');
                batchDeleteStub = sinon.stub(services, 'batchDeleteObjectMetadata')
                    .callsFake((mpuBucketName, keysToDelete, log, cb) =>
                        // Simulate a non-retryable error that should be converted to retryable
                         cb(errors.NoSuchKey)
                    );

                const completeBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>1</PartNumber>' +
                    `<ETag>"${partHash}"</ETag>` +
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
                    actionImplicitDenies: false,
                };

                completeMultipartUpload(authInfo, completeRequest, log, err => {
                    // Restore original function
                    batchDeleteStub.restore();

                    // Should get an error (retryable behavior)
                    assert(err, 'Expected an error when metadata deletion fails');

                    // Verify S3 object was created successfully despite the error
                    const objMD = metadata.keyMaps.get(bucketName).get(objectKey);
                    assert(objMD, 'S3 object should exist even when metadata cleanup fails');
                    assert.strictEqual(objMD.uploadId, testUploadId);

                    done();
                });
            });
        });
    });

    it('should not return error if batchDeleteExtraParts fails', done => {
        const fullSizedPart = crypto.randomBytes(5 * 1024 * 1024);
        const partBody = Buffer.from('I am a smaller part\n', 'utf8');
        let batchDeleteStub;

        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            assert.ifError(err);
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];

            // Upload part 1 (will be included in completion)
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
            }, fullSizedPart);

            objectPutPart(authInfo, partRequest1, undefined, log, (err, part1ETag) => {
                assert.ifError(err);

                // Upload part 2 (will be an "extra part" not included in completion)
                const partRequest2 = new DummyRequest({
                    bucketName,
                    namespace,
                    objectKey,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    url: `/${objectKey}?partNumber=2&uploadId=${testUploadId}`,
                    query: {
                        partNumber: '2',
                        uploadId: testUploadId,
                    },
                }, partBody);

                objectPutPart(authInfo, partRequest2, undefined, log, err => {
                    assert.ifError(err);

                    // Mock data.batchDelete to fail when deleting extra parts
                    const { data } = require('../../../lib/data/wrapper');
                    batchDeleteStub = sinon.stub(data, 'batchDelete')
                        .callsFake((locations, method, dataStoreName, log, cb) =>
                            // Always fail extra part deletion
                             cb(new Error('Simulated extra part deletion failure'))
                        );

                    // Complete MPU with only part 1 (part 2 becomes "extra part")
                    const completeBody = '<CompleteMultipartUpload>' +
                        '<Part>' +
                        '<PartNumber>1</PartNumber>' +
                        `<ETag>"${part1ETag}"</ETag>` +
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
                        actionImplicitDenies: false,
                    };

                    completeMultipartUpload(authInfo, completeRequest, log, err => {
                        // Restore original function
                        batchDeleteStub.restore();

                        // Should NOT get an error despite extra part deletion failing
                        assert.ifError(err, 'Should not return error when extra part deletion fails');

                        // Verify S3 object was created successfully
                        const objMD = metadata.keyMaps.get(bucketName).get(objectKey);
                        assert(objMD, 'S3 object should exist');
                        assert.strictEqual(objMD.uploadId, testUploadId);

                        // Verify MPU metadata was cleaned up
                        assert.strictEqual(metadata.keyMaps.get(mpuBucket).size, 0,
                            'MPU metadata should be cleaned up');

                        done();
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
    let testPutObjectRequests;

    beforeEach(done => {
        cleanup();
        testPutObjectRequests = objData
              .slice(0, 2)
              .map(data => versioningTestUtils.createPutObjectRequest(
                  bucketName, objectKey, data));
        bucketPut(authInfo, bucketPutRequest, log, done);
    });

    after(done => {
        cleanup();
        done();
    });

    it('should delete null version when creating new null version, ' +
    'when null version is the latest version', done => {
        async.waterfall([
            next => bucketPutVersioning(authInfo,
                suspendVersioningRequest, log, err => next(err)),
            next => initiateMultipartUpload(
                authInfo, initiateRequest, log, next),
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
                const origPutObject = metadataBackend.putObject;
                let callCount = 0;
                metadataBackend.putObject =
                    (putBucketName, objName, objVal, params, log, cb) => {
                        if (callCount === 0) {
                            // first putObject sets the completeInProgress flag in the overview key
                            assert.strictEqual(putBucketName, `${constants.mpuBucketPrefix}${bucketName}`);
                            assert.strictEqual(
                                objName, `overview${splitter}${objectKey}${splitter}${testUploadId}`);
                            assert.strictEqual(objVal.completeInProgress, true);
                        } else {
                            assert.strictEqual(params.replayId, testUploadId);
                            assert.strictEqual(objVal.originOp, 's3:ObjectCreated:CompleteMultipartUpload');
                            metadataBackend.putObject = origPutObject;
                        }
                        origPutObject(
                            putBucketName, objName, objVal, params, log, cb);
                        callCount += 1;
                    };
                const parts = [{ partNumber: 1, eTag }];
                const completeRequest = _createCompleteMpuRequest(testUploadId,
                    parts);
                completeMultipartUpload(authInfo, completeRequest, log,
                                        err => next(err, testUploadId));
            },
            (testUploadId, next) => {
                const origPutObject = metadataBackend.putObject;
                metadataBackend.putObject =
                    (putBucketName, objName, objVal, params, log, cb) => {
                        assert.strictEqual(params.oldReplayId, testUploadId);
                        assert.strictEqual(objVal.originOp, 's3:ObjectCreated:Put');
                        metadataBackend.putObject = origPutObject;
                        origPutObject(
                            putBucketName, objName, objVal, params, log, cb);
                    };
                // overwrite null version with a non-MPU object
                objectPut(authInfo, testPutObjectRequests[1],
                          undefined, log, err => next(err));
            },
        ], err => {
            assert.ifError(err, `Unexpected err: ${err}`);
            done();
        });
    });

    it('should delete null version when creating new null version, ' +
    'when null version is not the latest version', done => {
        async.waterfall([
            // putting null version: put obj before versioning configured
            next => objectPut(authInfo, testPutObjectRequests[0],
                undefined, log, err => next(err)),
            next => bucketPutVersioning(authInfo,
                enableVersioningRequest, log, err => next(err)),
            // put another version:
            next => objectPut(authInfo, testPutObjectRequests[1],
                undefined, log, err => next(err)),
            next => bucketPutVersioning(authInfo,
                suspendVersioningRequest, log, err => next(err)),
            next => {
                versioningTestUtils.assertDataStoreValues(
                    ds, objData.slice(0, 2));
                initiateMultipartUpload(authInfo, initiateRequest, log, next);
            },
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
                const origPutObject = metadataBackend.putObject;
                let callCount = 0;
                metadataBackend.putObject =
                    (putBucketName, objName, objVal, params, log, cb) => {
                        if (callCount === 0) {
                            // first putObject sets the completeInProgress flag in the overview key
                            assert.strictEqual(putBucketName, `${constants.mpuBucketPrefix}${bucketName}`);
                            assert.strictEqual(
                                objName, `overview${splitter}${objectKey}${splitter}${testUploadId}`);
                            assert.strictEqual(objVal.completeInProgress, true);
                        } else {
                            assert.strictEqual(params.replayId, testUploadId);
                            metadataBackend.putObject = origPutObject;
                        }
                        origPutObject(
                            putBucketName, objName, objVal, params, log, cb);
                        callCount += 1;
                    };
                const parts = [{ partNumber: 1, eTag }];
                const completeRequest = _createCompleteMpuRequest(testUploadId,
                    parts);
                completeMultipartUpload(authInfo, completeRequest, log,
                                        err => next(err, testUploadId));
            },
            (testUploadId, next) => {
                versioningTestUtils.assertDataStoreValues(
                    ds, [undefined, objData[1], objData[2]]);

                const origPutObject = metadataBackend.putObject;
                metadataBackend.putObject =
                    (putBucketName, objName, objVal, params, log, cb) => {
                        assert.strictEqual(params.oldReplayId, testUploadId);
                        metadataBackend.putObject = origPutObject;
                        origPutObject(
                            putBucketName, objName, objVal, params, log, cb);
                    };
                // overwrite null version with a non-MPU object
                objectPut(authInfo, testPutObjectRequests[1],
                          undefined, log, err => next(err));
            },
        ], err => {
            assert.ifError(err, `Unexpected err: ${err}`);
            done();
        });
    });

    it('should finish deleting metadata on completeMultipartUpload retry',
    done => {
        let origDeleteObject;
        async.waterfall([
            next => bucketPutVersioning(authInfo,
                enableVersioningRequest, log, err => next(err)),
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
                origDeleteObject = metadataBackend.deleteObject;
                metadataBackend.deleteObject = (
                    bucketName, objName, params, log, cb) => {
                        // prevent deletions from MPU bucket only
                    if (bucketName === mpuBucket) {
                        return process.nextTick(
                            () => cb(errors.InternalError));
                    }
                    return origDeleteObject(
                        bucketName, objName, params, log, cb);
                };
                const parts = [{ partNumber: 1, eTag }];
                const completeRequest = _createCompleteMpuRequest(
                    testUploadId, parts);
                completeMultipartUpload(authInfo, completeRequest, log, err => {
                    // expect a failure here because we could not
                    // remove the overview key
                    assert.strictEqual(err.is.InternalError, true);
                    next(null, eTag, testUploadId);
                });
            },
            (eTag, testUploadId, next) => {
                // allow MPU bucket metadata deletions to happen again
                metadataBackend.deleteObject = origDeleteObject;
                // retry the completeMultipartUpload with the same
                // metadata, as an application would normally do after
                // a failure
                const parts = [{ partNumber: 1, eTag }];
                const completeRequest = _createCompleteMpuRequest(
                    testUploadId, parts);
                completeMultipartUpload(authInfo, completeRequest, log, next);
            },
        ], err => {
            assert.ifError(err);
            let nbVersions = 0;
            for (const key of metadata.keyMaps.get(bucketName).keys()) {
                if (key !== objectKey && key.startsWith(objectKey)) {
                    nbVersions += 1;
                }
            }
            // There should be only one version of the object, since
            // the second call should not have created a new version
            assert.strictEqual(nbVersions, 1);
            for (const key of metadata.keyMaps.get(mpuBucket).keys()) {
                assert.fail('There should be no more keys in MPU bucket, ' +
                            `found "${key}"`);
            }
            done();
        });
    });

    it('should complete an MPU and promote its MD if it has been created by a failed complete before' +
        'without creating a new version', done => {
        const delMeta = metadataBackend.deleteObject;
        metadataBackend.deleteObject = (bucketName, objName, params, log, cb) => cb(errors.InternalError);
        const partBody = Buffer.from('I am a part\n', 'utf8');
        initiateRequest.headers['x-amz-meta-stuff'] =
            'I am some user metadata';
        async.waterfall([
            next => bucketPutVersioning(authInfo,
                enableVersioningRequest, log, err => next(err)),
            next => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            assert.ifError(err);
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const partHash = crypto.createHash('md5').update(partBody).digest('hex');
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
                partHash,
            }, partBody);
            objectPutPart(authInfo, partRequest, undefined, log, () => {
                const completeBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>1</PartNumber>' +
                    `<ETag>"${partHash}"</ETag>` +
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
                    actionImplicitDenies: false,
                };
                completeMultipartUpload(authInfo,
                    completeRequest, log, err => {
                        assert(err.is.InternalError);
                        const MD = metadata.keyMaps.get(bucketName)
                                                    .get(objectKey);
                        assert(MD);
                        const firstVersionId = MD.versionId;
                        assert.strictEqual(MD['x-amz-meta-stuff'],
                            'I am some user metadata');
                        assert.strictEqual(MD.uploadId, testUploadId);
                        metadataBackend.deleteObject = delMeta;
                        assert.strictEqual(metadata.keyMaps.get(mpuBucket).size, 2);
                        completeMultipartUpload(authInfo,
                            completeRequest, log, err => {
                                assert.ifError(err);
                                const MD = metadata.keyMaps.get(bucketName)
                                                    .get(objectKey);
                                assert(MD);
                                assert.strictEqual(MD.versionId, firstVersionId);
                                assert.strictEqual(MD['x-amz-meta-stuff'],
                                    'I am some user metadata');
                                assert.strictEqual(metadata.keyMaps.get(mpuBucket).size, 0);
                                done();
                            });
                    });
            });
        });
    });
});

describe('multipart upload with object lock', () => {
    before(done => {
        cleanup();
        bucketPut(authInfo, lockEnabledBucketRequest, log, done);
    });

    after(cleanup);

    it('mpu object should contain retention info when mpu initiated with ' +
    'object retention', done => {
        let versionId;
        async.waterfall([
            next => initiateMultipartUpload(authInfo, retentionInitiateRequest,
                log, next),
            (result, corsHeaders, next) => parseString(result, next),
            (json, next) => {
                const partBody = Buffer.from('foobar', 'utf8');
                const testUploadId =
                    json.InitiateMultipartUploadResult.UploadId[0];
                const partRequest = _createPutPartRequest(testUploadId, 1,
                    partBody);
                partRequest.bucketName = lockedBucket;
                partRequest.headers = { host: `${lockedBucket}.s3.amazonaws.com` };
                objectPutPart(authInfo, partRequest, undefined, log,
                    (err, eTag) => next(err, eTag, testUploadId));
            },
            (eTag, testUploadId, next) => {
                const parts = [{ partNumber: 1, eTag }];
                const completeRequest = _createCompleteMpuRequest(testUploadId,
                    parts);
                completeRequest.bucketName = lockedBucket;
                completeRequest.headers = { host: `${lockedBucket}.s3.amazonaws.com` };
                completeMultipartUpload(authInfo, completeRequest, log, next);
            },
            (xml, headers, next) => {
                versionId = headers['x-amz-version-id'];
                getObjectRetention(authInfo, getObjectLockInfoRequest, log, next);
            },
            (result, corsHeaders, next) => parseString(result, next),
        ], (err, json) => {
            assert.ifError(err);
            assert.deepStrictEqual(json.Retention, expectedRetentionConfig);
            changeObjectLock(
                [{ bucket: lockedBucket, key: objectKey, versionId }], '', done);
        });
    });

    it('mpu object should contain legal hold info when mpu initiated with ' +
    'legal hold', done => {
        let versionId;
        async.waterfall([
            next => initiateMultipartUpload(authInfo, legalHoldInitiateRequest,
                log, next),
            (result, corsHeaders, next) => parseString(result, next),
            (json, next) => {
                const partBody = Buffer.from('foobar', 'utf8');
                const testUploadId =
                    json.InitiateMultipartUploadResult.UploadId[0];
                const partRequest = _createPutPartRequest(testUploadId, 1,
                    partBody);
                partRequest.bucketName = lockedBucket;
                partRequest.headers = { host: `${lockedBucket}.s3.amazonaws.com` };
                objectPutPart(authInfo, partRequest, undefined, log,
                    (err, eTag) => next(err, eTag, testUploadId));
            },
            (eTag, testUploadId, next) => {
                const parts = [{ partNumber: 1, eTag }];
                const completeRequest = _createCompleteMpuRequest(testUploadId,
                    parts);
                completeRequest.bucketName = lockedBucket;
                completeRequest.headers = { host: `${lockedBucket}.s3.amazonaws.com` };
                completeMultipartUpload(authInfo, completeRequest, log, next);
            },
            (xml, headers, next) => {
                versionId = headers['x-amz-version-id'];
                getObjectLegalHold(authInfo, getObjectLockInfoRequest, log, next);
            },
            (result, corsHeaders, next) => parseString(result, next),
        ], (err, json) => {
            assert.ifError(err);
            assert.deepStrictEqual(json.LegalHold, expectedLegalHold);
            changeObjectLock(
                [{ bucket: lockedBucket, key: objectKey, versionId }], '', done);
        });
    });
});

describe('multipart upload overheadField', () => {
    const any = sinon.match.any;

    beforeEach(() => {
        cleanup();
        sinon.spy(metadataswitch, 'putObjectMD');
    });

    after(() => {
        metadataswitch.putObjectMD.restore();
        cleanup();
    });

    it('should pass overheadField', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                assert.strictEqual(mpuKeys.size, 1);
                assert(mpuKeys.keys().next().value
                    .startsWith(`overview${splitter}${objectKey}`));
                parseString(result, next);
            },
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            assert.ifError(err);
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            md5Hash.update(bufferBody);
            const partHash = md5Hash.digest('hex');
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
                partHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, err => {
                assert.ifError(err);
                sinon.assert.calledWith(metadataswitch.putObjectMD.lastCall,
                    any, any, any, sinon.match({ overheadField: sinon.match.array }), any, any);
                done();
            });
        });
    });
});

describe('complete mpu with bucket policy', () => {
    function getPolicyRequest(policy) {
        return {
            socket: {
                remoteAddress: '1.1.1.1',
            },
            bucketName,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            post: JSON.stringify(policy),
            actionImplicitDenies: false,
        };
    }
    /** Additional fields are required on existing request mocks */
    const requestFix = {
        connection: { encrypted: false },
        destroy: () => {},
    };
    const initiateReqFixed = Object.assign({}, initiateRequest, requestFix);
    const partBody = Buffer.from('I am a part\n', 'utf8');
    const md5Hash = crypto.createHash('md5').update(partBody);
    const partHash = md5Hash.digest('hex');
    const completeBody = '<CompleteMultipartUpload>' +
    '<Part>' +
    '<PartNumber>1</PartNumber>' +
    `<ETag>"${partHash}"</ETag>` +
    '</Part>' +
    '</CompleteMultipartUpload>';

    beforeEach(done => {
        cleanup();
        sinon.spy(metadataswitch, 'putObjectMD');
        bucketPut(authInfo, bucketPutRequest, log, done);
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should complete with a deny on unrelated object as non root', done => {
        const bucketPutPolicyRequest = getPolicyRequest({
            Version: '2012-10-17',
            Statement: [
                {
                    Effect: 'Deny',
                    Principal: '*',
                    Action: ['s3:PutObject'],
                    Resource: `arn:aws:s3:::${bucketName}/unrelated_obj`,
                },
            ],
        });
        /** root user doesn't check bucket policy */
        const authNotRoot = makeAuthInfo(canonicalID, 'not-root');

        async.waterfall([
            next => bucketPutPolicy(authInfo,
                bucketPutPolicyRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authNotRoot,
                initiateReqFixed, log, next),
            (result, corsHeaders, next) => parseString(result, next),
            (json, next) => {
                const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
                const partRequest = new DummyRequest(Object.assign({
                    socket: {
                        remoteAddress: '1.1.1.1',
                    },
                    bucketName,
                    namespace,
                    objectKey,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
                    query: {
                        partNumber: '1',
                        uploadId: testUploadId,
                    },
                    partHash,
                }, requestFix), partBody);
                objectPutPart(authNotRoot, partRequest,
                    undefined, log, err => next(err, testUploadId));
            },
            (testUploadId, next) => {
                const completeRequest = new DummyRequest(Object.assign({
                    socket: {
                        remoteAddress: '1.1.1.1',
                    },
                    bucketName,
                    namespace,
                    objectKey,
                    parsedHost: 's3.amazonaws.com',
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: { uploadId: testUploadId },
                    post: completeBody,
                    actionImplicitDenies: false,
                }, requestFix));
                completeMultipartUpload(authNotRoot, completeRequest,
                    log, next);
            },
        ],
        err => {
            assert.ifError(err);
            done();
        });
    });

    it('should set bucketOwnerId if requester is not destination bucket owner', done => {
        const partBody = Buffer.from('I am a part\n', 'utf8');
        const bucketPutPolicyRequest = getPolicyRequest({
            Version: '2012-10-17',
            Statement: [
                {
                    Effect: 'Allow',
                    Principal: { AWS: `arn:aws:iam::${authInfoOtherAcc.shortid}:root` },
                    Action: ['s3:*'],
                    Resource: `arn:aws:s3:::${bucketName}/*`,
                },
            ],
        });
        async.waterfall([
            next => bucketPutPolicy(authInfo, bucketPutPolicyRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfoOtherAcc,
                initiateRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            assert.ifError(err);
            const testUploadId =
                json.InitiateMultipartUploadResult.UploadId[0];
            const partHash = crypto.createHash('md5').update(partBody).digest('hex');
            const partRequest = new DummyRequest(Object.assign({
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
                // What is being tested is that the partHash being sent
                // to the API for the part is stored and then used to
                // calculate the final ETag upon completion
                // of the multipart upload.
                partHash,
                socket: {
                    remoteAddress: '1.1.1.1',
                },
            }, requestFix), partBody);
            objectPutPart(authInfoOtherAcc, partRequest, undefined, log, err => {
                assert.ifError(err);
                const completeBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>1</PartNumber>' +
                    `<ETag>"${partHash}"</ETag>` +
                    '</Part>' +
                    '</CompleteMultipartUpload>';
                const completeRequest = new DummyRequest(Object.assign({
                    bucketName,
                    namespace,
                    objectKey,
                    parsedHost: 's3.amazonaws.com',
                    url: `/${objectKey}?uploadId=${testUploadId}`,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    query: { uploadId: testUploadId },
                    post: completeBody,
                    actionImplicitDenies: false,
                    socket: {
                        remoteAddress: '1.1.1.1',
                    },
                }, requestFix));
                completeMultipartUpload(authInfoOtherAcc,
                    completeRequest, log, err => {
                        assert.ifError(err);
                        sinon.assert.calledWith(
                            metadataswitch.putObjectMD.lastCall,
                            bucketName,
                            objectKey,
                            sinon.match({ bucketOwnerId: authInfo.canonicalId }),
                            sinon.match.any,
                            sinon.match.any,
                            sinon.match.any
                        );
                        done();
                    });
            });
        });
    });
});

describe('multipart upload in ingestion bucket', () => {
    const dataClient = data.client;
    const prevDataImplName = data.implName;
    const prevConfigBackendsData = data.config.backends.data;

    let versionID;

    before(() => {
        versionID = versioning.VersionID.encode(versioning.VersionID.generateVersionId('0', ''));

        // Setup multi-backend, this is required for ingestion
        data.switch(new storage.data.MultipleBackendGateway({
            'us-east-1': dataClient,
            'us-east-2': dataClient,
        }, metadata, data.locStorageCheckFn));
        data.implName = 'multipleBackends';

        // "mock" the data location, simulating a backend supporting MPU
        data.config.backends.data = 'multiple';
        dataClient.clientType = 'aws_s3';
    });

    after(() => {
        data.switch(dataClient);
        data.implName = prevDataImplName;
        data.config.backends.data = prevConfigBackendsData;
        delete dataClient.clientType;
    });

    beforeEach(() => {
        cleanup();

        // "mock" the data location, simulating a backend supporting MPU
        dataClient.createMPU = sinon.stub().yields(undefined, {
            uploadId: 'mock-uploadId',
        });
        dataClient.uploadPart = sinon.stub().yields(undefined, {
            dataStoreType: dataClient.clientType,
            dataStoreETag: 'mock-part-eTag',
        });
        dataClient.completeMPU = sinon.stub().yields(undefined, {
            key: objectKey,
            eTag: 'mock-eTag',
            dataStoreVersionId: versionID,
            contentLength: 12,
        });
    });

    afterEach(() => {
        sinon.restore();
    });

    const newPutIngestBucketRequest = location => new DummyRequest({
        bucketName,
        namespace,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
        post: '<?xml version="1.0" encoding="UTF-8"?>' +
            '<CreateBucketConfiguration ' +
            'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
            `<LocationConstraint>${location}</LocationConstraint>` +
            '</CreateBucketConfiguration>',
    });
    const archiveRestoreRequested = {
        archiveInfo: { foo: 0, bar: 'stuff' }, // opaque, can be anything...
        restoreRequestedAt: new Date().toString(),
        restoreRequestedDays: 5,
    };

    it('should use the versionID from the backend', async () => {
        await _bucketPut(authInfo, newPutIngestBucketRequest('us-east-1:ingest'), log);

        const headers = await _uploadMpuObject();
        assert.strictEqual(headers['x-amz-version-id'], versionID);
    });

    it('should not use the versionID from the backend when writing in another location', async () => {
        await _bucketPut(authInfo, newPutIngestBucketRequest('us-east-1:ingest'), log);

        const headers = await _uploadMpuObject({ location: 'us-east-2' });
        assert.notEqual(headers['x-amz-version-id'], versionID);
    });

    it('should not use the versionID from the backend when it is not a valid versionID', async () => {
        dataClient.completeMPU.onCall(0).yields(undefined, {
            key: objectKey,
            eTag: 'mock-eTag',
            dataStoreVersionId: undefined,
            contentLength: 12,
        });

        await _bucketPut(authInfo, newPutIngestBucketRequest('us-east-1:ingest'), log);

        const headers = await _uploadMpuObject();
        assert.notEqual(headers['x-amz-version-id'], versionID);
    });

    it('should add versionID to backend putObject when restoring object', async () => {
        const restoredVersionID = versioning.VersionID.encode(versioning.VersionID.generateVersionId('0', ''));
        dataClient.completeMPU.onCall(0).callThrough();
        dataClient.completeMPU.onCall(1).yields(undefined, {
            key: objectKey,
            eTag: 'mock-eTag',
            dataStoreVersionId: restoredVersionID,
            contentLength: 12,
        });

        await _bucketPut(authInfo, newPutIngestBucketRequest('us-east-1:ingest'), log);

        let headers = await _uploadMpuObject();
        assert.strictEqual(headers['x-amz-version-id'], versionID);
        assert.strictEqual(dataClient.createMPU.firstCall.args[1]['x-amz-meta-scal-version-id'], undefined);

        await util.promisify(fakeMetadataArchive)(bucketName, objectKey, versionID, archiveRestoreRequested);

        headers = await _uploadMpuObject({
            versionID,
        });
        assert.strictEqual(headers['x-amz-version-id'], versionID);
        assert.strictEqual(dataClient.createMPU.lastCall.args[1]['x-amz-meta-scal-version-id'], versionID);
    });

    it('should not add versionID to backend putObject when restoring object to another location', async () => {
        const restoredVersionID = versioning.VersionID.encode(versioning.VersionID.generateVersionId('0', ''));
        dataClient.completeMPU.onCall(0).callThrough();
        dataClient.completeMPU.onCall(1).yields(undefined, {
            key: objectKey,
            eTag: 'mock-eTag',
            dataStoreVersionId: restoredVersionID,
            contentLength: 12,
        });

        await _bucketPut(authInfo, newPutIngestBucketRequest('us-east-1:ingest'), log);

        let headers = await _uploadMpuObject();
        assert.strictEqual(headers['x-amz-version-id'], versionID);
        assert.strictEqual(dataClient.createMPU.firstCall.args[1]['x-amz-meta-scal-version-id'], undefined);

        await util.promisify(fakeMetadataArchive)(bucketName, objectKey, versionID, archiveRestoreRequested);

        headers = await _uploadMpuObject({
            versionID,
            location: 'us-east-2',
        });
        assert.strictEqual(headers['x-amz-version-id'], versionID);
        assert.strictEqual(dataClient.createMPU.lastCall.args[1]['x-amz-meta-scal-version-id'], undefined);
    });
});

describe('initiateMultipartUpload with objectKeyByteLimit', () => {
    const originalObjectKeyByteLimit = config.objectKeyByteLimit;

    beforeEach(() => {
        cleanup();
    });

    afterEach(() => {
        config.objectKeyByteLimit = originalObjectKeyByteLimit;
    });

    const createTestInitiateRequest = longKey => new DummyRequest({
        bucketName,
        namespace,
        objectKey: longKey,
        headers: {},
        url: `/${bucketName}/${longKey}?uploads`,
        query: { uploads: '' },
    });

    it('should reject object key longer than 915 bytes by default', done => {
        const longKey = 'a'.repeat(916);
        const testInitiateRequest = createTestInitiateRequest(longKey);

        bucketPut(authInfo, bucketPutRequest, log, err => {
            assert.ifError(err);
            initiateMultipartUpload(authInfo, testInitiateRequest, log, err => {
                assert(err);
                assert.strictEqual(err.KeyTooLong, true);
                assert.match(err.description, /915/);
                done();
            });
        });
    });

    it('should accept object key longer than 915 bytes with objectKeyByteLimit', done => {
        config.objectKeyByteLimit = 1024;

        const longKey = 'a'.repeat(1024);
        const testInitiateRequest = createTestInitiateRequest(longKey);

        bucketPut(authInfo, bucketPutRequest, log, err => {
            assert.ifError(err);
            initiateMultipartUpload(authInfo, testInitiateRequest, log, (err, xml) => {
                assert.ifError(err);
                assert(xml);
                done();
            });
        });
    });

    it('should reject object key exceeding objectKeyByteLimit', done => {
        config.objectKeyByteLimit = 1024;

        const longKey = 'a'.repeat(1025);
        const testInitiateRequest = createTestInitiateRequest(longKey);

        bucketPut(authInfo, bucketPutRequest, log, err => {
            assert.ifError(err);
            initiateMultipartUpload(authInfo, testInitiateRequest, log, err => {
                assert(err);
                assert.strictEqual(err.KeyTooLong, true);
                assert.match(err.description, /1024/);
                done();
            });
        });
    });
});

describe('objectPutPart checksum response headers', () => {
    let testUploadId;

    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, bucketPutRequest, log, err => {
            assert.ifError(err);
            initiateMultipartUpload(authInfo, initiateRequest, log, (err, result) => {
                assert.ifError(err);
                parseString(result, (err, json) => {
                    assert.ifError(err);
                    testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
                    done();
                });
            });
        });
    });

    afterEach(() => cleanup());

    it('should return x-amz-checksum-sha256 response header when x-amz-checksum-sha256 is provided', done => {
        const sha256Value = crypto.createHash('sha256').update(postBody).digest('base64');
        const partRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey,
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-checksum-sha256': sha256Value,
            },
            url: `/${objectKey}?partNumber=1&uploadId=${testUploadId}`,
            query: { partNumber: '1', uploadId: testUploadId },
            actionImplicitDenies: false,
        }, postBody);

        objectPutPart(authInfo, partRequest, undefined, log, (err, _hexDigest, resHeaders) => {
            assert.ifError(err);
            assert.strictEqual(resHeaders['x-amz-checksum-sha256'], sha256Value);
            done();
        });
    });

    it('should return x-amz-checksum-crc64nvme response header when no checksum header is provided', done => {
        const expectedCrc64nvme = '5evlCr2wyO4=';
        const partRequest = _createPutPartRequest(testUploadId, '1', postBody);

        objectPutPart(authInfo, partRequest, undefined, log, (err, _hexDigest, resHeaders) => {
            assert.ifError(err);
            assert.strictEqual(resHeaders['x-amz-checksum-crc64nvme'], expectedCrc64nvme);
            done();
        });
    });
});
