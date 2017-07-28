const assert = require('assert');
const async = require('async');
const AWS = require('aws-sdk');
const { parseString } = require('xml2js');
const { errors } = require('arsenal');

const { getRealAwsConfig } =
    require('../functional/aws-node-sdk/test/support/awsConfig');
const { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils } =
    require('../unit/helpers');
const DummyRequest = require('../unit/DummyRequest');
const { config } = require('../../lib/Config');
const metadata = require('../../lib/metadata/in_memory/metadata').metadata;

const { bucketPut } = require('../../lib/api/bucketPut');
const bucketGet = require('../../lib/api/bucketGet');
const objectPut = require('../../lib/api/objectPut');
const objectGet = require('../../lib/api/objectGet');
const bucketPutVersioning = require('../../lib/api/bucketPutVersioning');
const initiateMultipartUpload =
    require('../../lib/api/initiateMultipartUpload');
const multipartDelete = require('../../lib/api/multipartDelete');
const objectPutPart = require('../../lib/api/objectPutPart');
const completeMultipartUpload =
    require('../../lib/api/completeMultipartUpload');
const listParts = require('../../lib/api/listParts');
const listMultipartUploads = require('../../lib/api/listMultipartUploads');

const awsLocation = 'aws-test';
const awsConfig = getRealAwsConfig(awsLocation);
const s3 = new AWS.S3(awsConfig);
const log = new DummyRequestLogger();

const fakeUploadId = 'fakeuploadid';
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const awsBucket = config.locationConstraints[awsLocation].details.bucketName;
const smallBody = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);
const locMetaHeader = 'x-amz-meta-scal-location-constraint';
const bucketPutRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    post: '',
    parsedHost: 'localhost',
};
const objectKey = 'testObject';
const fileKey = 'fileKey';
const memKey = 'memKey';
const objectPutParams = {
    bucketName,
    namespace,
    objectKey,
    url: '/',
};
const objectGetRequest = {
    bucketName,
    namespace,
    objectKey,
    headers: {},
    url: `/${bucketName}/${objectKey}`,
};
const bucketGetRequest = {
    bucketName,
    namespace,
    headers: { host: '/' },
    url: `/${bucketName}`,
    query: { versions: '', prefix: objectKey },
};
const awsETag = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const awsETagBigObj = 'f1c9645dbc14efddc7d8a322685f26eb';
const deleteParams = {
    bucketName,
    namespace,
    objectKey,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
};
const partParams = {
    bucketName,
    namespace,
    objectKey,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
};
const completeBody = '<CompleteMultipartUpload>' +
    '<Part>' +
    '<PartNumber>1</PartNumber>' +
    `<ETag>"${awsETagBigObj}"</ETag>` +
    '</Part>' +
    '<Part>' +
    '<PartNumber>2</PartNumber>' +
    `<ETag>"${awsETag}"</ETag>` +
    '</Part>' +
    '</CompleteMultipartUpload>';
const completeParams = {
    bucketName,
    namespace,
    objectKey,
    parsedHost: 's3.amazonaws.com',
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    post: completeBody,
};
const listRequest = {
    bucketName,
    namespace,
    objectKey,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
};

const awsParams = { Bucket: awsBucket, Key: objectKey };

function assertMpuInitResults(initResult, key, cb) {
    parseString(initResult, (err, json) => {
        assert.equal(err, null, `Error parsing mpu init results: ${err}`);
        assert.strictEqual(json.InitiateMultipartUploadResult
            .Bucket[0], bucketName);
        assert.strictEqual(json.InitiateMultipartUploadResult
            .Key[0], key);
        assert(json.InitiateMultipartUploadResult.UploadId[0]);
        cb(json.InitiateMultipartUploadResult.UploadId[0]);
    });
}

function assertMpuCompleteResults(compResult) {
    parseString(compResult, (err, json) => {
        assert.equal(err, null,
            `Error parsing mpu complete results: ${err}`);
        assert.strictEqual(
            json.CompleteMultipartUploadResult.Location[0],
            `http://${bucketName}.s3.amazonaws.com/${objectKey}`);
        assert.strictEqual(
            json.CompleteMultipartUploadResult.Bucket[0],
            bucketName);
        assert.strictEqual(
            json.CompleteMultipartUploadResult.Key[0], objectKey);
        const MD = metadata.keyMaps.get(bucketName).get(objectKey);
        assert(MD);
    });
}

function assertListResults(listResult, testAttribute, uploadId) {
    parseString(listResult, (err, json) => {
        assert.equal(err, null, `Error parsing list part results: ${err}`);
        assert.strictEqual(json.ListPartsResult.Key[0], objectKey);
        assert.strictEqual(json.ListPartsResult.UploadId[0], uploadId);
        assert.strictEqual(json.ListPartsResult.Initiator[0].ID[0],
            authInfo.getCanonicalID());

        // attributes to test specific to PartNumberMarker being set
        // in listParts
        if (testAttribute === 'partNumMarker') {
            assert.strictEqual(json.ListPartsResult.NextPartNumberMarker,
                undefined);
            assert.strictEqual(json.ListPartsResult.IsTruncated[0], 'false');
            assert.strictEqual(json.ListPartsResult.Part.length, 1);
            assert.strictEqual(json.ListPartsResult.PartNumberMarker[0], '1');
            // data of second part put
            assert.strictEqual(json.ListPartsResult.Part[0].PartNumber[0], '2');
            assert.strictEqual(json.ListPartsResult.Part[0].ETag[0],
                `"${awsETag}"`);
            assert.strictEqual(json.ListPartsResult.Part[0].Size[0], '11');
        } else {
            // common attributes to test if MaxParts set or
            // neither MaxParts nor PartNumberMarker set
            assert.strictEqual(json.ListPartsResult.PartNumberMarker,
                undefined);
            assert.strictEqual(json.ListPartsResult.Part[0].PartNumber[0], '1');
            assert.strictEqual(json.ListPartsResult.Part[0].ETag[0],
                `"${awsETagBigObj}"`);
            assert.strictEqual(json.ListPartsResult.Part[0].Size[0],
                '10485760');

            // attributes to test specific to MaxParts being set in listParts
            if (testAttribute === 'maxParts') {
                assert.strictEqual(json.ListPartsResult.NextPartNumberMarker[0],
                    '1');
                assert.strictEqual(json.ListPartsResult.IsTruncated[0], 'true');
                assert.strictEqual(json.ListPartsResult.Part.length, 1);
                assert.strictEqual(json.ListPartsResult.MaxParts[0], '1');
            } else {
                // attributes to test if neither MaxParts nor
                // PartNumberMarker set
                assert.strictEqual(json.ListPartsResult.NextPartNumberMarker,
                    undefined);
                assert.strictEqual(json.ListPartsResult.IsTruncated[0],
                    'false');
                assert.strictEqual(json.ListPartsResult.Part.length, 2);
                assert.strictEqual(json.ListPartsResult.MaxParts[0], '1000');
                assert.strictEqual(json.ListPartsResult.Part[1].PartNumber[0],
                    '2');
                assert.strictEqual(json.ListPartsResult.Part[1].ETag[0],
                    `"${awsETag}"`);
                assert.strictEqual(json.ListPartsResult.Part[1].Size[0], '11');
            }
        }
    });
}

function assertObjOnBackend(expectedBackend, cb) {
    return objectGet(authInfo, objectGetRequest, false, log,
    (err, result, metaHeaders) => {
        assert.equal(err, null, `Error getting object on S3: ${err}`);
        assert.strictEqual(metaHeaders[locMetaHeader], expectedBackend);
        if (expectedBackend === awsLocation) {
            return s3.headObject({ Bucket: awsBucket, Key: objectKey },
            (err, result) => {
                assert.equal(err, null, 'Error on headObject call to AWS: ' +
                    `${err}`);
                assert.strictEqual(result.Metadata[locMetaHeader], awsLocation);
                return cb();
            });
        }
        return cb();
    });
}

function assertVersioning(done) {
    bucketGet(authInfo, bucketGetRequest, log,
    (err, res) => {
        assert.equal(err, null, `Error getting versioning: ${err}`);
        parseString(res, (err, json) => {
            assert.equal(err, null, 'Error parsing get version results ' +
            `${err}`);
            assert.strictEqual(json.ListVersionsResult.Version.length, 2);
            done();
        });
    });
}

function putParts(uploadId, key, cb) {
    const putPartParam1 = Object.assign({
        url: `/${key}?partNumber=1&uploadId=${uploadId}`,
        query: { partNumber: '1', uploadId } }, partParams);
    putPartParam1.objectKey = key;
    const partRequest = new DummyRequest(putPartParam1, bigBody);
    objectPutPart(authInfo, partRequest, undefined, log, err => {
        assert.equal(err, null, `Error putting part: ${err}`);
        const putPartParam2 = Object.assign({
            url: `/${key}?partNumber=1&uploadId=${uploadId}`,
            query: { partNumber: '2', uploadId } }, partParams);
        putPartParam2.objectKey = key;
        const partRequest2 = new DummyRequest(putPartParam2, smallBody);
        objectPutPart(authInfo, partRequest2, undefined, log, err => {
            assert.equal(err, null, `Error putting part: ${err}`);
            cb();
        });
    });
}

function mpuSetup(location, key, cb) {
    const initiateRequest = {
        bucketName,
        namespace,
        objectKey: key,
        headers: { 'host': `${bucketName}.s3.amazonaws.com`,
            'x-amz-meta-scal-location-constraint': location },
        url: `/${key}?uploads`,
        parsedHost: 'localhost',
    };
    initiateMultipartUpload(authInfo, initiateRequest, log,
    (err, result) => {
        assert.strictEqual(err, null, 'Error initiating MPU');
        assertMpuInitResults(result, key, uploadId => {
            putParts(uploadId, key, () => {
                cb(uploadId);
            });
        });
    });
}

function putObject(putBackend, cb) {
    const putParams = Object.assign({ headers: {
        'host': `${bucketName}.s3.amazonaws.com`,
        'x-amz-meta-scal-location-constraint': putBackend } }, objectPutParams);
    const objectPutRequest = new DummyRequest(putParams, smallBody);
    objectPut(authInfo, objectPutRequest, undefined, log, err => {
        assert.equal(err, null, `Error putting object to ${putBackend} ${err}`);
        cb();
    });
}

function abortMPU(uploadId, cb) {
    const abortParams = Object.assign({ UploadId: uploadId }, awsParams);
    s3.abortMultipartUpload(abortParams, err => {
        assert.equal(err, null, `Error aborting MPU: ${err}`);
        cb();
    });
}

function abortMultipleMpus(backendsInfo, callback) {
    async.forEach(backendsInfo, (backend, cb) => {
        const uploadId = backend.uploadId;
        const delParams = Object.assign({
            url: `/${backend.key}?uploadId=${uploadId}`,
            query: { uploadId } }, deleteParams);
        delParams.objectKey = backend.key;
        multipartDelete(authInfo, delParams, log, err => {
            cb(err);
        });
    }, err => {
        assert.equal(err, null, `Error aborting MPU: ${err}`);
        callback();
    });
}

describe('Multipart Upload API with AWS Backend', function mpuTestSuite() {
    this.timeout(30000);

    beforeEach(done => {
        bucketPut(authInfo, bucketPutRequest, log, err => {
            assert.equal(err, null, `Error creating bucket: ${err}`);
            done();
        });
    });

    afterEach(() => {
        cleanup();
    });

    it('should initiate a multipart upload on real AWS', done => {
        const initiateRequest = {
            bucketName,
            namespace,
            objectKey,
            headers: { 'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-scal-location-constraint': `${awsLocation}` },
            url: `/${objectKey}?uploads`,
            parsedHost: 'localhost',
        };

        initiateMultipartUpload(authInfo, initiateRequest, log,
        (err, result) => {
            assert.strictEqual(err, null, 'Error initiating MPU');
            assertMpuInitResults(result, objectKey, uploadId => {
                abortMPU(uploadId, done);
            });
        });
    });

    it('should list the parts of a multipart upload on real AWS', done => {
        mpuSetup(awsLocation, objectKey, uploadId => {
            const listParams = Object.assign({
                url: `/${objectKey}?uploadId=${uploadId}`,
                query: { uploadId } }, listRequest);
            listParts(authInfo, listParams, log, (err, result) => {
                assert.equal(err, null, `Error listing parts on AWS: ${err}`);
                assertListResults(result, null, uploadId);
                abortMPU(uploadId, done);
            });
        });
    });

    it('should only return number of parts equal to specified maxParts',
    done => {
        mpuSetup(awsLocation, objectKey, uploadId => {
            const listParams = Object.assign({
                url: `/${objectKey}?uploadId=${uploadId}`,
                query: { uploadId, 'max-parts': '1' } }, listRequest);
            listParts(authInfo, listParams, log, (err, result) => {
                assert.equal(err, null);
                assertListResults(result, 'maxParts', uploadId);
                abortMPU(uploadId, done);
            });
        });
    });

    it('should only list parts after PartNumberMarker', done => {
        mpuSetup(awsLocation, objectKey, uploadId => {
            const listParams = Object.assign({
                url: `/${objectKey}?uploadId=${uploadId}`,
                query: { uploadId, 'part-number-marker': '1' } }, listRequest);
            listParts(authInfo, listParams, log, (err, result) => {
                assert.equal(err, null);
                assertListResults(result, 'partNumMarker', uploadId);
                abortMPU(uploadId, done);
            });
        });
    });

    it('should return an error on listParts of deleted MPU', done => {
        mpuSetup(awsLocation, objectKey, uploadId => {
            abortMPU(uploadId, () => {
                const listParams = Object.assign({
                    url: `/${objectKey}?uploadId=${uploadId}`,
                    query: { uploadId } }, listRequest);
                listParts(authInfo, listParams, log, err => {
                    assert.deepStrictEqual(err, errors.InternalError
                      .customizeDescription('Error returned from AWS: ' +
                      'The specified upload does not exist. The upload ID ' +
                      'may be invalid, or the upload may have been aborted ' +
                      'or completed.'));
                    done();
                });
            });
        });
    });

    it('should abort a multipart upload on real AWS', done => {
        mpuSetup(awsLocation, objectKey, uploadId => {
            const delParams = Object.assign({
                url: `/${objectKey}?uploadId=${uploadId}`,
                query: { uploadId } }, deleteParams);
            multipartDelete(authInfo, delParams, log, err => {
                assert.equal(err, null, `Error aborting MPU: ${err}`);
                s3.listParts({ Bucket: awsBucket, Key: objectKey,
                UploadId: uploadId }, err => {
                    assert.strictEqual(err.code, 'NoSuchUpload');
                    done();
                });
            });
        });
    });

    it('should return error on abort of MPU that does not exist', done => {
        // legacyAwsBehavior is true (otherwise, there would be no error)
        const delParams = Object.assign({
            url: `/fakekey?uploadId=${fakeUploadId}`,
            query: { fakeUploadId } }, deleteParams);
        delParams.objectKey = 'fakekey';
        multipartDelete(authInfo, delParams, log, err => {
            assert.equal(err, errors.NoSuchUpload,
                `Error aborting MPU: ${err}`);
            done();
        });
    });

    it('should return InternalError if MPU deleted directly from AWS ' +
    'and try to complete from S3', done => {
        mpuSetup(awsLocation, objectKey, uploadId => {
            abortMPU(uploadId, () => {
                const compParams = Object.assign({
                    url: `/${objectKey}?uploadId=${uploadId}`,
                    query: { uploadId } }, completeParams);
                completeMultipartUpload(authInfo, compParams, log, err => {
                    assert.strictEqual(err.code, 500);
                    done();
                });
            });
        });
    });

    it('should complete a multipart upload on real AWS', done => {
        mpuSetup(awsLocation, objectKey, uploadId => {
            const compParams = Object.assign({
                url: `/${objectKey}?uploadId=${uploadId}`,
                query: { uploadId } }, completeParams);
            completeMultipartUpload(authInfo, compParams, log,
            (err, result) => {
                assert.equal(err, null, `Error completing mpu on AWS: ${err}`);
                assertMpuCompleteResults(result);
                assertObjOnBackend(awsLocation, done);
            });
        });
    });

    it('should complete MPU on AWS with same key as object put to file',
    done => {
        putObject('file', () => {
            mpuSetup(awsLocation, objectKey, uploadId => {
                const compParams = Object.assign({
                    url: `/${objectKey}?uploadId=${uploadId}`,
                    query: { uploadId } }, completeParams);
                completeMultipartUpload(authInfo, compParams, log,
                (err, result) => {
                    assert.equal(err, null, 'Error completing mpu on AWS ' +
                    `${err}`);
                    assertMpuCompleteResults(result);
                    assertObjOnBackend(awsLocation, done);
                });
            });
        });
    });

    it('should complete MPU on file with same key as object put to AWS',
    done => {
        putObject(awsLocation, () => {
            mpuSetup('file', objectKey, uploadId => {
                const compParams = Object.assign({
                    url: `/${fileKey}?uploadId=${uploadId}`,
                    query: { uploadId } }, completeParams);
                completeMultipartUpload(authInfo, compParams, log,
                (err, result) => {
                    assert.equal(err, null, 'Error completing mpu on file ' +
                    `${err}`);
                    assertMpuCompleteResults(result);
                    assertObjOnBackend('file', done);
                });
            });
        });
    });

    it('should complete MPU on AWS with Scality S3 versioning enabled',
    done => {
        // putting null version: put obj before versioning configured
        putObject(awsLocation, () => {
            const enableVersioningRequest = versioningTestUtils.
                createBucketPutVersioningReq(bucketName, 'Enabled');
            bucketPutVersioning(authInfo, enableVersioningRequest, log, err => {
                assert.equal(err, null, 'Error enabling bucket versioning: ' +
                    `${err}`);
                mpuSetup(awsLocation, objectKey, uploadId => {
                    const compParams = Object.assign({
                        url: `/${objectKey}?uploadId=${uploadId}`,
                        query: { uploadId } }, completeParams);
                    completeMultipartUpload(authInfo, compParams, log,
                    (err, result) => {
                        assert.equal(err, null, 'Error completing mpu on ' +
                        `AWS: ${err}`);
                        assertMpuCompleteResults(result);
                        assertObjOnBackend(awsLocation, () => {
                            assertVersioning(done);
                        });
                    });
                });
            });
        });
    });

    it('should return invalidPart error', done => {
        mpuSetup(awsLocation, objectKey, uploadId => {
            const errorBody = '<CompleteMultipartUpload>' +
                '<Part>' +
                '<PartNumber>1</PartNumber>' +
                `<ETag>"${awsETag}"</ETag>` +
                '</Part>' +
                '<Part>' +
                '<PartNumber>2</PartNumber>' +
                `<ETag>"${awsETag}"</ETag>` +
                '</Part>' +
                '</CompleteMultipartUpload>';
            const compParams = Object.assign({
                url: `/${objectKey}?uploadId=${uploadId}`,
                query: { uploadId } }, completeParams);
            compParams.post = errorBody;
            completeMultipartUpload(authInfo, compParams, log, err => {
                assert.deepStrictEqual(err, errors.InvalidPart);
                done();
            });
        });
    });

    it('should return invalidPartOrder error', done => {
        mpuSetup(awsLocation, objectKey, uploadId => {
            const errorBody = '<CompleteMultipartUpload>' +
                '<Part>' +
                '<PartNumber>2</PartNumber>' +
                `<ETag>"${awsETag}"</ETag>` +
                '</Part>' +
                '<Part>' +
                '<PartNumber>1</PartNumber>' +
                `<ETag>"${awsETagBigObj}"</ETag>` +
                '</Part>' +
                '</CompleteMultipartUpload>';
            const compParams = Object.assign({
                url: `/${objectKey}?uploadId=${uploadId}`,
                query: { uploadId } }, completeParams);
            compParams.post = errorBody;
            completeMultipartUpload(authInfo, compParams, log, err => {
                assert.deepStrictEqual(err, errors.InvalidPartOrder);
                done();
            });
        });
    });

    it('should return entityTooSmall error', done => {
        mpuSetup(awsLocation, objectKey, uploadId => {
            const putPartParam = Object.assign({
                query: { partNumber: '3', uploadId } }, partParams);
            const partRequest3 = new DummyRequest(putPartParam, smallBody);
            objectPutPart(authInfo, partRequest3, undefined, log, err => {
                assert.equal(err, null, `Error putting part: ${err}`);
                const errorBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>2</PartNumber>' +
                    `<ETag>"${awsETag}"</ETag>` +
                    '</Part>' +
                    '<Part>' +
                    '<PartNumber>3</PartNumber>' +
                    `<ETag>"${awsETag}"</ETag>` +
                    '</Part>' +
                    '</CompleteMultipartUpload>';
                const compParams = Object.assign({
                    url: `/${objectKey}?uploadId=${uploadId}`,
                    query: { uploadId } }, completeParams);
                compParams.post = errorBody;
                completeMultipartUpload(authInfo, compParams, log, err => {
                    assert.deepStrictEqual(err, errors.EntityTooSmall);
                    done();
                });
            });
        });
    });

    it('should list all multipart uploads on all backends', done => {
        async.series([
            cb => mpuSetup('file', fileKey,
                fileUploadId => cb(null, fileUploadId)),
            cb => mpuSetup('mem', memKey, memUploadId =>
                cb(null, memUploadId)),
            cb => mpuSetup(awsLocation, objectKey, awsUploadId =>
                cb(null, awsUploadId)),
        ], (err, uploadIds) => {
            assert.equal(err, null, `Error setting up MPUs: ${err}`);
            const listMpuParams = {
                bucketName,
                namespace,
                headers: { host: '/' },
                url: `/${bucketName}?uploads`,
                query: {},
            };
            listMultipartUploads(authInfo, listMpuParams, log,
            (err, mpuListXml) => {
                assert.equal(err, null, `Error listing MPUs: ${err}`);
                parseString(mpuListXml, (err, json) => {
                    const mpuListing = json.ListMultipartUploadsResult.Upload;
                    assert.strictEqual(fileKey, mpuListing[0].Key[0]);
                    assert.strictEqual(uploadIds[0], mpuListing[0].UploadId[0]);
                    assert.strictEqual(memKey, mpuListing[1].Key[0]);
                    assert.strictEqual(uploadIds[1], mpuListing[1].UploadId[0]);
                    assert.strictEqual(objectKey, mpuListing[2].Key[0]);
                    assert.strictEqual(uploadIds[2], mpuListing[2].UploadId[0]);
                    const backendsInfo = [
                        { backend: 'file', key: fileKey,
                            uploadId: uploadIds[0] },
                        { backend: 'mem', key: memKey, uploadId: uploadIds[1] },
                        { backend: 'aws', key: objectKey,
                            uploadId: uploadIds[2] },
                    ];
                    abortMultipleMpus(backendsInfo, done);
                });
            });
        });
    });
});
