const assert = require('assert');
const async = require('async');
const { S3Client, HeadObjectCommand, AbortMultipartUploadCommand, ListPartsCommand } = require('@aws-sdk/client-s3');
const { parseString } = require('xml2js');
const { models } = require('arsenal');

const BucketInfo = models.BucketInfo;
const { getRealAwsConfig } =
    require('../functional/aws-node-sdk/test/support/awsConfig');
const { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils } =
    require('../unit/helpers');
const DummyRequest = require('../unit/DummyRequest');
const { config } = require('../../lib/Config');
const { metadata } = require('arsenal').storage.metadata.inMemory.metadata;

const { bucketPut } = require('../../lib/api/bucketPut');
const objectPut = require('../../lib/api/objectPut');
const objectGet = require('../../lib/api/objectGet');
const bucketPutVersioning = require('../../lib/api/bucketPutVersioning');
const initiateMultipartUpload =
    require('../../lib/api/initiateMultipartUpload');
const multipartDelete = require('../../lib/api/multipartDelete');
const objectPutCopyPart = require('../../lib/api/objectPutCopyPart');
const objectPutPart = require('../../lib/api/objectPutPart');
const completeMultipartUpload =
    require('../../lib/api/completeMultipartUpload');
const listParts = require('../../lib/api/listParts');
const listMultipartUploads = require('../../lib/api/listMultipartUploads');
const constants = require('../../constants');

const splitter = constants.splitter;
const memLocation = 'scality-internal-mem';
const fileLocation = 'scality-internal-file';
const awsLocation = 'awsbackend';
const awsLocationMismatch = 'awsbackendmismatch';
const awsConfig = getRealAwsConfig(awsLocation);
const s3Client = new S3Client(awsConfig);
const log = new DummyRequestLogger();

const fakeUploadId = 'fakeuploadid';
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;
const awsBucket = config.locationConstraints[awsLocation].details.bucketName;
const awsMismatchBucket = config.locationConstraints[awsLocationMismatch]
                            .details.bucketName;
const smallBody = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);
const locMetaHeader = 'scal-location-constraint';
const isCEPH = (config.locationConstraints[awsLocation]
                    .details.awsEndpoint !== undefined &&
                config.locationConstraints[awsLocation]
                    .details.awsEndpoint.indexOf('amazon') === -1);
const itSkipCeph = isCEPH ? it.skip : it;
const bucketPutRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    post: '',
    parsedHost: 'localhost',
    actionImplicitDenies: false,
};

const awsETag = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const awsETagBigObj = 'f1c9645dbc14efddc7d8a322685f26eb';
const tagSet = 'key1=value1&key2=value2';
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

const basicParams = {
    bucketName,
    namespace,
    actionImplicitDenies: false,
};

function getObjectGetRequest(objectKey) {
    return Object.assign({
        objectKey,
        headers: {},
        url: `/${bucketName}/${objectKey}`,
    }, basicParams);
}

function getDeleteParams(objectKey, uploadId) {
    return Object.assign({
        url: `/${objectKey}?uploadId=${uploadId}`,
        query: { uploadId },
        objectKey,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
    }, basicParams);
}

function getPartParams(objectKey, uploadId, partNumber) {
    return Object.assign({
        objectKey,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: `/${objectKey}?partNumber=${partNumber}&uploadId=${uploadId}`,
        query: { partNumber, uploadId },
    }, basicParams);
}

function _getOverviewKey(objectKey, uploadId) {
    return `overview${splitter}${objectKey}${splitter}${uploadId}`;
}

function getCompleteParams(objectKey, uploadId) {
    return Object.assign({
        objectKey,
        parsedHost: 's3.amazonaws.com',
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        post: completeBody,
        url: `/${objectKey}?uploadId=${uploadId}`,
        query: { uploadId },
    }, basicParams);
}

function getListParams(objectKey, uploadId) {
    return Object.assign({
        objectKey,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: `/${objectKey}?uploadId=${uploadId}`,
        query: { uploadId },
    }, basicParams);
}

function getAwsParams(objectKey) {
    return { Bucket: awsBucket, Key: objectKey };
}

function getAwsParamsBucketNotMatch(objectKey) {
    return { Bucket: awsMismatchBucket, Key: `${bucketName}/${objectKey}` };
}

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

function assertMpuCompleteResults(compResult, objectKey) {
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

function assertListResults(listResult, testAttribute, uploadId, objectKey) {
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

function _getZenkoObjectKey(objectKey) {
    if (objectKey.startsWith(bucketName)) {
        // if it's a bucketNotMatch objectKey, remove the bucketName + '/'
        return objectKey.substring(bucketName.length + 1);
    }
    return objectKey;
}

function assertObjOnBackend(expectedBackend, objectKey, cb) {
    const zenkoObjectKey = _getZenkoObjectKey(objectKey);
    return objectGet(authInfo, getObjectGetRequest(zenkoObjectKey), false, log,
    (err, result, metaHeaders) => {
        assert.equal(err, null, `Error getting object on S3: ${err}`);
        assert.strictEqual(metaHeaders[`x-amz-meta-${locMetaHeader}`], expectedBackend);
        if (expectedBackend === awsLocation) {
            const headObjectParams = { Bucket: awsBucket, Key: objectKey };
            const headObjectCommand = new HeadObjectCommand(headObjectParams);
            return s3Client.send(headObjectCommand)
            .then(result => {
                assert.strictEqual(result.Metadata[locMetaHeader], awsLocation);
                return cb();
            })
            .catch(err => {
                assert.equal(err, null, 'Error on headObject call to AWS: ' +
                    `${err}`);
                return cb();
            });
        }
        return process.nextTick(cb);
    });
}

function putParts(uploadId, key, cb) {
    const putPartParam1 = getPartParams(key, uploadId, 1);
    const partRequest = new DummyRequest(putPartParam1, bigBody);
    objectPutPart(authInfo, partRequest, undefined, log, err => {
        assert.equal(err, null, `Error putting part: ${err}`);
        const putPartParam2 = getPartParams(key, uploadId, 2);
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
        actionImplicitDenies: false,
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

function putObject(putBackend, objectKey, cb) {
    const putParams = Object.assign({
        headers: {
            'host': `${bucketName}.s3.amazonaws.com`,
            'x-amz-meta-scal-location-constraint': putBackend,
        },
        url: '/',
        objectKey,
    }, basicParams);
    const objectPutRequest = new DummyRequest(putParams, smallBody);
    return objectPut(authInfo, objectPutRequest, undefined, log, err => {
        assert.equal(err, null, `Error putting object to ${putBackend} ${err}`);
        return cb();
    });
}

function abortMPU(uploadId, awsParams, cb) {
    const abortParams = Object.assign({ UploadId: uploadId }, awsParams);
    const abortCommand = new AbortMultipartUploadCommand(abortParams);
    s3Client.send(abortCommand)
    .then(() => {
        cb();
    })
    .catch(err => {
        assert.equal(err, null, `Error aborting MPU: ${err}`);
        cb();
    });
}

function abortMultipleMpus(backendsInfo, callback) {
    async.forEach(backendsInfo, (backend, cb) => {
        const delParams = getDeleteParams(backend.key, backend.uploadId);
        multipartDelete(authInfo, delParams, log, err => {
            cb(err);
        });
    }, err => {
        assert.equal(err, null, `Error aborting MPU: ${err}`);
        callback();
    });
}

describe('Multipart Upload API with AWS Backend', function mpuTestSuite() {
    this.timeout(60000);

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
        const objectKey = `key-${Date.now()}`;
        const initiateRequest = {
            bucketName,
            namespace,
            objectKey,
            headers: { 'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-scal-location-constraint': `${awsLocation}` },
            url: `/${objectKey}?uploads`,
            parsedHost: 'localhost',
            actionImplicitDenies: false,
        };

        initiateMultipartUpload(authInfo, initiateRequest, log,
        (err, result) => {
            assert.strictEqual(err, null, 'Error initiating MPU');
            assertMpuInitResults(result, objectKey, uploadId => {
                abortMPU(uploadId, getAwsParams(objectKey), done);
            });
        });
    });

    it('should initiate a multipart upload on AWS location with ' +
    'bucketMatch equals false', done => {
        const objectKey = `key-${Date.now()}`;
        const initiateRequest = {
            bucketName,
            namespace,
            objectKey,
            headers: { 'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-scal-location-constraint':
                `${awsLocationMismatch}` },
            url: `/${objectKey}?uploads`,
            parsedHost: 'localhost',
            actionImplicitDenies: false,
        };

        initiateMultipartUpload(authInfo, initiateRequest, log,
        (err, result) => {
            assert.strictEqual(err, null, 'Error initiating MPU');
            assertMpuInitResults(result, objectKey, uploadId => {
                abortMPU(uploadId, getAwsParamsBucketNotMatch(objectKey), done);
            });
        });
    });

    it('should initiate a multipart upload with tags on AWS', done => {
        const objectKey = `key-${Date.now()}`;
        const initiateRequest = {
            bucketName,
            namespace,
            objectKey,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-scal-location-constraint': `${awsLocation}`,
                'x-amz-tagging': tagSet,
            },
            url: `/${objectKey}?uploads`,
            parsedHost: 'localhost',
            actionImplicitDenies: false,
        };

        initiateMultipartUpload(authInfo, initiateRequest, log,
        (err, result) => {
            assert.ifError(err);
            assertMpuInitResults(result, objectKey, uploadId => {
                abortMPU(uploadId, getAwsParams(objectKey), done);
            });
        });
    });

    it('should list the parts of a multipart upload on real AWS', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocation, objectKey, uploadId => {
            const listParams = getListParams(objectKey, uploadId);
            listParts(authInfo, listParams, log, (err, result) => {
                assert.equal(err, null, `Error listing parts on AWS: ${err}`);
                assertListResults(result, null, uploadId, objectKey);
                abortMPU(uploadId, getAwsParams(objectKey), done);
            });
        });
    });

    it('should list the parts of a multipart upload on real AWS location ' +
    'with bucketMatch set to false', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocationMismatch, objectKey, uploadId => {
            const listParams = getListParams(objectKey, uploadId);
            listParts(authInfo, listParams, log, (err, result) => {
                assert.equal(err, null, `Error listing parts on AWS: ${err}`);
                assertListResults(result, null, uploadId, objectKey);
                abortMPU(uploadId, getAwsParamsBucketNotMatch(objectKey), done);
            });
        });
    });

    it('should only return number of parts equal to specified maxParts',
    function itF(done) {
        this.timeout(90000);
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocation, objectKey, uploadId => {
            const listParams = getListParams(objectKey, uploadId);
            listParams.query['max-parts'] = '1';
            listParts(authInfo, listParams, log, (err, result) => {
                assert.equal(err, null);
                assertListResults(result, 'maxParts', uploadId, objectKey);
                abortMPU(uploadId, getAwsParams(objectKey), done);
            });
        });
    });

    it('should only list parts after PartNumberMarker', done => {
        this.timeout(90000);
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocation, objectKey, uploadId => {
            const listParams = getListParams(objectKey, uploadId);
            listParams.query['part-number-marker'] = '1';
            listParts(authInfo, listParams, log, (err, result) => {
                assert.equal(err, null);
                assertListResults(result, 'partNumMarker', uploadId, objectKey);
                abortMPU(uploadId, getAwsParams(objectKey), done);
            });
        });
    });

    it('should return an error on listParts of deleted MPU', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocation, objectKey, uploadId => {
            abortMPU(uploadId, getAwsParams(objectKey), () => {
                const listParams = getListParams(objectKey, uploadId);
                listParts(authInfo, listParams, log, err => {
                    let wantedDesc = 'Error returned from AWS: ' +
                        'The specified upload does not exist. The upload ID ' +
                        'may be invalid, or the upload may have been aborted' +
                        ' or completed.';
                    if (isCEPH) {
                        wantedDesc = 'Error returned from AWS: null';
                    }
                    assert.strictEqual(err.is.ServiceUnavailable, true);
                    assert.deepStrictEqual(err.description, wantedDesc);
                    done();
                });
            });
        });
    });

    it('should abort a multipart upload on real AWS', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocation, objectKey, uploadId => {
            const delParams = getDeleteParams(objectKey, uploadId);
            multipartDelete(authInfo, delParams, log, err => {
                assert.equal(err, null, `Error aborting MPU: ${err}`);
                const listPartsParams = {
                    Bucket: awsBucket,
                    Key: objectKey,
                    UploadId: uploadId,
                };
                const listPartsCommand = new ListPartsCommand(listPartsParams);
                s3Client.send(listPartsCommand)
                .then(() => {
                    // Should not reach here
                    assert.fail('Expected error but got success');
                })
                .catch(err => {
                    const wantedError = isCEPH ? 'NoSuchKey' : 'NoSuchUpload';
                    assert.strictEqual(err.name, wantedError);
                    done();
                });
            });
        });
    });

    it('should abort a multipart upload on real AWS location with' +
    'bucketMatch set to false', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocationMismatch, objectKey, uploadId => {
            const delParams = getDeleteParams(objectKey, uploadId);
            multipartDelete(authInfo, delParams, log, err => {
                assert.equal(err, null, `Error aborting MPU: ${err}`);
                const listPartsParams = {
                    Bucket: awsBucket,
                    Key: `${bucketName}/${objectKey}`,
                    UploadId: uploadId,
                };
                const listPartsCommand = new ListPartsCommand(listPartsParams);
                s3Client.send(listPartsCommand)
                .then(() => {
                    // Should not reach here
                    assert.fail('Expected error but got success');
                })
                .catch(err => {
                    const wantedError = isCEPH ? 'NoSuchKey' : 'NoSuchUpload';
                    assert.strictEqual(err.name, wantedError);
                    done();
                });
            });
        });
    });

    it('should return error on abort of MPU that does not exist', done => {
        // legacyAwsBehavior is true (otherwise, there would be no error)
        const fakeKey = `key-${Date.now()}`;
        const delParams = getDeleteParams(fakeKey, fakeUploadId);
        multipartDelete(authInfo, delParams, log, err => {
            assert(err.is.NoSuchUpload, `Error aborting MPU: ${err}`);
            done();
        });
    });

    it('should return ServiceUnavailable if MPU deleted directly from AWS ' +
    'and try to complete from S3', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocation, objectKey, uploadId => {
            abortMPU(uploadId, getAwsParams(objectKey), () => {
                const compParams = getCompleteParams(objectKey, uploadId);
                completeMultipartUpload(authInfo, compParams, log, err => {
                    assert.strictEqual(err.code, 503);
                    done();
                });
            });
        });
    });

    it('should complete a multipart upload on real AWS', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocation, objectKey, uploadId => {
            const compParams = getCompleteParams(objectKey, uploadId);
            completeMultipartUpload(authInfo, compParams, log,
            (err, result) => {
                assert.equal(err, null, `Error completing mpu on AWS: ${err}`);
                assertMpuCompleteResults(result, objectKey);
                assertObjOnBackend(awsLocation, objectKey, done);
            });
        });
    });

    it('should complete a multipart upload on real AWS location with ' +
    'bucketMatch set to false', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocationMismatch, objectKey, uploadId => {
            const compParams = getCompleteParams(objectKey, uploadId);
            completeMultipartUpload(authInfo, compParams, log,
            (err, result) => {
                assert.equal(err, null, `Error completing mpu on AWS: ${err}`);
                assertMpuCompleteResults(result, objectKey);
                assertObjOnBackend(awsLocationMismatch,
                `${bucketName}/${objectKey}`, done);
            });
        });
    });

    it('should complete MPU on AWS with same key as object put to file',
    done => {
        const objectKey = `key-${Date.now()}`;
        return putObject(fileLocation, objectKey, () => {
            mpuSetup(awsLocation, objectKey, uploadId => {
                const compParams = getCompleteParams(objectKey, uploadId);
                completeMultipartUpload(authInfo, compParams, log,
                (err, result) => {
                    assert.equal(err, null, 'Error completing mpu on AWS ' +
                    `${err}`);
                    assertMpuCompleteResults(result, objectKey);
                    assertObjOnBackend(awsLocation, objectKey, done);
                });
            });
        });
    });

    it('should complete MPU on file with same key as object put to AWS',
    done => {
        const objectKey = `key-${Date.now()}`;
        putObject(awsLocation, objectKey, () => {
            mpuSetup(fileLocation, objectKey, uploadId => {
                const compParams = getCompleteParams(objectKey, uploadId);
                completeMultipartUpload(authInfo, compParams, log,
                (err, result) => {
                    assert.equal(err, null, 'Error completing mpu on file ' +
                    `${err}`);
                    assertMpuCompleteResults(result, objectKey);
                    assertObjOnBackend(fileLocation, objectKey, done);
                });
            });
        });
    });

    it('should be successful initiating MPU on AWS with Scality ' +
    'S3 versioning enabled', done => {
        const objectKey = `key-${Date.now()}`;
        // putting null version: put obj before versioning configured
        putObject(awsLocation, objectKey, () => {
            const enableVersioningRequest = versioningTestUtils.
                createBucketPutVersioningReq(bucketName, 'Enabled');
            bucketPutVersioning(authInfo, enableVersioningRequest, log, err => {
                assert.equal(err, null, 'Error enabling bucket versioning: ' +
                    `${err}`);
                const initiateRequest = {
                    bucketName,
                    namespace,
                    objectKey,
                    headers: { 'host': `${bucketName}.s3.amazonaws.com`,
                        'x-amz-meta-scal-location-constraint': awsLocation },
                    url: `/${objectKey}?uploads`,
                    parsedHost: 'localhost',
                    actionImplicitDenies: false,
                };
                initiateMultipartUpload(authInfo, initiateRequest, log,
                err => {
                    assert.strictEqual(err, null);
                    done();
                });
            });
        });
    });

    it('should return invalidPart error', done => {
        const objectKey = `key-${Date.now()}`;
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
            const compParams = getCompleteParams(objectKey, uploadId);
            compParams.post = errorBody;
            completeMultipartUpload(authInfo, compParams, log, err => {
                assert.strictEqual(err.is.InvalidPart, true);
                done();
            });
        });
    });
    // Ceph doesn't care about order
    itSkipCeph('should return invalidPartOrder error', async () => {
        const objectKey = `key-${Date.now()}`;
        
        try {
            const uploadId = await mpuSetup(awsLocation, objectKey);
            
            // Create the error body for invalid part order
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
            
            const compParams = getCompleteParams(objectKey, uploadId);
            compParams.post = errorBody;
            
            // Use CompleteMultipartUploadCommand
            const command = new CompleteMultipartUploadCommand({
                Bucket: bucketName,
                Key: objectKey,
                UploadId: uploadId,
                MultipartUpload: {
                    Parts: [
                        { PartNumber: 2, ETag: `"${awsETag}"` },
                        { PartNumber: 1, ETag: `"${awsETagBigObj}"` }
                    ]
                }
            });
            
            try {
                await s3Client.send(command);
                throw new Error('Expected InvalidPartOrder error');
            } catch (err) {
                assert.strictEqual(err.name, 'InvalidPartOrder');
            }
        } catch (err) {
            throw err;
        }
    });

    it('should return entityTooSmall error', async () => {
        const objectKey = `key-${Date.now()}`;
        
        try {
            const uploadId = await mpuSetup(awsLocation, objectKey);
            
            // Upload part 3 with small body
            const putPartParam = getPartParams(objectKey, uploadId, 3);
            const uploadPartCommand = new UploadPartCommand({
                Bucket: bucketName,
                Key: objectKey,
                UploadId: uploadId,
                PartNumber: 3,
                Body: smallBody
            });
            
            await s3Client.send(uploadPartCommand);
            
            // Attempt to complete with parts that are too small
            const completeCommand = new CompleteMultipartUploadCommand({
                Bucket: bucketName,
                Key: objectKey,
                UploadId: uploadId,
                MultipartUpload: {
                    Parts: [
                        { PartNumber: 1, ETag: `"${awsETagBigObj}"` },
                        { PartNumber: 2, ETag: `"${awsETag}"` },
                        { PartNumber: 3, ETag: `"${awsETag}"` }
                    ]
                }
            });
            
            try {
                await s3Client.send(completeCommand);
                throw new Error('Expected EntityTooSmall error');
            } catch (err) {
                assert.strictEqual(err.name, 'EntityTooSmall');
            }
        } catch (err) {
            throw err;
        }
    });

    it('should list all multipart uploads on all backends', async () => {
        const objectKey = `testkey-${Date.now()}`;
        const fileKey = `fileKey-${Date.now()}`;
        const memKey = `memKey-${Date.now()}`;
        
        try {
            // Setup multipart uploads
            const uploadIds = await Promise.all([
                mpuSetup(fileLocation, fileKey),
                mpuSetup(memLocation, memKey),
                mpuSetup(awsLocation, objectKey)
            ]);
            
            // List multipart uploads using AWS SDK v3
            const listCommand = new ListMultipartUploadsCommand({
                Bucket: bucketName,
                MaxUploads: 1000
            });
            
            const response = await s3Client.send(listCommand);
            
            // Verify the uploads are listed
            const uploads = response.Uploads || [];
            assert.strictEqual(uploads.length, 3);
            
            // Find and verify each upload
            const fileUpload = uploads.find(u => u.Key === fileKey);
            const memUpload = uploads.find(u => u.Key === memKey);
            const awsUpload = uploads.find(u => u.Key === objectKey);
            
            assert.strictEqual(fileUpload.UploadId, uploadIds[0]);
            assert.strictEqual(memUpload.UploadId, uploadIds[1]);
            assert.strictEqual(awsUpload.UploadId, uploadIds[2]);
            
            // Cleanup - abort all uploads
            const backendsInfo = [
                { backend: fileLocation, key: fileKey, uploadId: uploadIds[0] },
                { backend: memLocation, key: memKey, uploadId: uploadIds[1] },
                { backend: 'aws', key: objectKey, uploadId: uploadIds[2] }
            ];
            
            await abortMultipleMpus(backendsInfo);
        } catch (err) {
            throw err;
        }
    });

    describe('with mpu initiated on legacy version', () => {
        beforeEach(function() {
            this.currentTest.lcObj = config.locationConstraints;
            const legacyObj = Object.assign(config.locationConstraints, {
                legacy: {
                    type: 'mem',
                    legacyAwsBehavior: true,
                    details: {},
                }
            });
            config.setLocationConstraints(legacyObj);
        });

        afterEach(function() {
            config.setLocationConstraints(this.currentTest.lcObj);
        });

        it('should complete a multipart upload', async () => {
            const objectKey = `testkey-${Date.now()}`;
            
            try {
                const uploadId = await mpuSetup('scality-internal-mem', objectKey);
                const mpuOverviewKey = _getOverviewKey(objectKey, uploadId);
                
                // Modify metadata to mimic legacy behavior
                const bucketMD = BucketInfo.fromObj(metadata.buckets.get(bucketName));
                const objMD = metadata.keyMaps.get(mpuBucket).get(mpuOverviewKey);
                
                // Remove location constraints to mimic legacy behavior
                bucketMD.setLocationConstraint(undefined);
                objMD.controllingLocationConstraint = undefined;
                objMD.dataStoreName = undefined;
                objMD[constants.objectLocationConstraintHeader] = undefined;
                
                metadata.buckets.set(bucketName, bucketMD);
                metadata.keyMaps.get(mpuBucket).set(mpuOverviewKey, objMD);
                
                // Complete the multipart upload
                const compParams = getCompleteParams(objectKey, uploadId);
                const completeCommand = new CompleteMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId,
                    MultipartUpload: compParams.MultipartUpload
                });
                
                const completeRes = await s3Client.send(completeCommand);
                assertMpuCompleteResults(completeRes, objectKey);
            } catch (err) {
                throw err;
            }
        });

        it('should abort a multipart upload', async () => {
            const objectKey = `testkey-${Date.now()}`;
            
            try {
                const uploadId = await mpuSetup('scality-internal-mem', objectKey);
                const mpuOverviewKey = _getOverviewKey(objectKey, uploadId);
                
                // Modify metadata for legacy behavior
                const bucketMD = BucketInfo.fromObj(metadata.buckets.get(bucketName));
                const objMD = metadata.keyMaps.get(mpuBucket).get(mpuOverviewKey);
                
                bucketMD.setLocationConstraint(undefined);
                objMD.controllingLocationConstraint = undefined;
                objMD.dataStoreName = undefined;
                objMD[constants.objectLocationConstraintHeader] = undefined;
                
                metadata.buckets.set(bucketName, bucketMD);
                metadata.keyMaps.get(mpuBucket).set(mpuOverviewKey, objMD);
                
                // Abort the multipart upload
                const abortCommand = new AbortMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId
                });
                
                await s3Client.send(abortCommand);
                
                // Verify the upload was aborted
                const listCommand = new ListPartsCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId
                });
                
                try {
                    await s3Client.send(listCommand);
                    throw new Error('Expected NoSuchUpload error');
                } catch (err) {
                    assert.strictEqual(err.name, 'NoSuchUpload');
                }
            } catch (err) {
                throw err;
            }
        });

        it('should list multipart upload parts', async () => {
            const objectKey = `testkey-${Date.now()}`;
            
            try {
                const uploadId = await mpuSetup('scality-internal-mem', objectKey);
                const mpuOverviewKey = _getOverviewKey(objectKey, uploadId);
                
                // Modify metadata for legacy behavior
                const bucketMD = BucketInfo.fromObj(metadata.buckets.get(bucketName));
                const objMD = metadata.keyMaps.get(mpuBucket).get(mpuOverviewKey);
                
                bucketMD.setLocationConstraint(undefined);
                objMD.controllingLocationConstraint = undefined;
                objMD.dataStoreName = undefined;
                objMD[constants.objectLocationConstraintHeader] = undefined;
                
                metadata.buckets.set(bucketName, bucketMD);
                metadata.keyMaps.get(mpuBucket).set(mpuOverviewKey, objMD);
                
                // List parts
                const listCommand = new ListPartsCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId
                });
                
                const res = await s3Client.send(listCommand);
                assertListResults(res, null, uploadId, objectKey);
                
                // Cleanup
                const abortCommand = new AbortMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId
                });
                
                await s3Client.send(abortCommand);
            } catch (err) {
                throw err;
            }
        });

        it('should copy an object as a part to a multipart upload', async () => {
            const objectKey = `testkey-${Date.now()}`;
            
            try {
                const uploadId = await mpuSetup('scality-internal-mem', objectKey);
                const mpuOverviewKey = _getOverviewKey(objectKey, uploadId);
                
                // Create copy source object
                const copyObjectKey = `copykey-${Date.now()}`;
                await putObject('scality-internal-mem', copyObjectKey);
                
                // Modify metadata for legacy behavior
                const bucketMD = BucketInfo.fromObj(metadata.buckets.get(bucketName));
                const mpuMD = metadata.keyMaps.get(mpuBucket).get(mpuOverviewKey);
                const copyObjMD = metadata.keyMaps.get(bucketName).get(copyObjectKey);
                
                bucketMD.setLocationConstraint(undefined);
                mpuMD.controllingLocationConstraint = undefined;
                mpuMD.dataStoreName = undefined;
                mpuMD[constants.objectLocationConstraintHeader] = undefined;
                copyObjMD.controllingLocationConstraint = undefined;
                copyObjMD.dataStoreName = undefined;
                copyObjMD[constants.objectLocationConstraintHeader] = undefined;
                
                metadata.buckets.set(bucketName, bucketMD);
                metadata.keyMaps.get(mpuBucket).set(mpuOverviewKey, mpuMD);
                metadata.keyMaps.get(bucketName).set(copyObjectKey, copyObjMD);
                
                // Copy part
                const copyPartCommand = new UploadPartCopyCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId,
                    PartNumber: 3,
                    CopySource: `${bucketName}/${copyObjectKey}`
                });
                
                await s3Client.send(copyPartCommand);
                
                // List parts to verify
                const listCommand = new ListPartsCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId
                });
                
                const listRes = await s3Client.send(listCommand);
                const parts = listRes.Parts || [];
                const part3 = parts.find(p => p.PartNumber === 3);
                
                assert.strictEqual(part3.PartNumber, 3);
                assert.strictEqual(part3.ETag, `"${awsETag}"`);
                assert.strictEqual(part3.Size, 11);
                
                // Cleanup
                const abortCommand = new AbortMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                    UploadId: uploadId
                });
                
                await s3Client.send(abortCommand);
            } catch (err) {
                throw err;
            }
        });
    });
});
    