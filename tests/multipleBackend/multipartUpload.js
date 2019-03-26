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
const { metadata } = require('arsenal').storage.metadata.inMemory.metadata;
const mdWrapper = require('../../lib/metadata/wrapper');

const { bucketPut } = require('../../lib/api/bucketPut');
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
const constants = require('../../constants');

const splitter = constants.splitter;
const memLocation = 'scality-internal-mem';
const fileLocation = 'scality-internal-file';
const awsLocation = 'awsbackend';
const awsLocationMismatch = 'awsbackendmismatch';
const awsConfig = getRealAwsConfig(awsLocation);
const s3 = new AWS.S3(awsConfig);
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
};

const basicParams = {
    bucketName,
    namespace,
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

const awsETag = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const awsETagBigObj = 'f1c9645dbc14efddc7d8a322685f26eb';
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
        expect(err).toEqual(null);
        expect(json.InitiateMultipartUploadResult
            .Bucket[0]).toBe(bucketName);
        expect(json.InitiateMultipartUploadResult
            .Key[0]).toBe(key);
        expect(json.InitiateMultipartUploadResult.UploadId[0]).toBeTruthy();
        cb(json.InitiateMultipartUploadResult.UploadId[0]);
    });
}

function assertMpuCompleteResults(compResult, objectKey) {
    parseString(compResult, (err, json) => {
        expect(err).toEqual(null);
        expect(json.CompleteMultipartUploadResult.Location[0]).toBe(`http://${bucketName}.s3.amazonaws.com/${objectKey}`);
        expect(json.CompleteMultipartUploadResult.Bucket[0]).toBe(bucketName);
        expect(json.CompleteMultipartUploadResult.Key[0]).toBe(objectKey);
        const MD = metadata.keyMaps.get(bucketName).get(objectKey);
        expect(MD).toBeTruthy();
    });
}

function assertListResults(listResult, testAttribute, uploadId, objectKey) {
    parseString(listResult, (err, json) => {
        expect(err).toEqual(null);
        expect(json.ListPartsResult.Key[0]).toBe(objectKey);
        expect(json.ListPartsResult.UploadId[0]).toBe(uploadId);
        expect(json.ListPartsResult.Initiator[0].ID[0]).toBe(authInfo.getCanonicalID());

        // attributes to test specific to PartNumberMarker being set
        // in listParts
        if (testAttribute === 'partNumMarker') {
            expect(json.ListPartsResult.NextPartNumberMarker).toBe(undefined);
            expect(json.ListPartsResult.IsTruncated[0]).toBe('false');
            expect(json.ListPartsResult.Part.length).toBe(1);
            expect(json.ListPartsResult.PartNumberMarker[0]).toBe('1');
            // data of second part put
            expect(json.ListPartsResult.Part[0].PartNumber[0]).toBe('2');
            expect(json.ListPartsResult.Part[0].ETag[0]).toBe(`"${awsETag}"`);
            expect(json.ListPartsResult.Part[0].Size[0]).toBe('11');
        } else {
            // common attributes to test if MaxParts set or
            // neither MaxParts nor PartNumberMarker set
            expect(json.ListPartsResult.PartNumberMarker).toBe(undefined);
            expect(json.ListPartsResult.Part[0].PartNumber[0]).toBe('1');
            expect(json.ListPartsResult.Part[0].ETag[0]).toBe(`"${awsETagBigObj}"`);
            expect(json.ListPartsResult.Part[0].Size[0]).toBe('10485760');

            // attributes to test specific to MaxParts being set in listParts
            if (testAttribute === 'maxParts') {
                expect(json.ListPartsResult.NextPartNumberMarker[0]).toBe('1');
                expect(json.ListPartsResult.IsTruncated[0]).toBe('true');
                expect(json.ListPartsResult.Part.length).toBe(1);
                expect(json.ListPartsResult.MaxParts[0]).toBe('1');
            } else {
                // attributes to test if neither MaxParts nor
                // PartNumberMarker set
                expect(json.ListPartsResult.NextPartNumberMarker).toBe(undefined);
                expect(json.ListPartsResult.IsTruncated[0]).toBe('false');
                expect(json.ListPartsResult.Part.length).toBe(2);
                expect(json.ListPartsResult.MaxParts[0]).toBe('1000');
                expect(json.ListPartsResult.Part[1].PartNumber[0]).toBe('2');
                expect(json.ListPartsResult.Part[1].ETag[0]).toBe(`"${awsETag}"`);
                expect(json.ListPartsResult.Part[1].Size[0]).toBe('11');
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
        expect(err).toEqual(null);
        expect(metaHeaders[`x-amz-meta-${locMetaHeader}`]).toBe(expectedBackend);
        if (expectedBackend === awsLocation) {
            return s3.headObject({ Bucket: awsBucket, Key: objectKey },
            (err, result) => {
                expect(err).toEqual(null);
                expect(result.Metadata[locMetaHeader]).toBe(awsLocation);
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
        expect(err).toEqual(null);
        const putPartParam2 = getPartParams(key, uploadId, 2);
        const partRequest2 = new DummyRequest(putPartParam2, smallBody);
        objectPutPart(authInfo, partRequest2, undefined, log, err => {
            expect(err).toEqual(null);
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
        expect(err).toBe(null);
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
        expect(err).toEqual(null);
        return cb();
    });
}

function abortMPU(uploadId, awsParams, cb) {
    const abortParams = Object.assign({ UploadId: uploadId }, awsParams);
    s3.abortMultipartUpload(abortParams, err => {
        expect(err).toEqual(null);
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
        expect(err).toEqual(null);
        callback();
    });
}

describe('Multipart Upload API with AWS Backend', () => {
    this.timeout(60000);

    beforeEach(done => {
        bucketPut(authInfo, bucketPutRequest, log, err => {
            expect(err).toEqual(null);
            done();
        });
    });

    afterEach(() => {
        cleanup();
    });

    test('should initiate a multipart upload on real AWS', done => {
        const objectKey = `key-${Date.now()}`;
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
            expect(err).toBe(null);
            assertMpuInitResults(result, objectKey, uploadId => {
                abortMPU(uploadId, getAwsParams(objectKey), done);
            });
        });
    });

    test('should initiate a multipart upload on AWS location with ' +
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
        };

        initiateMultipartUpload(authInfo, initiateRequest, log,
        (err, result) => {
            expect(err).toBe(null);
            assertMpuInitResults(result, objectKey, uploadId => {
                abortMPU(uploadId, getAwsParamsBucketNotMatch(objectKey), done);
            });
        });
    });

    test('should list the parts of a multipart upload on real AWS', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocation, objectKey, uploadId => {
            const listParams = getListParams(objectKey, uploadId);
            listParts(authInfo, listParams, log, (err, result) => {
                expect(err).toEqual(null);
                assertListResults(result, null, uploadId, objectKey);
                abortMPU(uploadId, getAwsParams(objectKey), done);
            });
        });
    });

    test('should list the parts of a multipart upload on real AWS location ' +
    'with bucketMatch set to false', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocationMismatch, objectKey, uploadId => {
            const listParams = getListParams(objectKey, uploadId);
            listParts(authInfo, listParams, log, (err, result) => {
                expect(err).toEqual(null);
                assertListResults(result, null, uploadId, objectKey);
                abortMPU(uploadId, getAwsParamsBucketNotMatch(objectKey), done);
            });
        });
    });

    test(
        'should only return number of parts equal to specified maxParts',
        done => {
            this.timeout(90000);
            const objectKey = `key-${Date.now()}`;
            mpuSetup(awsLocation, objectKey, uploadId => {
                const listParams = getListParams(objectKey, uploadId);
                listParams.query['max-parts'] = '1';
                listParts(authInfo, listParams, log, (err, result) => {
                    expect(err).toEqual(null);
                    assertListResults(result, 'maxParts', uploadId, objectKey);
                    abortMPU(uploadId, getAwsParams(objectKey), done);
                });
            });
        }
    );

    test('should only list parts after PartNumberMarker', done => {
        this.timeout(90000);
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocation, objectKey, uploadId => {
            const listParams = getListParams(objectKey, uploadId);
            listParams.query['part-number-marker'] = '1';
            listParts(authInfo, listParams, log, (err, result) => {
                expect(err).toEqual(null);
                assertListResults(result, 'partNumMarker', uploadId, objectKey);
                abortMPU(uploadId, getAwsParams(objectKey), done);
            });
        });
    });

    test('should return an error on listParts of deleted MPU', done => {
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
                    assert.deepStrictEqual(err, errors.ServiceUnavailable
                      .customizeDescription(wantedDesc));
                    done();
                });
            });
        });
    });

    test('should abort a multipart upload on real AWS', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocation, objectKey, uploadId => {
            const delParams = getDeleteParams(objectKey, uploadId);
            multipartDelete(authInfo, delParams, log, err => {
                expect(err).toEqual(null);
                s3.listParts({
                    Bucket: awsBucket,
                    Key: objectKey,
                    UploadId: uploadId,
                }, err => {
                    const wantedError = isCEPH ? 'NoSuchKey' : 'NoSuchUpload';
                    expect(err.code).toBe(wantedError);
                    done();
                });
            });
        });
    });

    test('should abort a multipart upload on real AWS location with' +
    'bucketMatch set to false', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocationMismatch, objectKey, uploadId => {
            const delParams = getDeleteParams(objectKey, uploadId);
            multipartDelete(authInfo, delParams, log, err => {
                expect(err).toEqual(null);
                s3.listParts({
                    Bucket: awsBucket,
                    Key: `${bucketName}/${objectKey}`,
                    UploadId: uploadId,
                }, err => {
                    const wantedError = isCEPH ? 'NoSuchKey' : 'NoSuchUpload';
                    expect(err.code).toBe(wantedError);
                    done();
                });
            });
        });
    });

    test('should return error on abort of MPU that does not exist', done => {
        // legacyAwsBehavior is true (otherwise, there would be no error)
        const fakeKey = `key-${Date.now()}`;
        const delParams = getDeleteParams(fakeKey, fakeUploadId);
        multipartDelete(authInfo, delParams, log, err => {
            expect(err).toEqual(errors.NoSuchUpload);
            done();
        });
    });

    test('should return ServiceUnavailable if MPU deleted directly from AWS ' +
    'and try to complete from S3', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocation, objectKey, uploadId => {
            abortMPU(uploadId, getAwsParams(objectKey), () => {
                const compParams = getCompleteParams(objectKey, uploadId);
                completeMultipartUpload(authInfo, compParams, log, err => {
                    expect(err.code).toBe(503);
                    done();
                });
            });
        });
    });

    test('should complete a multipart upload on real AWS', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocation, objectKey, uploadId => {
            const compParams = getCompleteParams(objectKey, uploadId);
            completeMultipartUpload(authInfo, compParams, log,
            (err, result) => {
                expect(err).toEqual(null);
                assertMpuCompleteResults(result, objectKey);
                assertObjOnBackend(awsLocation, objectKey, done);
            });
        });
    });

    test('should complete a multipart upload on real AWS location with ' +
    'bucketMatch set to false', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocationMismatch, objectKey, uploadId => {
            const compParams = getCompleteParams(objectKey, uploadId);
            completeMultipartUpload(authInfo, compParams, log,
            (err, result) => {
                expect(err).toEqual(null);
                assertMpuCompleteResults(result, objectKey);
                assertObjOnBackend(awsLocationMismatch,
                `${bucketName}/${objectKey}`, done);
            });
        });
    });

    test(
        'should complete MPU on AWS with same key as object put to file',
        done => {
            const objectKey = `key-${Date.now()}`;
            return putObject(fileLocation, objectKey, () => {
                mpuSetup(awsLocation, objectKey, uploadId => {
                    const compParams = getCompleteParams(objectKey, uploadId);
                    completeMultipartUpload(authInfo, compParams, log,
                    (err, result) => {
                        expect(err).toEqual(null);
                        assertMpuCompleteResults(result, objectKey);
                        assertObjOnBackend(awsLocation, objectKey, done);
                    });
                });
            });
        }
    );

    test(
        'should complete MPU on file with same key as object put to AWS',
        done => {
            const objectKey = `key-${Date.now()}`;
            putObject(awsLocation, objectKey, () => {
                mpuSetup(fileLocation, objectKey, uploadId => {
                    const compParams = getCompleteParams(objectKey, uploadId);
                    completeMultipartUpload(authInfo, compParams, log,
                    (err, result) => {
                        expect(err).toEqual(null);
                        assertMpuCompleteResults(result, objectKey);
                        assertObjOnBackend(fileLocation, objectKey, done);
                    });
                });
            });
        }
    );

    test('should be successful initiating MPU on AWS with Scality ' +
    'S3 versioning enabled', done => {
        const objectKey = `key-${Date.now()}`;
        // putting null version: put obj before versioning configured
        putObject(awsLocation, objectKey, () => {
            const enableVersioningRequest = versioningTestUtils.
                createBucketPutVersioningReq(bucketName, 'Enabled');
            bucketPutVersioning(authInfo, enableVersioningRequest, log, err => {
                expect(err).toEqual(null);
                const initiateRequest = {
                    bucketName,
                    namespace,
                    objectKey,
                    headers: { 'host': `${bucketName}.s3.amazonaws.com`,
                        'x-amz-meta-scal-location-constraint': awsLocation },
                    url: `/${objectKey}?uploads`,
                    parsedHost: 'localhost',
                };
                initiateMultipartUpload(authInfo, initiateRequest, log,
                err => {
                    expect(err).toBe(null);
                    done();
                });
            });
        });
    });

    test('should return invalidPart error', done => {
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
                assert.deepStrictEqual(err, errors.InvalidPart);
                done();
            });
        });
    });
    // Ceph doesn't care about order
    itSkipCeph('should return invalidPartOrder error', done => {
        const objectKey = `key-${Date.now()}`;
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
            const compParams = getCompleteParams(objectKey, uploadId);
            compParams.post = errorBody;
            completeMultipartUpload(authInfo, compParams, log, err => {
                assert.deepStrictEqual(err, errors.InvalidPartOrder);
                done();
            });
        });
    });
    test('should return entityTooSmall error', done => {
        const objectKey = `key-${Date.now()}`;
        mpuSetup(awsLocation, objectKey, uploadId => {
            const putPartParam = getPartParams(objectKey, uploadId, 3);
            const partRequest3 = new DummyRequest(putPartParam, smallBody);
            objectPutPart(authInfo, partRequest3, undefined, log, err => {
                expect(err).toEqual(null);
                const errorBody = '<CompleteMultipartUpload>' +
                    '<Part>' +
                    '<PartNumber>1</PartNumber>' +
                    `<ETag>"${awsETagBigObj}"</ETag>` +
                    '</Part>' +
                    '<Part>' +
                    '<PartNumber>2</PartNumber>' +
                    `<ETag>"${awsETag}"</ETag>` +
                    '</Part>' +
                    '<Part>' +
                    '<PartNumber>3</PartNumber>' +
                    `<ETag>"${awsETag}"</ETag>` +
                    '</Part>' +
                    '</CompleteMultipartUpload>';
                const compParams = getCompleteParams(objectKey, uploadId);
                compParams.post = errorBody;
                completeMultipartUpload(authInfo, compParams, log, err => {
                    assert.deepStrictEqual(err, errors.EntityTooSmall);
                    done();
                });
            });
        });
    });

    test('should list all multipart uploads on all backends', done => {
        const objectKey = `testkey-${Date.now()}`;
        const fileKey = `fileKey-${Date.now()}`;
        const memKey = `memKey-${Date.now()}`;
        async.series([
            cb => mpuSetup(fileLocation, fileKey,
                fileUploadId => cb(null, fileUploadId)),
            cb => mpuSetup(memLocation, memKey, memUploadId =>
                cb(null, memUploadId)),
            cb => mpuSetup(awsLocation, objectKey, awsUploadId =>
                cb(null, awsUploadId)),
        ], (err, uploadIds) => {
            expect(err).toEqual(null);
            const listMpuParams = {
                bucketName,
                namespace,
                headers: { host: '/' },
                url: `/${bucketName}?uploads`,
                query: {},
            };
            listMultipartUploads(authInfo, listMpuParams, log,
            (err, mpuListXml) => {
                expect(err).toEqual(null);
                parseString(mpuListXml, (err, json) => {
                    const mpuListing = json.ListMultipartUploadsResult.Upload;
                    expect(fileKey).toBe(mpuListing[0].Key[0]);
                    expect(uploadIds[0]).toBe(mpuListing[0].UploadId[0]);
                    expect(memKey).toBe(mpuListing[1].Key[0]);
                    expect(uploadIds[1]).toBe(mpuListing[1].UploadId[0]);
                    expect(objectKey).toBe(mpuListing[2].Key[0]);
                    expect(uploadIds[2]).toBe(mpuListing[2].UploadId[0]);
                    const backendsInfo = [
                        { backend: fileLocation, key: fileKey,
                            uploadId: uploadIds[0] },
                        { backend: memLocation, key: memKey,
                            uploadId: uploadIds[1] },
                        { backend: 'aws', key: objectKey,
                            uploadId: uploadIds[2] },
                    ];
                    abortMultipleMpus(backendsInfo, done);
                });
            });
        });
    });

    test(
        'should complete a multipart upload initiated on legacy version',
        done => {
            const objectKey = `testkey-${Date.now()}`;
            mpuSetup('scality-internal-mem', objectKey, uploadId => {
                const mputOverviewKey =
                _getOverviewKey(objectKey, uploadId);
                mdWrapper.getObjectMD(mpuBucket, mputOverviewKey, {}, log,
                (err, res) => {
                    // remove location constraint to mimic legacy behvior
                    // eslint-disable-next-line no-param-reassign
                    res.controllingLocationConstraint = undefined;
                    const compParams = getCompleteParams(objectKey, uploadId);
                    completeMultipartUpload(authInfo, compParams, log,
                    (err, result) => {
                        expect(err).toEqual(null);
                        assertMpuCompleteResults(result, objectKey);
                        done();
                    });
                });
            });
        }
    );
});
