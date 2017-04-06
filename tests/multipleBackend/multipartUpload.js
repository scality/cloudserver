const assert = require('assert');
const AWS = require('aws-sdk');
const { parseString } = require('xml2js');
const { errors } = require('arsenal');

const { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils } =
    require('../unit/helpers');
const DummyRequest = require('../unit/DummyRequest');
const constants = require('../../constants');
const { config } = require('../../lib/Config');
const metadata = require('../../lib/metadata/in_memory/metadata').metadata;

const { bucketPut } = require('../../lib/api/bucketPut');
const bucketGet = require('../../lib/api/bucketGet');
const objectPut = require('../../lib/api/objectPut');
const objectGet = require('../../lib/api/objectGet');
const bucketPutVersioning = require('../../lib/api/bucketPutVersioning');
const initiateMultipartUpload =
    require('../../lib/api/initiateMultipartUpload');
const objectPutPart = require('../../lib/api/objectPutPart');
const completeMultipartUpload =
    require('../../lib/api/completeMultipartUpload');

const s3 = new AWS.S3();
const log = new DummyRequestLogger();

const splitter = constants.splitter;
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;
const awsLocation = 'aws-test';
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
const initiateRequest = {
    bucketName,
    namespace,
    objectKey,
    headers: { 'host': `${bucketName}.s3.amazonaws.com`,
        'x-amz-meta-scal-location-constraint': `${awsLocation}` },
    url: `/${objectKey}?uploads`,
    parsedHost: 'localhost',
};
const awsETag = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const awsETagBigObj = 'f1c9645dbc14efddc7d8a322685f26eb';
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

const awsParams = { Bucket: awsBucket, Key: objectKey };

function assertMpuInitResults(initResult, cb) {
    parseString(initResult, (err, json) => {
        assert.equal(err, null, `Error parsing mpu init results: ${err}`);
        assert.strictEqual(json.InitiateMultipartUploadResult
            .Bucket[0], bucketName);
        assert.strictEqual(json.InitiateMultipartUploadResult
            .Key[0], objectKey);
        assert(json.InitiateMultipartUploadResult.UploadId[0]);
        const mpuKeys = metadata.keyMaps.get(mpuBucket);
        assert.strictEqual(mpuKeys.size, 1);
        assert(mpuKeys.keys().next().value
            .startsWith(`overview${splitter}${objectKey}`));
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

function putParts(uploadId, cb) {
    partParams.url =
        `/${objectKey}?partNumber=1&uploadId=${uploadId}`;
    partParams.query = { partNumber: '1', uploadId };
    const partRequest = new DummyRequest(partParams, bigBody);
    objectPutPart(authInfo, partRequest, undefined, log, err => {
        assert.equal(err, null, `Error putting part: ${err}`);
        partParams.query = { partNumber: '2', uploadId };
        const partRequest2 = new DummyRequest(partParams, smallBody);
        objectPutPart(authInfo, partRequest2, undefined, log, err => {
            assert.equal(err, null, `Error putting part: ${err}`);
            cb();
        });
    });
}

function mpuSetup(cb) {
    initiateMultipartUpload(authInfo, initiateRequest, log,
    (err, result) => {
        assert.strictEqual(err, null, 'Error initiating MPU');
        assertMpuInitResults(result, uploadId => {
            putParts(uploadId, () => {
                cb(uploadId);
            });
        });
    });
}

function putObject(putBackend, cb) {
    objectPutParams.headers = {
        'host': `${bucketName}.s3.amazonaws.com`,
        'x-amz-meta-scal-location-constraint': putBackend,
    };
    const objectPutRequest = new DummyRequest(objectPutParams, smallBody);
    objectPut(authInfo, objectPutRequest, undefined, log, err => {
        assert.equal(err, null, `Error putting object to ${putBackend} ${err}`);
        cb();
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
        initiateMultipartUpload(authInfo, initiateRequest, log,
        (err, result) => {
            assert.strictEqual(err, null, 'Error initiating MPU');
            assertMpuInitResults(result, uploadId => {
                awsParams.UploadId = uploadId;
                s3.abortMultipartUpload(awsParams, err => {
                    assert.strictEqual(err, null,
                        `Error aborting MPU ${err}`);
                    done();
                });
            });
        });
    });

    it('should complete a multipart upload on real AWS', done => {
        mpuSetup(uploadId => {
            completeParams.url = `/${objectKey}?uploadId=${uploadId}`;
            completeParams.query = { uploadId };
            completeMultipartUpload(authInfo, completeParams, log,
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
            mpuSetup(uploadId => {
                completeParams.url = `/${objectKey}?uploadId=${uploadId}`;
                completeParams.query = { uploadId };
                completeMultipartUpload(authInfo, completeParams, log,
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
            initiateRequest.headers = {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-scal-location-constraint': 'file',
            };
            mpuSetup(uploadId => {
                completeParams.url = `/${objectKey}?uploadId=${uploadId}`;
                completeParams.query = { uploadId };
                completeMultipartUpload(authInfo, completeParams, log,
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
            initiateRequest.headers = {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-scal-location-constraint': awsLocation,
            };
            const enableVersioningRequest = versioningTestUtils.
                createBucketPutVersioningReq(bucketName, 'Enabled');
            bucketPutVersioning(authInfo, enableVersioningRequest, log, err => {
                assert.equal(err, null, 'Error enabling bucket versioning: ' +
                    `${err}`);
                mpuSetup(uploadId => {
                    completeParams.url = `/${objectKey}?uploadId=${uploadId}`;
                    completeParams.query = { uploadId };
                    completeMultipartUpload(authInfo, completeParams, log,
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
        mpuSetup(uploadId => {
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
            completeParams.post = errorBody;
            completeParams.url = `/${objectKey}?uploadId=${uploadId}`;
            completeParams.query = { uploadId };
            completeMultipartUpload(authInfo, completeParams, log, err => {
                assert.deepStrictEqual(err, errors.InvalidPart);
                done();
            });
        });
    });

    it('should return invalidPartOrder error', done => {
        mpuSetup(uploadId => {
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
            completeParams.post = errorBody;
            completeParams.url = `/${objectKey}?uploadId=${uploadId}`;
            completeParams.query = { uploadId };
            completeMultipartUpload(authInfo, completeParams, log, err => {
                assert.deepStrictEqual(err, errors.InvalidPartOrder);
                done();
            });
        });
    });

    it('should return entityTooSmall error', done => {
        mpuSetup(uploadId => {
            partParams.query = { partNumber: '3', uploadId };
            const partRequest3 = new DummyRequest(partParams, smallBody);
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
                completeParams.post = errorBody;
                completeParams.url = `/${objectKey}?uploadId=${uploadId}`;
                completeParams.query = { uploadId };
                completeMultipartUpload(authInfo, completeParams, log, err => {
                    assert.deepStrictEqual(err, errors.EntityTooSmall);
                    done();
                });
            });
        });
    });
});
