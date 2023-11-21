const assert = require('assert');
const async = require('async');
const { parseString } = require('xml2js');
const AWS = require('aws-sdk');
const { storage, errors } = require('arsenal');

const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../unit/helpers');
const { bucketPut } = require('../../lib/api/bucketPut');
const initiateMultipartUpload = require('../../lib/api/initiateMultipartUpload');
const objectPut = require('../../lib/api/objectPut');
const objectPutCopyPart = require('../../lib/api/objectPutCopyPart');
const DummyRequest = require('../unit/DummyRequest');
const constants = require('../../constants');
const { metadata } = storage.metadata.inMemory.metadata;
const { ds } = storage.data.inMemory.datastore;

const s3 = new AWS.S3();

const { splitter } = constants;
const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';

const bucketName = 'superbucket9999999';
const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;
const body = Buffer.from('I am a body', 'utf8');

const memLocation = 'scality-internal-mem';
const fileLocation = 'scality-internal-file';
const awsBucket = 'multitester555';
const awsLocation = 'awsbackend';
const awsLocation2 = 'awsbackend2';
const awsLocationMismatch = 'awsbackendmismatch';
const partETag = 'be747eb4b75517bf6b3cf7c5fbb62f3a';

// const describeSkipIfE2E = process.env.S3_END_TO_END ? describe.skip : describe;
const { config } = require('../../lib/Config');
const isCEPH = (config.locationConstraints[awsLocation]
                    .details.awsEndpoint !== undefined &&
                config.locationConstraints[awsLocation]
                    .details.awsEndpoint.indexOf('amazon') === -1);
const itSkipCeph = isCEPH ? it.skip : it;

function getSourceAndDestKeys() {
    const timestamp = Date.now();
    return {
        sourceObjName: `supersourceobject-${timestamp}`,
        destObjName: `copycatobject-${timestamp}`,
    };
}

function getAwsParams(destObjName, uploadId) {
    return { Bucket: awsBucket, Key: destObjName, UploadId: uploadId };
}

function getAwsParamsBucketMismatch(destObjName, uploadId) {
    const params = getAwsParams(destObjName, uploadId);
    params.Key = `${bucketName}/${destObjName}`;
    return params;
}

function copyPutPart(bucketLoc, mpuLoc, srcObjLoc, requestHost, cb,
    errorPutCopyPart) {
    const keys = getSourceAndDestKeys();
    const { sourceObjName, destObjName } = keys;
    const post = bucketLoc ? '<?xml version="1.0" encoding="UTF-8"?>'
        + '<CreateBucketConfiguration '
        + 'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        + `<LocationConstraint>${bucketLoc}</LocationConstraint>`
        + '</CreateBucketConfiguration>' : '';
    const bucketPutReq = new DummyRequest({
        bucketName,
        namespace,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
        post,
    });
    if (requestHost) {
        bucketPutReq.parsedHost = requestHost;
    }
    const initiateReq = {
        bucketName,
        namespace,
        objectKey: destObjName,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: `/${destObjName}?uploads`,
        actionImplicitDenies: false,
    };
    if (mpuLoc) {
        initiateReq.headers = {
            'host': `${bucketName}.s3.amazonaws.com`,
            'x-amz-meta-scal-location-constraint': `${mpuLoc}`,
        };
    }
    if (requestHost) {
        initiateReq.parsedHost = requestHost;
    }
    const sourceObjPutParams = {
        bucketName,
        namespace,
        objectKey: sourceObjName,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
        actionImplicitDenies: false,
    };
    if (srcObjLoc) {
        sourceObjPutParams.headers = {
            'host': `${bucketName}.s3.amazonaws.com`,
            'x-amz-meta-scal-location-constraint': `${srcObjLoc}`,
        };
    }
    const sourceObjPutReq = new DummyRequest(sourceObjPutParams, body);
    if (requestHost) {
        sourceObjPutReq.parsedHost = requestHost;
    }

    async.waterfall([
        next => {
            bucketPut(authInfo, bucketPutReq, log, err => {
                assert.ifError(err, 'Error putting bucket');
                next(err);
            });
        },
        next => {
            objectPut(authInfo, sourceObjPutReq, undefined, log, err => next(err));
        },
        next => {
            initiateMultipartUpload(authInfo, initiateReq, log, next);
        },
        (result, corsHeaders, next) => {
            const mpuKeys = metadata.keyMaps.get(mpuBucket);
            assert.strictEqual(mpuKeys.size, 1);
            assert(mpuKeys.keys().next().value
                .startsWith(`overview${splitter}${destObjName}`));
            parseString(result, next);
        },
    ],
    (err, json) => {
        // Need to build request in here since do not have
        // uploadId until here
        assert.ifError(err, 'Error putting source object or initiate MPU');
        const testUploadId = json.InitiateMultipartUploadResult
            .UploadId[0];
        const copyPartParams = {
            bucketName,
            namespace,
            objectKey: destObjName,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            url: `/${destObjName}?partNumber=1&uploadId=${testUploadId}`,
            query: {
                partNumber: '1',
                uploadId: testUploadId,
            },
        };
        const copyPartReq = new DummyRequest(copyPartParams);
        return objectPutCopyPart(authInfo, copyPartReq,
            bucketName, sourceObjName, undefined, log, (err, copyResult) => {
                if (errorPutCopyPart) {
                    assert.strictEqual(err.code, errorPutCopyPart.statusCode);
                    assert(err.is[errorPutCopyPart.code]);
                    return cb();
                }
                assert.strictEqual(err, null);
                return parseString(copyResult, (err, json) => {
                    assert.equal(err, null, `Error parsing copy result ${err}`);
                    assert.strictEqual(json.CopyPartResult.ETag[0],
                        `"${partETag}"`);
                    assert(json.CopyPartResult.LastModified);
                    return cb(keys, testUploadId);
                });
            });
    });
}

function assertPartList(partList, uploadId) {
    assert.strictEqual(partList.UploadId, uploadId);
    assert.strictEqual(partList.Parts.length, 1);
    assert.strictEqual(partList.Parts[0].ETag, `"${partETag}"`);
    assert.strictEqual(partList.Parts[0].PartNumber, 1);
    assert.strictEqual(partList.Parts[0].Size, 11);
}

describe.skip('ObjectCopyPutPart API with multiple backends',
function testSuite() {
    this.timeout(60000);

        beforeEach(() => {
            cleanup();
        });

        it('should copy part to mem based on mpu location', done => {
            copyPutPart(fileLocation, memLocation, null, 'localhost', () => {
            // object info is stored in ds beginning at index one,
            // so an array length of two means only one object
            // was stored in mem
            assert.strictEqual(ds.length, 2);
            assert.deepStrictEqual(ds[1].value, body);
            done();
        });
    });

    it('should copy part to file based on mpu location', done => {
        copyPutPart(memLocation, fileLocation, null, 'localhost', () => {
            assert.strictEqual(ds.length, 2);
            done();
        });
    });

    itSkipCeph('should copy part to AWS based on mpu location', done => {
        copyPutPart(memLocation, awsLocation, null, 'localhost',
        (keys, uploadId) => {
            assert.strictEqual(ds.length, 2);
            const awsReq = getAwsParams(keys.destObjName, uploadId);
            s3.listParts(awsReq, (err, partList) => {
                assertPartList(partList, uploadId);
                s3.abortMultipartUpload(awsReq, err => {
                    assert.equal(err, null, `Error aborting MPU: ${err}. ` +
                    `You must abort MPU with upload ID ${uploadId} manually.`);
                    done();
                });
            });
        });
    });

    it('should copy part to mem from AWS based on mpu location', done => {
        copyPutPart(awsLocation, memLocation, null, 'localhost', () => {
            assert.strictEqual(ds.length, 2);
            assert.deepStrictEqual(ds[1].value, body);
            done();
        });
    });

    it('should copy part to mem based on bucket location', done => {
        copyPutPart(memLocation, null, null, 'localhost', () => {
            // ds length should be three because both source
            // and copied objects should be in mem
            assert.strictEqual(ds.length, 3);
            assert.deepStrictEqual(ds[2].value, body);
            done();
        });
    });

    it('should copy part to file based on bucket location', done => {
        copyPutPart(fileLocation, null, null, 'localhost', () => {
            // ds should be empty because both source and
            // coped objects should be in file
            assert.deepStrictEqual(ds, []);
            done();
        });
    });

    itSkipCeph('should copy part to AWS based on bucket location', done => {
        copyPutPart(awsLocation, null, null, 'localhost', (keys, uploadId) => {
            assert.deepStrictEqual(ds, []);
            const awsReq = getAwsParams(keys.destObjName, uploadId);
            s3.listParts(awsReq, (err, partList) => {
                assertPartList(partList, uploadId);
                s3.abortMultipartUpload(awsReq, err => {
                    assert.equal(err, null, `Error aborting MPU: ${err}. ` +
                    `You must abort MPU with upload ID ${uploadId} manually.`);
                    done();
                });
            });
        });
    });

    itSkipCeph('should copy part an object on AWS location that has ' +
    'bucketMatch equals false to a mpu with a different AWS location', done => {
        copyPutPart(null, awsLocation, awsLocationMismatch, 'localhost',
        (keys, uploadId) => {
            assert.deepStrictEqual(ds, []);
            const awsReq = getAwsParams(keys.destObjName, uploadId);
            s3.listParts(awsReq, (err, partList) => {
                assertPartList(partList, uploadId);
                s3.abortMultipartUpload(awsReq, err => {
                    assert.equal(err, null, `Error aborting MPU: ${err}. ` +
                    `You must abort MPU with upload ID ${uploadId} manually.`);
                    done();
                });
            });
        });
    });

    itSkipCeph('should copy part an object on AWS to a mpu with a different ' +
    'AWS location that has bucketMatch equals false', done => {
        copyPutPart(null, awsLocationMismatch, awsLocation, 'localhost',
        (keys, uploadId) => {
            assert.deepStrictEqual(ds, []);
            const awsReq = getAwsParamsBucketMismatch(keys.destObjName,
                uploadId);
            s3.listParts(awsReq, (err, partList) => {
                assertPartList(partList, uploadId);
                s3.abortMultipartUpload(awsReq, err => {
                    assert.equal(err, null, `Error aborting MPU: ${err}. ` +
                    `You must abort MPU with upload ID ${uploadId} manually.`);
                    done();
                });
            });
        });
    });

    itSkipCeph('should return error 403 AccessDenied copying part to a ' +
    'different AWS location without object READ access',
    done => {
        copyPutPart(null, awsLocation, awsLocation2, 'localhost', done,
            errors.AccessDenied);
    });


    it('should copy part to file based on request endpoint', done => {
        copyPutPart(null, null, memLocation, 'localhost', () => {
            assert.strictEqual(ds.length, 2);
            done();
        });
    });
});
