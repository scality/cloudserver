const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const { parseString } = require('xml2js');
const AWS = require('aws-sdk');
const { config } = require('../../lib/Config');
const { cleanup, DummyRequestLogger, makeAuthInfo }
    = require('../unit/helpers');
const { ds } = require('../../lib/data/in_memory/backend');
const { bucketPut } = require('../../lib/api/bucketPut');
const initiateMultipartUpload
    = require('../../lib/api/initiateMultipartUpload');
const objectPutPart = require('../../lib/api/objectPutPart');
const DummyRequest = require('../unit/DummyRequest');
const { metadata } = require('../../lib/metadata/in_memory/metadata');
const constants = require('../../constants');
const { getRealAwsConfig } =
    require('../functional/aws-node-sdk/test/support/awsConfig');

const memLocation = 'scality-internal-mem';
const fileLocation = 'scality-internal-file';
const awsLocation = 'awsbackend';
const awsLocationMismatch = 'awsbackendmismatch';
const awsConfig = getRealAwsConfig(awsLocation);
const s3 = new AWS.S3(awsConfig);

const splitter = constants.splitter;
const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = `bucketname-${Date.now}`;

const body1 = Buffer.from('I am a body', 'utf8');
const body2 = Buffer.from('I am a body with a different ETag', 'utf8');
const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;
const md5Hash1 = crypto.createHash('md5');
const md5Hash2 = crypto.createHash('md5');
const calculatedHash1 = md5Hash1.update(body1).digest('hex');
const calculatedHash2 = md5Hash2.update(body2).digest('hex');

const describeSkipIfE2E = process.env.S3_END_TO_END ? describe.skip : describe;

function putPart(bucketLoc, mpuLoc, requestHost, cb,
errorDescription) {
    const objectName = `objectName-${Date.now()}`;
    const post = bucketLoc ? '<?xml version="1.0" encoding="UTF-8"?>' +
        '<CreateBucketConfiguration ' +
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        `<LocationConstraint>${bucketLoc}</LocationConstraint>` +
        '</CreateBucketConfiguration>' : '';
    const bucketPutReq = {
        bucketName,
        namespace,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
        post,
    };
    if (requestHost) {
        bucketPutReq.parsedHost = requestHost;
    }
    const initiateReq = {
        bucketName,
        namespace,
        objectKey: objectName,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: `/${objectName}?uploads`,
    };
    if (mpuLoc) {
        initiateReq.headers = { 'host': `${bucketName}.s3.amazonaws.com`,
            'x-amz-meta-scal-location-constraint': `${mpuLoc}` };
    }
    if (requestHost) {
        initiateReq.parsedHost = requestHost;
    }
    async.waterfall([
        next => {
            bucketPut(authInfo, bucketPutReq, log, err => {
                assert.ifError(err, 'Error putting bucket');
                next(err);
            });
        },
        next => {
            initiateMultipartUpload(authInfo, initiateReq, log, next);
        },
        (result, corsHeaders, next) => {
            const mpuKeys = metadata.keyMaps.get(mpuBucket);
            assert.strictEqual(mpuKeys.size, 1);
            assert(mpuKeys.keys().next().value
                .startsWith(`overview${splitter}${objectName}`));
            parseString(result, next);
        },
    ],
    (err, json) => {
        if (errorDescription) {
            assert.strictEqual(err.code, 400);
            assert(err.InvalidArgument);
            assert(err.description.indexOf(errorDescription) > -1);
            return cb();
        }
        // Need to build request in here since do not have uploadId
        // until here
        const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
        const partReqParams = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            url: `/${objectName}?partNumber=1&uploadId=${testUploadId}`,
            query: {
                partNumber: '1',
                uploadId: testUploadId,
            },
        };
        const partReq = new DummyRequest(partReqParams, body1);
        return objectPutPart(authInfo, partReq, undefined, log, err => {
            assert.strictEqual(err, null);
            if (bucketLoc !== awsLocation && mpuLoc !== awsLocation &&
            bucketLoc !== awsLocationMismatch &&
            mpuLoc !== awsLocationMismatch) {
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
                assert.strictEqual(partETag, calculatedHash1);
            }
            cb(objectName, testUploadId);
        });
    });
}

function listAndAbort(uploadId, calculatedHash2, objectName, done) {
    const awsBucket = config.locationConstraints[awsLocation].
        details.bucketName;
    const params = {
        Bucket: awsBucket,
        Key: objectName,
        UploadId: uploadId,
    };
    s3.listParts(params, (err, data) => {
        assert.equal(err, null, `Error listing parts: ${err}`);
        assert.strictEqual(data.Parts.length, 1);
        if (calculatedHash2) {
            assert.strictEqual(`"${calculatedHash2}"`, data.Parts[0].ETag);
        }
        s3.abortMultipartUpload(params, err => {
            assert.equal(err, null, `Error aborting MPU: ${err}. ` +
            `You must abort MPU with upload ID ${uploadId} manually.`);
            done();
        });
    });
}

describeSkipIfE2E('objectPutPart API with multiple backends',
function testSuite() {
    this.timeout(5000);

    beforeEach(() => {
        cleanup();
    });

    it('should upload a part to file based on mpu location', done => {
        putPart(memLocation, fileLocation, 'localhost', () => {
            // if ds is empty, the object is not in mem, which means it
            // must be in file because those are the only possibilities
            // for unit tests
            assert.deepStrictEqual(ds, []);
            done();
        });
    });

    it('should put a part to mem based on mpu location', done => {
        putPart(fileLocation, memLocation, 'localhost', () => {
            assert.deepStrictEqual(ds[1].value, body1);
            done();
        });
    });

    it('should put a part to AWS based on mpu location', done => {
        putPart(fileLocation, awsLocation, 'localhost',
        (objectName, uploadId) => {
            assert.deepStrictEqual(ds, []);
            listAndAbort(uploadId, null, objectName, done);
        });
    });

    it('should replace part if two parts uploaded with same part number to AWS',
    done => {
        putPart(fileLocation, awsLocation, 'localhost',
        (objectName, uploadId) => {
            assert.deepStrictEqual(ds, []);
            const partReqParams = {
                bucketName,
                namespace,
                objectKey: objectName,
                headers: { 'host': `${bucketName}.s3.amazonaws.com`,
                    'x-amz-meta-scal-location-constraint': awsLocation },
                url: `/${objectName}?partNumber=1&uploadId=${uploadId}`,
                query: {
                    partNumber: '1', uploadId,
                },
            };
            const partReq = new DummyRequest(partReqParams, body2);
            objectPutPart(authInfo, partReq, undefined, log, err => {
                assert.equal(err, null, `Error putting second part: ${err}`);
                listAndAbort(uploadId, calculatedHash2, objectName, done);
            });
        });
    });

    it('should upload part based on mpu location even if part ' +
        'location constraint is specified ', done => {
        putPart(fileLocation, memLocation, 'localhost', () => {
            assert.deepStrictEqual(ds[1].value, body1);
            done();
        });
    });

    it('should put a part to file based on bucket location', done => {
        putPart(fileLocation, null, 'localhost', () => {
            assert.deepStrictEqual(ds, []);
            done();
        });
    });

    it('should put a part to mem based on bucket location', done => {
        putPart(memLocation, null, 'localhost', () => {
            assert.deepStrictEqual(ds[1].value, body1);
            done();
        });
    });

    it('should put a part to AWS based on bucket location', done => {
        putPart(awsLocation, null, 'localhost',
        (objectName, uploadId) => {
            assert.deepStrictEqual(ds, []);
            listAndAbort(uploadId, null, objectName, done);
        });
    });

    it('should put a part to AWS based on bucket location with bucketMatch ' +
    'set to true', done => {
        putPart(null, awsLocation, 'localhost',
        (objectName, uploadId) => {
            assert.deepStrictEqual(ds, []);
            listAndAbort(uploadId, null, objectName, done);
        });
    });

    it('should put a part to AWS based on bucket location with bucketMatch ' +
    'set to false', done => {
        putPart(null, awsLocationMismatch, 'localhost',
        (objectName, uploadId) => {
            assert.deepStrictEqual(ds, []);
            listAndAbort(uploadId, null, `${bucketName}/${objectName}`, done);
        });
    });

    it('should put a part to file based on request endpoint', done => {
        putPart(null, null, 'localhost', () => {
            assert.deepStrictEqual(ds, []);
            done();
        });
    });
});
