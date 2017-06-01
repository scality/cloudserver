const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const { parseString } = require('xml2js');

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

const splitter = constants.splitter;
const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const objectName = 'objectName';
const body = Buffer.from('I am a body', 'utf8');
const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;

const describeSkipIfE2E = process.env.S3_END_TO_END ? it.skip : it;

function putPart(bucketLoc, mpuLoc, partLoc, requestHost, cb,
errorDescription) {
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
        const md5Hash = crypto.createHash('md5');
        const bufferBody = Buffer.from(body);
        const calculatedHash = md5Hash.update(bufferBody).digest('hex');
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
            calculatedHash,
        };
        if (partLoc) {
            partReqParams.headers = { 'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-scal-location-constraint': `${partLoc}`,
            };
        }
        const partReq = new DummyRequest(partReqParams, body);
        return objectPutPart(authInfo, partReq, undefined, log, err => {
            assert.strictEqual(err, null);
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
            assert.strictEqual(partETag, calculatedHash);
            cb();
        });
    });
}

describeSkipIfE2E('objectPutPart API with multiple backends', () => {
    afterEach(() => {
        cleanup();
    });

    it('should return error InvalidArgument if no host and data backend ' +
    'set to "multiple"', done => {
        putPart('mem', 'file', null, null, () => done(),
        'Endpoint Location Error');
    });

    it('should upload a part to file based on mpu location', done => {
        putPart('mem', 'file', null, 'localhost', () => {
            // if ds is empty, the object is not in mem, which means it
            // must be in file because those are the only possibilities
            // for unit tests
            assert.deepStrictEqual(ds, []);
            done();
        });
    });

    it('should put a part to mem based on mpu location', done => {
        putPart('file', 'mem', null, 'localhost', () => {
            assert.deepStrictEqual(ds[1].value, body);
            done();
        });
    });

    it('should upload part based on mpu location even if part ' +
        'location constraint is specified ', done => {
        putPart('file', 'mem', 'file', 'localhost', () => {
            assert.deepStrictEqual(ds[1].value, body);
            done();
        });
    });

    it('should put a part to file based on bucket location', done => {
        putPart('file', null, null, 'localhost', () => {
            assert.deepStrictEqual(ds, []);
            done();
        });
    });

    it('should put a part to mem based on bucket location', done => {
        putPart('mem', null, null, 'localhost', () => {
            assert.deepStrictEqual(ds[1].value, body);
            done();
        });
    });

    it('should put a part to file based on request endpoint', done => {
        putPart(null, null, null, 'localhost', () => {
            assert.deepStrictEqual(ds, []);
            done();
        });
    });
});
