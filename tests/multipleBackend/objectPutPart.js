import assert from 'assert';
import async from 'async';
import crypto from 'crypto';
import { parseString } from 'xml2js';

import { cleanup, DummyRequestLogger, makeAuthInfo } from '../unit/helpers';
import { ds } from '../../lib/data/in_memory/backend';
import bucketPut from '../../lib/api/bucketPut';
import initiateMultipartUpload from '../../lib/api/initiateMultipartUpload';
import objectPutPart from '../../lib/api/objectPutPart';
import DummyRequest from '../unit/DummyRequest';
import { metadata } from '../../lib/metadata/in_memory/metadata';
import constants from '../../constants';

const splitter = constants.splitter;
const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const objectName = 'objectName';
const body = Buffer.from('I am a body', 'utf8');
const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;

function putPart(bucketLoc, mpuLoc, partLoc, host, cb) {
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
    if (host) {
        initiateReq.parsedHost = host;
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
        objectPutPart(authInfo, partReq, undefined, log, err => {
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

describe('objectPutPart API with multiple backends', () => {
    beforeEach(() => {
        cleanup();
    });

    it('should upload a part to file based on mpu location', done => {
        putPart('mem', 'file', null, null, () => {
            // if ds is empty, the object is not in mem, which means it
            // must be in file because those are the only possibilities
            // for unit tests
            assert.deepStrictEqual(ds, []);
            done();
        });
    });

    it('should put a part to mem based on mpu location', done => {
        putPart('file', 'mem', null, null, () => {
            assert.deepStrictEqual(ds[1].value, body);
            done();
        });
    });

    it('should upload part based on mpu location even if part ' +
        'location constraint is specified ', done => {
        putPart('file', 'mem', 'file', null, () => {
            assert.deepStrictEqual(ds[1].value, body);
            done();
        });
    });

    it('should put a part to file based on bucket location', done => {
        putPart('file', null, null, null, () => {
            assert.deepStrictEqual(ds, []);
            done();
        });
    });

    it('should put a part to mem based on bucket location', done => {
        putPart('mem', null, null, null, () => {
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
