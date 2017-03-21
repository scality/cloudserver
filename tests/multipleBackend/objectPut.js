import assert from 'assert';

import { cleanup, DummyRequestLogger, makeAuthInfo } from '../unit/helpers';
import { ds } from '../../lib/data/in_memory/backend';
import bucketPut from '../../lib/api/bucketPut';
import objectPut from '../../lib/api/objectPut';
import DummyRequest from '../unit/DummyRequest';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const body = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const objectName = 'objectName';

function put(bucketLoc, objLoc, bucketHost, cb) {
    const post = bucketLoc ? '<?xml version="1.0" encoding="UTF-8"?>' +
        '<CreateBucketConfiguration ' +
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        `<LocationConstraint>${bucketLoc}</LocationConstraint>` +
        '</CreateBucketConfiguration>' : '';
    const bucketPutReq = new DummyRequest({
        bucketName,
        namespace,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
        post,
    });
    if (bucketHost) {
        bucketPutReq.parsedHost = bucketHost;
    }
    const objPutParams = {
        bucketName,
        namespace,
        objectKey: objectName,
        headers: {},
        url: `/${bucketName}/${objectName}`,
        calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
    };
    if (objLoc) {
        objPutParams.headers = {
            'x-amz-meta-scal-location-constraint': `${objLoc}`,
        };
    }
    const testPutObjReq = new DummyRequest(objPutParams, body);
    bucketPut(authInfo, bucketPutReq, log, () => {
        objectPut(authInfo, testPutObjReq, undefined, log, (err, result) => {
            assert.strictEqual(err, null, `Error putting object: ${err}`);
            assert.strictEqual(result, correctMD5);
            cb();
        });
    });
}

describe('objectPutAPI with multiple backends', () => {
    beforeEach(() => {
        cleanup();
    });

    it('should put an object to mem', done => {
        put('file', 'mem', null, () => {
            assert.deepStrictEqual(ds[1].value, body);
            done();
        });
    });

    it('should put an object to file', done => {
        put('mem', 'file', null, () => {
            assert.deepStrictEqual(ds, []);
            done();
        });
    });

    it('should put an object to mem based on bucket location', done => {
        put('mem', null, null, () => {
            assert.deepStrictEqual(ds[1].value, body);
            done();
        });
    });

    it('should put an object to file based on bucket location', done => {
        put('file', null, null, () => {
            assert.deepStrictEqual(ds, []);
            done();
        });
    });

    it('should put an object to file based on request endpoint', done => {
        put(null, null, 'localhost', () => {
            assert.deepStrictEqual(ds, []);
            done();
        });
    });
});
