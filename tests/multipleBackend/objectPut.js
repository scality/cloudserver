const assert = require('assert');

const { cleanup, DummyRequestLogger, makeAuthInfo }
    = require('../unit/helpers');
const { ds } = require('../../lib/data/in_memory/backend');
const { bucketPut } = require('../../lib/api/bucketPut');
const objectPut = require('../../lib/api/objectPut');
const DummyRequest = require('../unit/DummyRequest');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const body = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const objectName = 'objectName';

const describeSkipIfE2E = process.env.S3_END_TO_END ? describe.skip : describe;

function put(bucketLoc, objLoc, requestHost, cb, errorDescription) {
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
    if (requestHost) {
        bucketPutReq.parsedHost = requestHost;
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
    if (requestHost) {
        testPutObjReq.parsedHost = requestHost;
    }
    bucketPut(authInfo, bucketPutReq, log, () => {
        objectPut(authInfo, testPutObjReq, undefined, log, (err,
            resHeaders) => {
            if (errorDescription) {
                assert.strictEqual(err.code, 400);
                assert(err.InvalidArgument);
                assert(err.description.indexOf(errorDescription) > -1);
            } else {
                assert.strictEqual(err, null, `Error putting object: ${err}`);
                assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
            }
            cb();
        });
    });
}

describeSkipIfE2E('objectPutAPI with multiple backends', function testSuite() {
    this.timeout(5000);

    afterEach(() => {
        cleanup();
    });

    it('should put an object to mem', done => {
        put('file', 'mem', 'localhost', () => {
            assert.deepStrictEqual(ds[1].value, body);
            done();
        });
    });

    it('should put an object to file', done => {
        put('mem', 'file', 'localhost', () => {
            assert.deepStrictEqual(ds, []);
            done();
        });
    });

    it('should put an object to AWS', done => {
        put('mem', 'aws-test', 'localhost', () => {
            assert.deepStrictEqual(ds, []);
            done();
        });
    });

    it('should put an object to mem based on bucket location', done => {
        put('mem', null, 'localhost', () => {
            assert.deepStrictEqual(ds[1].value, body);
            done();
        });
    });

    it('should put an object to file based on bucket location', done => {
        put('file', null, 'localhost', () => {
            assert.deepStrictEqual(ds, []);
            done();
        });
    });

    it('should put an object to AWS based on bucket location', done => {
        put('aws-test', null, 'localhost', () => {
            assert.deepStrictEqual(ds, []);
            done();
        });
    });

    it('should put an object to Azure based on bucket location', done => {
        put('azuretest', null, 'localhost', () => {
            assert.deepStrictEqual(ds, []);
            done();
        });
    });

    it('should put an object to Azure based on object location', done => {
        put('mem', 'azuretest', 'localhost', () => {
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
