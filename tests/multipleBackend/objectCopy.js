const assert = require('assert');
const async = require('async');

const { bucketPut } = require('../../lib/api/bucketPut');
const objectPut = require('../../lib/api/objectPut');
const objectCopy = require('../../lib/api/objectCopy');
const { metadata } = require('../../lib/metadata/in_memory/metadata');
const DummyRequest = require('../unit/DummyRequest');
const { cleanup, DummyRequestLogger, makeAuthInfo }
    = require('../unit/helpers');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const destBucketName = 'destbucketname';
const sourceBucketName = 'sourcebucketname';
const memLocation = 'mem-test';
const fileLocation = 'file-test';

function _createBucketPutRequest(bucketName, bucketLoc) {
    const post = bucketLoc ? '<?xml version="1.0" encoding="UTF-8"?>' +
        '<CreateBucketConfiguration ' +
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        `<LocationConstraint>${bucketLoc}</LocationConstraint>` +
        '</CreateBucketConfiguration>' : '';
    return new DummyRequest({
        bucketName,
        namespace,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
        post,
    });
}

function _createObjectCopyRequest(destBucketName, objectKey) {
    const params = {
        bucketName: destBucketName,
        namespace,
        objectKey,
        headers: {},
        url: `/${destBucketName}/${objectKey}`,
    };
    return new DummyRequest(params);
}

function _createObjectPutRequest(bucketName, objectKey, body) {
    const sourceObjPutParams = {
        bucketName,
        namespace,
        objectKey,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
    };
    return new DummyRequest(sourceObjPutParams, body);
}

function copySetup(params, cb) {
    const { sourceBucket, sourceLocation, sourceKey, destBucket,
        destLocation, body } = params;
    const putDestBucketRequest =
        _createBucketPutRequest(destBucket, destLocation);
    const putSourceBucketRequest =
        _createBucketPutRequest(sourceBucket, sourceLocation);
    const putSourceObjRequest = _createObjectPutRequest(sourceBucket,
        sourceKey, body);
    async.series([
        callback => bucketPut(authInfo, putDestBucketRequest, log, callback),
        callback => bucketPut(authInfo, putSourceBucketRequest, log, callback),
        callback => objectPut(authInfo, putSourceObjRequest, undefined, log,
            callback),
    ], err => cb(err));
}

describe('ObjectCopy API with multiple backends', () => {
    before(() => {
        cleanup();
    });

    after(() => cleanup());

    it('object metadata for newly stored object should have dataStoreName ' +
    'if copying to mem based on bucket location', done => {
        const params = {
            sourceBucket: sourceBucketName,
            sourceKey: `sourcekey-${Date.now()}`,
            sourceLocation: fileLocation,
            body: 'testbody',
            destBucket: destBucketName,
            destLocation: memLocation,
        };
        const destKey = `destkey-${Date.now()}`;
        const testObjectCopyRequest =
            _createObjectCopyRequest(destBucketName, destKey);
        copySetup(params, err => {
            assert.strictEqual(err, null, `Error setting up copy: ${err}`);
            objectCopy(authInfo, testObjectCopyRequest, sourceBucketName,
                params.sourceKey, undefined, log, err => {
                    assert.strictEqual(err, null, `Error copying: ${err}`);
                    const bucket = metadata.keyMaps.get(params.destBucket);
                    const objMd = bucket.get(destKey);
                    assert.strictEqual(objMd.dataStoreName, memLocation);
                    done();
                });
        });
    });
});
