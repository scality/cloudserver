const assert = require('assert');
const async = require('async');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const objectPut = require('../../../lib/api/objectPut');
const objectCopy = require('../../../lib/api/objectCopy');
const { ds } = require('../../../lib/data/in_memory/backend');
const DummyRequest = require('../DummyRequest');
const { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils }
    = require('../helpers');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const destBucketName = 'destbucketname';
const sourceBucketName = 'sourcebucketname';
const objectKey = 'objectName';

function _createBucketPutRequest(bucketName) {
    return new DummyRequest({
        bucketName,
        namespace,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
    });
}

function _createObjectCopyRequest(destBucketName) {
    const params = {
        bucketName: destBucketName,
        namespace,
        objectKey,
        headers: {},
        url: `/${destBucketName}/${objectKey}`,
    };
    return new DummyRequest(params);
}

const putDestBucketRequest = _createBucketPutRequest(destBucketName);
const putSourceBucketRequest = _createBucketPutRequest(sourceBucketName);
const enableVersioningRequest = versioningTestUtils
    .createBucketPutVersioningReq(destBucketName, 'Enabled');
const suspendVersioningRequest = versioningTestUtils
    .createBucketPutVersioningReq(destBucketName, 'Suspended');

describe('objectCopy with versioning', () => {
    const objData = ['foo0', 'foo1', 'foo2'].map(str =>
        Buffer.from(str, 'utf8'));

    const testPutObjectRequests = objData.slice(0, 2).map(data =>
        versioningTestUtils.createPutObjectRequest(destBucketName, objectKey,
            data));
    testPutObjectRequests.push(versioningTestUtils
        .createPutObjectRequest(sourceBucketName, objectKey, objData[2]));

    before(done => {
        cleanup();
        async.series([
            callback => bucketPut(authInfo, putDestBucketRequest, log,
                callback),
            callback => bucketPut(authInfo, putSourceBucketRequest, log,
                callback),
            // putting null version: put obj before versioning configured
            // in dest bucket
            callback => objectPut(authInfo, testPutObjectRequests[0],
                undefined, log, callback),
            callback => bucketPutVersioning(authInfo,
                enableVersioningRequest, log, callback),
            // put another version in dest bucket:
            callback => objectPut(authInfo, testPutObjectRequests[1],
                undefined, log, callback),
            callback => bucketPutVersioning(authInfo,
                suspendVersioningRequest, log, callback),
            // put source object in source bucket
            callback => objectPut(authInfo, testPutObjectRequests[2],
                undefined, log, callback),
        ], err => {
            if (err) {
                return done(err);
            }
            versioningTestUtils.assertDataStoreValues(ds, objData);
            return done();
        });
    });

    after(() => cleanup());

    it('should delete null version when creating new null version, ' +
    'even when null version is not the latest version', done => {
        // will have another copy of last object in datastore after objectCopy
        const expectedValues = [undefined, objData[1], objData[2], objData[2]];
        const testObjectCopyRequest = _createObjectCopyRequest(destBucketName);
        objectCopy(authInfo, testObjectCopyRequest, sourceBucketName, objectKey,
            undefined, log, err => {
                assert.ifError(err, `Unexpected err: ${err}`);
                process.nextTick(() => {
                    versioningTestUtils
                        .assertDataStoreValues(ds, expectedValues);
                    done();
                });
            });
    });
});
