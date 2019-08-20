const assert = require('assert');
const async = require('async');

const { errors } = require('arsenal');
const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const objectPut = require('../../../lib/api/objectPut');
const objectCopy = require('../../../lib/api/objectCopy');
const { ds } = require('../../../lib/data/in_memory/backend');
const metastore = require('../../../lib/metadata/in_memory/backend');
const DummyRequest = require('../DummyRequest');
const { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils }
    = require('../helpers');

const initiateMultipartUpload
    = require('../../../lib/api/initiateMultipartUpload');
const completeMultipartUpload
    = require('../../../lib/api/completeMultipartUpload');
const { parseString } = require('xml2js');
const crypto = require('crypto');
const objectPutPart = require('../../../lib/api/objectPutPart');

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

    beforeEach(done => {
        cleanup();
        const testPutObjectRequests = objData.slice(0, 2).map(data =>
            versioningTestUtils.createPutObjectRequest(
                destBucketName, objectKey, data));
        testPutObjectRequests.push(versioningTestUtils
            .createPutObjectRequest(sourceBucketName, objectKey, objData[2]));
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
                setImmediate(() => {
                    versioningTestUtils
                        .assertDataStoreValues(ds, expectedValues);
                    done();
                });
            });
    });

    describe('getObjectMD error condition', () => {
        function errorFunc(method) {
            if (method === metastore.getObject.name) {
                return errors.InternalError;
            }
            return null;
        }

        describe('non-0-byte object', () => {
            beforeEach(done => {
                assert.deepStrictEqual(ds[1].value, objData[0]);
                assert.deepStrictEqual(ds[2].value, objData[1]);
                assert.deepStrictEqual(ds[3].value, objData[2]);
                metastore.setErrorFunc(errorFunc);
                const req = _createObjectCopyRequest(destBucketName);
                objectCopy(
                    authInfo, req, sourceBucketName, objectKey, null, log,
                    err => {
                        assert.deepStrictEqual(err, errors.InternalError);
                        done();
                    });
            });

            afterEach(() => metastore.clearErrorFunc());

            it('should delete the destination data', () => {
                assert.strictEqual(ds.length, 5);
                assert.strictEqual(ds[0], undefined);
                // The source data for null version.
                assert.deepStrictEqual(ds[1].value, objData[0]);
                 // The destination data for null version.
                assert.deepStrictEqual(ds[2].value, objData[1]);
                // The source data for version 1.
                assert.deepStrictEqual(ds[3].value, objData[2]);
                // The destination data for version 1.
                assert.strictEqual(ds[4], undefined);
            });
        });

        describe('0-byte object', () => {
            function errorFunc(method) {
                if (method === metastore.getObject.name) {
                    return errors.InternalError;
                }
                return null;
            }

            beforeEach(done => {
                cleanup();

                const data = Buffer.from('', 'utf8');
                const srcRequest = versioningTestUtils.createPutObjectRequest(
                    sourceBucketName, objectKey, data);
                const destRequest = versioningTestUtils.createPutObjectRequest(
                    destBucketName, objectKey, data);

                async.series([
                    next => bucketPut(
                        authInfo, putSourceBucketRequest, log, next),
                    next => bucketPut(
                        authInfo, putDestBucketRequest, log, next),
                    next => objectPut(
                        authInfo, destRequest, null, log, next),
                    next => bucketPutVersioning(
                        authInfo, enableVersioningRequest, log, next),
                    next => objectPut(
                        authInfo, destRequest, null, log, next),
                    next => bucketPutVersioning(
                        authInfo, suspendVersioningRequest, log, next),
                    next => objectPut(
                        authInfo, srcRequest, null, log, next),
                ], err => {
                    if (err) {
                        return done(err);
                    }
                    assert.strictEqual(ds.length, 0);
                    metastore.setErrorFunc(errorFunc);
                    const req = _createObjectCopyRequest(destBucketName);
                    return objectCopy(authInfo, req, sourceBucketName,
                        objectKey, null, log, err => {
                            assert.deepStrictEqual(err, errors.InternalError);
                            done();
                        });
                });
            });

            afterEach(() => metastore.clearErrorFunc());

            it('should not attempt deletion of the data', () => {
                assert.strictEqual(ds.length, 0);
            });
        });
    });

    describe('putObjectMD error condition', () => {
        function errorFunc(method) {
            if (method === metastore.putObject.name) {
                return errors.InternalError;
            }
            return null;
        }

        beforeEach(done => {
            assert.deepStrictEqual(ds[1].value, objData[0]);
            assert.deepStrictEqual(ds[2].value, objData[1]);
            assert.deepStrictEqual(ds[3].value, objData[2]);
            metastore.setErrorFunc(errorFunc);
            const req = _createObjectCopyRequest(destBucketName);
            objectCopy(
                authInfo, req, sourceBucketName, objectKey, null, log, err => {
                    assert.deepStrictEqual(err, errors.InternalError);
                    done();
                });
        });

        afterEach(() => metastore.clearErrorFunc());

        it('should delete the destination data', () => {
            assert.strictEqual(ds.length, 5);
            assert.strictEqual(ds[0], undefined);
            // The source data for null version.
            assert.deepStrictEqual(ds[1].value, objData[0]);
             // The destination data for null version.
            assert.deepStrictEqual(ds[2].value, objData[1]);
            // The source data for version 1.
            assert.deepStrictEqual(ds[3].value, objData[2]);
            // The destination data for version 1.
            assert.strictEqual(ds[4], undefined);
        });
    });
});

describe('copy object from MPU', () => {
    const initiateMPURequest = {
        bucketName: sourceBucketName,
        namespace,
        objectKey,
        headers: { host: 'localhost' },
        url: `/${objectKey}?uploads`,
    };

    function _createPutPartRequest(uploadId, partNumber, partBody) {
        return new DummyRequest({
            bucketName: sourceBucketName,
            namespace,
            objectKey,
            headers: { host: 'localhost' },
            url: `/${objectKey}?partNumber=${partNumber}&uploadId=${uploadId}`,
            query: {
                partNumber,
                uploadId,
            },
            calculatedHash: crypto
                .createHash('md5')
                .update(partBody)
                .digest('hex'),
        }, partBody);
    }

    function _completeMPURequest(uploadId, parts) {
        const body = [];
        body.push('<CompleteMultipartUpload>');
        parts.forEach(part => {
            body.push(
                '<Part>' +
                    `<PartNumber>${part.partNumber}</PartNumber>` +
                    `<ETag>"${part.eTag}"</ETag>` +
                '</Part>'
            );
        });
        body.push('</CompleteMultipartUpload>');
        return {
            bucketName: sourceBucketName,
            namespace,
            objectKey,
            parsedHost: 'localhost',
            url: `/${objectKey}?uploadId=${uploadId}`,
            headers: { host: 'localhost' },
            query: { uploadId },
            post: body,
        };
    }

    const partOneData = Buffer.alloc((1024 * 1024) * 5, 1);
    const partTwoData = Buffer.alloc((1024 * 1024) * 5, 2);

    beforeEach(done => {
        cleanup();
        const parts = [];
        let uploadID;

        async.waterfall([
            next => bucketPut(authInfo, putSourceBucketRequest, log,
                err => next(err)),
            next => bucketPut(authInfo, putDestBucketRequest, log,
                err => next(err)),
            next => initiateMultipartUpload(
                authInfo, initiateMPURequest, log, next),
            (result, _, next) =>
                parseString(result, next),
            (json, next) => {
                uploadID = json.InitiateMultipartUploadResult.UploadId[0];
                const req = _createPutPartRequest(uploadID, 1, partOneData);
                objectPutPart(authInfo, req, null, log, (err, eTag) => {
                    if (err) {
                        return next(err);
                    }
                    parts.push({ partNumber: 1, eTag });
                    return next();
                });
            },
            next => {
                const req = _createPutPartRequest(uploadID, 2, partTwoData);
                objectPutPart(authInfo, req, null, log, (err, eTag) => {
                    if (err) {
                        return next(err);
                    }
                    parts.push({ partNumber: 2, eTag });
                    return next();
                });
            },
            next => {
                const req = _completeMPURequest(uploadID, parts);
                completeMultipartUpload(authInfo, req, log, next);
            },
        ], err => {
            if (err) {
                return done(err);
            }
            assert.deepStrictEqual(ds[1].value, partOneData);
            assert.deepStrictEqual(ds[2].value, partTwoData);
            const req = _createObjectCopyRequest(destBucketName);
            function errorFunc(method) {
                if (method === metastore.putObject.name) {
                    return errors.InternalError;
                }
                return null;
            }
            metastore.setErrorFunc(errorFunc);
            return objectCopy(authInfo, req, sourceBucketName, objectKey, null,
                log, err => {
                    assert.deepStrictEqual(err, errors.InternalError);
                    done();
                });
        });
    });

    afterEach(() => metastore.clearErrorFunc());

    it('should cleanup data', () => {
        assert.strictEqual(ds.length, 5);
        assert.deepStrictEqual(ds[1].value, partOneData);
        assert.deepStrictEqual(ds[2].value, partTwoData);
        assert.strictEqual(ds[3], undefined);
        assert.strictEqual(ds[4], undefined);
    });
});
