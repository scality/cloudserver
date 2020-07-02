const assert = require('assert');
const { errors } = require('arsenal');

const { bucketPut } = require('../../../lib/api/bucketPut');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const objectPut = require('../../../lib/api/objectPut');
const objectHead = require('../../../lib/api/objectHead');
const DummyRequest = require('../DummyRequest');
const changeObjectLock =
    require('../../functional/aws-node-sdk/lib/utility/objectLock-util');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const incorrectMD5 = 'fkjwelfjlslfksdfsdfsdfsdfsdfsdj';
const objectName = 'objectName';
const date = new Date();
const laterDate = date.setMinutes(date.getMinutes() + 30);
const earlierDate = date.setMinutes(date.getMinutes() - 30);
const testPutBucketRequest = {
    bucketName,
    namespace,
    headers: {},
    url: `/${bucketName}`,
};
const userMetadataKey = 'x-amz-meta-test';
const userMetadataValue = 'some metadata';

let testPutObjectRequest;

describe('objectHead API', () => {
    beforeEach(() => {
        cleanup();
        testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-meta-test': userMetadataValue },
            url: `/${bucketName}/${objectName}`,
            calculatedHash: correctMD5,
        }, postBody);
    });

    it('should return NotModified if request header ' +
       'includes "if-modified-since" and object ' +
       'not modified since specified time', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'if-modified-since': laterDate },
            url: `/${bucketName}/${objectName}`,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectHead(authInfo, testGetRequest, log, err => {
                        assert.deepStrictEqual(err, errors.NotModified);
                        done();
                    });
                });
        });
    });

    it('should return PreconditionFailed if request header ' +
       'includes "if-unmodified-since" and object has ' +
       'been modified since specified time', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'if-unmodified-since': earlierDate },
            url: `/${bucketName}/${objectName}`,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectHead(authInfo, testGetRequest, log, err => {
                        assert.deepStrictEqual(err,
                            errors.PreconditionFailed);
                        done();
                    });
                });
        });
    });

    it('should return PreconditionFailed if request header ' +
       'includes "if-match" and ETag of object ' +
       'does not match specified ETag', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'if-match': incorrectMD5 },
            url: `/${bucketName}/${objectName}`,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectHead(authInfo, testGetRequest, log, err => {
                        assert.deepStrictEqual(err,
                            errors.PreconditionFailed);
                        done();
                    });
                });
        });
    });

    it('should return NotModified if request header ' +
       'includes "if-none-match" and ETag of object does ' +
       'match specified ETag', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'if-none-match': correctMD5 },
            url: `/${bucketName}/${objectName}`,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectHead(authInfo, testGetRequest, log, err => {
                        assert.deepStrictEqual(err, errors.NotModified);
                        done();
                    });
                });
        });
    });

    it('should return Accept-Ranges header if request includes "Range" ' +
       'header with specified range bytes of an object', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { range: 'bytes=1-9' },
            url: `/${bucketName}/${objectName}`,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
                assert.strictEqual(err, null, `Error copying: ${err}`);
                objectHead(authInfo, testGetRequest, log, (err, res) => {
                    assert.strictEqual(res['accept-ranges'], 'bytes');
                    done();
                });
            });
        });
    });

    it('should return InvalidRequest error when both the Range header and ' +
       'the partNumber query parameter specified', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { range: 'bytes=1-9' },
            url: `/${bucketName}/${objectName}`,
            query: {
                partNumber: '1',
            },
        };
        const customizedInvalidRequestError = errors.InvalidRequest
            .customizeDescription('Cannot specify both Range header and ' +
                'partNumber query parameter.');

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
                assert.strictEqual(err, null, `Error objectPut: ${err}`);
                objectHead(authInfo, testGetRequest, log, err => {
                    assert.deepStrictEqual(err, customizedInvalidRequestError);
                    assert.deepStrictEqual(err.InvalidRequest, true);
                    done();
                });
            });
        });
    });

    it('should return InvalidArgument error if partNumber is nan', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
            query: {
                partNumber: 'nan',
            },
        };
        const customizedInvalidArgumentError = errors.InvalidArgument
            .customizeDescription('Part number must be a number.');

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
                assert.strictEqual(err, null, `Error objectPut: ${err}`);
                objectHead(authInfo, testGetRequest, log, err => {
                    assert.deepStrictEqual(err, customizedInvalidArgumentError);
                    assert.deepStrictEqual(err.InvalidArgument, true);
                    done();
                });
            });
        });
    });

    it('should not return Accept-Ranges header if request does not include ' +
       '"Range" header with specified range bytes of an object', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
                assert.strictEqual(err, null, `Error objectPut: ${err}`);
                objectHead(authInfo, testGetRequest, log, (err, res) => {
                    assert.strictEqual(res['accept-ranges'], undefined);
                    done();
                });
            });
        });
    });

    it('should get the object metadata', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectHead(authInfo, testGetRequest, log, (err, res) => {
                        assert.strictEqual(res[userMetadataKey],
                            userMetadataValue);
                        assert
                        .strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
                });
        });
    });

    it('should get the object metadata with object lock', done => {
        const testPutBucketRequestLock = {
            bucketName,
            namespace,
            headers: { 'x-amz-bucket-object-lock-enabled': true },
            url: `/${bucketName}`,
        };
        const testPutObjectRequestLock = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'x-amz-object-lock-retain-until-date': '2050-10-10',
                'x-amz-object-lock-mode': 'GOVERNANCE',
                'x-amz-object-lock-legal-hold': 'ON',
            },
            url: `/${bucketName}/${objectName}`,
            calculatedHash: correctMD5,
        }, postBody);
        const testGetRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        };

        bucketPut(authInfo, testPutBucketRequestLock, log, () => {
            objectPut(authInfo, testPutObjectRequestLock, undefined, log,
                (err, resHeaders) => {
                    assert.ifError(err);
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectHead(authInfo, testGetRequest, log, (err, res) => {
                        assert.ifError(err);
                        const expectedDate = testPutObjectRequestLock
                        .headers['x-amz-object-lock-retain-until-date'];
                        const expectedMode = testPutObjectRequestLock
                        .headers['x-amz-object-lock-mode'];
                        assert.ifError(err);
                        assert.strictEqual(
                            res['x-amz-object-lock-retain-until-date'],
                            expectedDate);
                        assert.strictEqual(res['x-amz-object-lock-mode'],
                            expectedMode);
                        assert.strictEqual(res['x-amz-object-lock-legal-hold'],
                            'ON');
                        changeObjectLock([{
                            bucket: bucketName,
                            key: objectName,
                            versionId: res['x-amz-version-id'],
                        }], '', done);
                    });
                });
        });
    });
});
