import { errors } from 'arsenal';
import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import { cleanup, DummyRequestLogger, makeAuthInfo } from '../helpers';
import objectPut from '../../../lib/api/objectPut';
import objectHead from '../../../lib/api/objectHead';
import DummyRequest from '../DummyRequest';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = new Buffer('I am a body');
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

        bucketPut(authInfo, testPutBucketRequest, log, (err, success) => {
            assert.strictEqual(success, 'Bucket created');
            objectPut(authInfo, testPutObjectRequest, log, (err, result) => {
                assert.strictEqual(result, correctMD5);
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
        bucketPut(authInfo, testPutBucketRequest, log, (err, success) => {
            assert.strictEqual(success, 'Bucket created');
            objectPut(authInfo, testPutObjectRequest, log, (err, result) => {
                assert.strictEqual(result, correctMD5);
                objectHead(authInfo, testGetRequest, log, err => {
                    assert.deepStrictEqual(err, errors.PreconditionFailed);
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

        bucketPut(authInfo, testPutBucketRequest, log, (err, success) => {
            assert.strictEqual(success, 'Bucket created');
            objectPut(authInfo, testPutObjectRequest, log, (err, result) => {
                assert.strictEqual(result, correctMD5);
                objectHead(authInfo, testGetRequest, log, err => {
                    assert.deepStrictEqual(err, errors.PreconditionFailed);
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

        bucketPut(authInfo, testPutBucketRequest, log, (err, success) => {
            assert.strictEqual(success, 'Bucket created');
            objectPut(authInfo, testPutObjectRequest, log, (err, result) => {
                assert.strictEqual(result, correctMD5);
                objectHead(authInfo, testGetRequest, log, err => {
                    assert.deepStrictEqual(err, errors.NotModified);
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

        bucketPut(authInfo, testPutBucketRequest, log, (err, success) => {
            assert.strictEqual(success, 'Bucket created');
            objectPut(authInfo, testPutObjectRequest, log, (err, result) => {
                assert.strictEqual(result, correctMD5);
                objectHead(authInfo, testGetRequest, log, (err, success) => {
                    assert.strictEqual(success[userMetadataKey],
                        userMetadataValue);
                    assert.strictEqual(success.ETag, `"${correctMD5}"`);
                    done();
                });
            });
        });
    });
});
