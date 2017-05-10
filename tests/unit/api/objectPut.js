import async from 'async';

import { errors } from 'arsenal';
import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import bucketPutACL from '../../../lib/api/bucketPutACL';
import bucketPutVersioning from '../../../lib/api/bucketPutVersioning';
import { parseTagFromQuery } from '../../../lib/api/apiUtils/object/tagging';
import { cleanup, DummyRequestLogger, makeAuthInfo, versioningTestUtils }
    from '../helpers';
import { ds } from '../../../lib/data/in_memory/backend';
import metadata from '../metadataswitch';
import objectPut from '../../../lib/api/objectPut';
import DummyRequest from '../DummyRequest';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const testPutBucketRequest = new DummyRequest({
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
});

const objectName = 'objectName';

let testPutObjectRequest;
const enableVersioningRequest =
    versioningTestUtils.createBucketPutVersioningReq(bucketName, 'Enabled');
const suspendVersioningRequest =
    versioningTestUtils.createBucketPutVersioningReq(bucketName, 'Suspended');

function generateString(number) {
    let word = '';
    for (let i = 0; i < number; i++) {
        word = `${word}w`;
    }
    return word;
}

function testAuth(bucketOwner, authUser, bucketPutReq, log, cb) {
    bucketPut(bucketOwner, bucketPutReq, log, () => {
        bucketPutACL(bucketOwner, testPutBucketRequest, log, err => {
            assert.strictEqual(err, undefined);
            objectPut(authUser, testPutObjectRequest, undefined,
                log, (err, resHeaders) => {
                    assert.strictEqual(err, null);
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    cb();
                });
        });
    });
}

describe('parseTagFromQuery', () => {
    const invalidArgument = { status: 'InvalidArgument', statusCode: 400 };
    const invalidTag = { status: 'InvalidTag', statusCode: 400 };
    const allowedChar = '+- =._:/';
    const tests = [
        { tagging: 'key1=value1', result: { key1: 'value1' } },
        { tagging: `key1=${encodeURIComponent(allowedChar)}`,
        result: { key1: allowedChar } },
        { tagging: 'key1=value1=value2', error: invalidArgument },
        { tagging: '=value1', error: invalidArgument },
        { tagging: 'key1%=value1', error: invalidArgument },
        { tagging: `${generateString(129)}=value1`, error: invalidTag },
        { tagging: `key1=${generateString(257)}`, error: invalidTag },
        { tagging: `${generateString(129)}=value1`, error: invalidTag },
        { tagging: `key1=${generateString(257)}`, error: invalidTag },
        { tagging: 'key1#=value1', error: invalidTag },
    ];
    tests.forEach(test => {
        const behavior = test.error ? 'fail' : 'pass';
        it(`should ${behavior} if tag set: "${test.tagging}"`, done => {
            const result = parseTagFromQuery(test.tagging);
            if (test.error) {
                assert(result[test.error.status]);
                assert.strictEqual(result.code, test.error.statusCode);
            } else {
                assert.deepStrictEqual(result, test.result);
            }
            done();
        });
    });
});

describe('objectPut API', () => {
    beforeEach(() => {
        cleanup();
        testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            url: '/',
        }, postBody);
    });


    it('should return an error if the bucket does not exist', done => {
        objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    it('should return an error if user is not authorized', done => {
        const putAuthInfo = makeAuthInfo('accessKey2');
        bucketPut(putAuthInfo, testPutBucketRequest,
            log, () => {
                objectPut(authInfo, testPutObjectRequest,
                    undefined, log, err => {
                        assert.deepStrictEqual(err, errors.AccessDenied);
                        done();
                    });
            });
    });

    it('should put object if user has FULL_CONTROL grant on bucket', done => {
        const bucketOwner = makeAuthInfo('accessKey2');
        const authUser = makeAuthInfo('accessKey3');
        testPutBucketRequest.headers['x-amz-grant-full-control'] =
            `id=${authUser.getCanonicalID()}`;
        testAuth(bucketOwner, authUser, testPutBucketRequest, log, done);
    });

    it('should put object if user has WRITE grant on bucket', done => {
        const bucketOwner = makeAuthInfo('accessKey2');
        const authUser = makeAuthInfo('accessKey3');
        testPutBucketRequest.headers['x-amz-grant-write'] =
            `id=${authUser.getCanonicalID()}`;

        testAuth(bucketOwner, authUser, testPutBucketRequest, log, done);
    });

    it('should put object in bucket with public-read-write acl', done => {
        const bucketOwner = makeAuthInfo('accessKey2');
        const authUser = makeAuthInfo('accessKey3');
        testPutBucketRequest.headers['x-amz-acl'] = 'public-read-write';

        testAuth(bucketOwner, authUser, testPutBucketRequest, log, done);
    });

    it('should successfully put an object', done => {
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
            calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
        }, postBody);

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    metadata.getObjectMD(bucketName, objectName,
                        {}, log, (err, md) => {
                            assert(md);
                            assert
                            .strictEqual(md['content-md5'], correctMD5);
                            done();
                        });
                });
        });
    });

    it('should successfully put an object with user metadata', done => {
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                // Note that Node will collapse common headers into one
                // (e.g. "x-amz-meta-test: hi" and "x-amz-meta-test:
                // there" becomes "x-amz-meta-test: hi, there")
                // Here we are not going through an actual http
                // request so will not collapse properly.
                'x-amz-meta-test': 'some metadata',
                'x-amz-meta-test2': 'some more metadata',
                'x-amz-meta-test3': 'even more metadata',
            },
            url: `/${bucketName}/${objectName}`,
            calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
        }, postBody);

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    metadata.getObjectMD(bucketName, objectName, {}, log,
                        (err, md) => {
                            assert(md);
                            assert.strictEqual(md['x-amz-meta-test'],
                                        'some metadata');
                            assert.strictEqual(md['x-amz-meta-test2'],
                                        'some more metadata');
                            assert.strictEqual(md['x-amz-meta-test3'],
                                        'even more metadata');
                            done();
                        });
                });
        });
    });

    it('should put an object with user metadata but no data', done => {
        const postBody = '';
        const correctMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
        const testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'content-length': '0',
                'x-amz-meta-test': 'some metadata',
                'x-amz-meta-test2': 'some more metadata',
                'x-amz-meta-test3': 'even more metadata',
            },
            parsedContentLength: 0,
            url: `/${bucketName}/${objectName}`,
            calculatedHash: 'd41d8cd98f00b204e9800998ecf8427e',
        }, postBody);

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    assert.deepStrictEqual(ds, []);
                    metadata.getObjectMD(bucketName, objectName, {}, log,
                        (err, md) => {
                            assert(md);
                            assert.strictEqual(md.location, null);
                            assert.strictEqual(md['x-amz-meta-test'],
                                        'some metadata');
                            assert.strictEqual(md['x-amz-meta-test2'],
                                       'some more metadata');
                            assert.strictEqual(md['x-amz-meta-test3'],
                                       'even more metadata');
                            done();
                        });
                });
        });
    });

    it('should not leave orphans in data when overwriting an object', done => {
        const testPutObjectRequest2 = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        }, Buffer.from('I am another body', 'utf8'));

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest,
                undefined, log, () => {
                    objectPut(authInfo, testPutObjectRequest2, undefined,
                        log,
                    () => {
                        // orphan objects don't get deleted
                        // until the next tick
                        // in memory
                        process.nextTick(() => {
                            // Data store starts at index 1
                            assert.strictEqual(ds[0], undefined);
                            assert.strictEqual(ds[1], undefined);
                            assert.deepStrictEqual(ds[2].value,
                                Buffer.from('I am another body', 'utf8'));
                            done();
                        });
                    });
                });
        });
    });
});

describe('objectPut API with versioning', () => {
    beforeEach(() => {
        cleanup();
    });

    const objData = ['foo0', 'foo1', 'foo2'].map(str =>
        Buffer.from(str, 'utf8'));
    const testPutObjectRequests = objData.map(data => versioningTestUtils
        .createPutObjectRequest(bucketName, objectName, data));

    it('should delete latest version when creating new null version ' +
    'if latest version is null version', done => {
        async.series([
            callback => bucketPut(authInfo, testPutBucketRequest, log,
                callback),
            // putting null version by putting obj before versioning configured
            callback => objectPut(authInfo, testPutObjectRequests[0], undefined,
                log, err => {
                    versioningTestUtils.assertDataStoreValues(ds, [objData[0]]);
                    callback(err);
                }),
            callback => bucketPutVersioning(authInfo, suspendVersioningRequest,
                log, callback),
            // creating new null version by putting obj after ver suspended
            callback => objectPut(authInfo, testPutObjectRequests[1],
                undefined, log, err => {
                    // wait until next tick since mem backend executes
                    // deletes in the next tick
                    process.nextTick(() => {
                        // old null version should be deleted
                        versioningTestUtils.assertDataStoreValues(ds,
                            [undefined, objData[1]]);
                        callback(err);
                    });
                }),
            // create another null version
            callback => objectPut(authInfo, testPutObjectRequests[2],
                undefined, log, err => {
                    process.nextTick(() => {
                        // old null version should be deleted
                        versioningTestUtils.assertDataStoreValues(ds,
                            [undefined, undefined, objData[2]]);
                        callback(err);
                    });
                }),
        ], done);
    });

    describe('when null version is not the latest version', () => {
        const objData = ['foo0', 'foo1', 'foo2'].map(str =>
            Buffer.from(str, 'utf8'));
        const testPutObjectRequests = objData.map(data => versioningTestUtils
            .createPutObjectRequest(bucketName, objectName, data));
        beforeEach(done => {
            async.series([
                callback => bucketPut(authInfo, testPutBucketRequest, log,
                    callback),
                // putting null version: put obj before versioning configured
                callback => objectPut(authInfo, testPutObjectRequests[0],
                    undefined, log, callback),
                callback => bucketPutVersioning(authInfo,
                    enableVersioningRequest, log, callback),
                // put another version:
                callback => objectPut(authInfo, testPutObjectRequests[1],
                    undefined, log, callback),
                callback => bucketPutVersioning(authInfo,
                    suspendVersioningRequest, log, callback),
            ], err => {
                if (err) {
                    return done(err);
                }
                versioningTestUtils.assertDataStoreValues(ds,
                    objData.slice(0, 2));
                return done();
            });
        });

        it('should still delete null version when creating new null version',
        done => {
            objectPut(authInfo, testPutObjectRequests[2], undefined,
                log, err => {
                    assert.ifError(err, `Unexpected err: ${err}`);
                    process.nextTick(() => {
                        // old null version should be deleted after putting
                        // new null version
                        versioningTestUtils.assertDataStoreValues(ds,
                            [undefined, objData[1], objData[2]]);
                        done(err);
                    });
                });
        });
    });
});
