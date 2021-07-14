const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const { versioning } = require('arsenal');
const versionIdUtils = versioning.VersionID;

const { makeRequest } = require('../../utils/makeRequest');
const BucketUtility = require('../../../aws-node-sdk/lib/utility/bucket-util');

const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';
const describeSkipIfAWS = process.env.AWS_ON_AIR ? describe.skip : describe;

const backbeatAuthCredentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

const TEST_BUCKET = 'backbeatbucket';
const TEST_ENCRYPTED_BUCKET = 'backbeatbucket-encrypted';
const TEST_KEY = 'fookey';
const NONVERSIONED_BUCKET = 'backbeatbucket-non-versioned';

const testArn = 'aws::iam:123456789012:user/bart';
const testKey = 'testkey';
const testKeyUTF8 = '䆩鈁櫨㟔罳';
const testData = 'testkey data';
const testDataMd5 = crypto.createHash('md5')
          .update(testData, 'utf-8')
          .digest('hex');
const testMd = {
    'md-model-version': 2,
    'owner-display-name': 'Bart',
    'owner-id': ('79a59df900b949e55d96a1e698fbaced' +
                 'fd6e09d98eacf8f8d5218e7cd47ef2be'),
    'last-modified': '2017-05-15T20:32:40.032Z',
    'content-length': testData.length,
    'content-md5': testDataMd5,
    'x-amz-server-version-id': '',
    'x-amz-storage-class': 'STANDARD',
    'x-amz-server-side-encryption': '',
    'x-amz-server-side-encryption-aws-kms-key-id': '',
    'x-amz-server-side-encryption-customer-algorithm': '',
    'location': null,
    'acl': {
        Canned: 'private',
        FULL_CONTROL: [],
        WRITE_ACP: [],
        READ: [],
        READ_ACP: [],
    },
    'nullVersionId': '99999999999999999999RG001  ',
    'isDeleteMarker': false,
    'versionId': '98505119639965999999RG001  9',
    'replicationInfo': {
        status: 'COMPLETED',
        backends: [{ site: 'zenko', status: 'PENDING' }],
        content: ['DATA', 'METADATA'],
        destination: 'arn:aws:s3:::dummy-dest-bucket',
        storageClass: 'STANDARD',
    },
};

/** makeBackbeatRequest - utility function to generate a request going
 * through backbeat route
 * @param {object} params - params for making request
 * @param {string} params.method - request method
 * @param {string} params.bucket - bucket name
 * @param {string} params.objectKey - object key
 * @param {string} params.subCommand - subcommand to backbeat
 * @param {object} [params.headers] - headers and their string values
 * @param {object} [params.authCredentials] - authentication credentials
 * @param {object} params.authCredentials.accessKey - access key
 * @param {object} params.authCredentials.secretKey - secret key
 * @param {string} [params.requestBody] - request body contents
 * @param {object} [params.queryObj] - query params
 * @param {function} callback - with error and response parameters
 * @return {undefined} - and call callback
 */
function makeBackbeatRequest(params, callback) {
    const { method, headers, bucket, objectKey, resourceType,
            authCredentials, requestBody, queryObj } = params;
    const options = {
        authCredentials,
        hostname: ipAddress,
        port: 8000,
        method,
        headers,
        path: `/_/backbeat/${resourceType}/${bucket}/${objectKey}`,
        requestBody,
        jsonResponse: true,
        queryObj,
    };
    makeRequest(options, callback);
}

function getMetadataToPut(putDataResponse) {
    const mdToPut = Object.assign({}, testMd);
    // Reproduce what backbeat does to update target metadata
    mdToPut.location = JSON.parse(putDataResponse.body);
    ['x-amz-server-side-encryption',
     'x-amz-server-side-encryption-aws-kms-key-id',
     'x-amz-server-side-encryption-customer-algorithm'].forEach(headerName => {
         if (putDataResponse.headers[headerName]) {
             mdToPut[headerName] = putDataResponse.headers[headerName];
         }
     });
    return mdToPut;
}

describeSkipIfAWS('backbeat routes', () => {
    let bucketUtil;
    let s3;

    before(done => {
        bucketUtil = new BucketUtility(
            'default', { signatureVersion: 'v4' });
        s3 = bucketUtil.s3;
        s3.createBucket({ Bucket: TEST_BUCKET }).promise()
            .then(() => s3.putBucketVersioning(
                {
                    Bucket: TEST_BUCKET,
                    VersioningConfiguration: { Status: 'Enabled' },
                }).promise())
            .then(() => s3.createBucket({
                Bucket: NONVERSIONED_BUCKET,
            }).promise())
            .then(() => s3.createBucket({ Bucket: TEST_ENCRYPTED_BUCKET }).promise())
            .then(() => s3.putBucketVersioning(
                {
                    Bucket: TEST_ENCRYPTED_BUCKET,
                    VersioningConfiguration: { Status: 'Enabled' },
                }).promise())
            .then(() => s3.putBucketEncryption(
                {
                    Bucket: TEST_ENCRYPTED_BUCKET,
                    ServerSideEncryptionConfiguration: {
                        Rules: [
                            {
                                ApplyServerSideEncryptionByDefault: {
                                    SSEAlgorithm: 'AES256',
                                },
                            },
                        ],
                    },
                }).promise())
            .then(() => done())
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
    });
    after(done => {
        bucketUtil.empty(TEST_BUCKET)
            .then(() => s3.deleteBucket({ Bucket: TEST_BUCKET }).promise())
            .then(() => bucketUtil.empty(TEST_ENCRYPTED_BUCKET))
            .then(() => s3.deleteBucket({ Bucket: TEST_ENCRYPTED_BUCKET }).promise())
            .then(() =>
                s3.deleteBucket({ Bucket: NONVERSIONED_BUCKET }).promise())
            .then(() => done());
    });

    describe('backbeat PUT routes', () => {
        describe('PUT data + metadata should create a new complete object',
        () => {
            [{
                caption: 'with ascii test key',
                key: testKey, encodedKey: testKey,
            },
            {
                caption: 'with UTF8 key',
                key: testKeyUTF8, encodedKey: encodeURI(testKeyUTF8),
            },
            {
                caption: 'with percents and spaces encoded as \'+\' in key',
                key: '50% full or 50% empty',
                encodedKey: '50%25+full+or+50%25+empty',
            },
            {
                caption: 'with legacy API v1',
                key: testKey, encodedKey: testKey,
                legacyAPI: true,
            },
            {
                caption: 'with encryption configuration',
                key: testKey, encodedKey: testKey,
                encryption: true,
            },
            {
                caption: 'with encryption configuration and legacy API v1',
                key: testKey, encodedKey: testKey,
                encryption: true,
                legacyAPI: true,
            }].concat([
                `${testKeyUTF8}/${testKeyUTF8}/%42/mykey`,
                'Pâtisserie=中文-español-English',
                'notes/spring/1.txt',
                'notes/spring/2.txt',
                'notes/spring/march/1.txt',
                'notes/summer/1.txt',
                'notes/summer/2.txt',
                'notes/summer/august/1.txt',
                'notes/year.txt',
                'notes/yore.rs',
                'notes/zaphod/Beeblebrox.txt',
            ].map(key => ({
                key, encodedKey: encodeURI(key),
                caption: `with key ${key}`,
            })))
            .forEach(testCase => {
                it(testCase.caption, done => {
                    async.waterfall([next => {
                        const queryObj = testCase.legacyAPI ? {} : { v2: '' };
                        makeBackbeatRequest({
                            method: 'PUT', bucket: testCase.encryption ?
                                TEST_ENCRYPTED_BUCKET : TEST_BUCKET,
                            objectKey: testCase.encodedKey,
                            resourceType: 'data',
                            queryObj,
                            headers: {
                                'content-length': testData.length,
                                'content-md5': testDataMd5,
                                'x-scal-canonical-id': testArn,
                            },
                            authCredentials: backbeatAuthCredentials,
                            requestBody: testData }, next);
                    }, (response, next) => {
                        assert.strictEqual(response.statusCode, 200);
                        const newMd = getMetadataToPut(response);
                        if (testCase.encryption && !testCase.legacyAPI) {
                            assert.strictEqual(typeof newMd.location[0].cryptoScheme, 'number');
                            assert.strictEqual(typeof newMd.location[0].cipheredDataKey, 'string');
                        } else {
                            // if no encryption or legacy API, data should not be encrypted
                            assert.strictEqual(newMd.location[0].cryptoScheme, undefined);
                            assert.strictEqual(newMd.location[0].cipheredDataKey, undefined);
                        }
                        makeBackbeatRequest({
                            method: 'PUT', bucket: testCase.encryption ?
                                TEST_ENCRYPTED_BUCKET : TEST_BUCKET,
                            objectKey: testCase.encodedKey,
                            resourceType: 'metadata',
                            queryObj: {
                                versionId: versionIdUtils.encode(
                                    testMd.versionId),
                            },
                            authCredentials: backbeatAuthCredentials,
                            requestBody: JSON.stringify(newMd),
                        }, next);
                    }, (response, next) => {
                        assert.strictEqual(response.statusCode, 200);
                        s3.getObject({
                            Bucket: testCase.encryption ?
                                TEST_ENCRYPTED_BUCKET : TEST_BUCKET,
                            Key: testCase.key,
                        }, (err, data) => {
                            assert.ifError(err);
                            assert.strictEqual(data.Body.toString(), testData);
                            next();
                        });
                    }], err => {
                        assert.ifError(err);
                        done();
                    });
                });
            });
        });

        it('PUT metadata with "x-scal-replication-content: METADATA"' +
        'header should replicate metadata only', done => {
            async.waterfall([next => {
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_ENCRYPTED_BUCKET,
                    objectKey: 'test-updatemd-key',
                    resourceType: 'data',
                    queryObj: { v2: '' },
                    headers: {
                        'content-length': testData.length,
                        'content-md5': testDataMd5,
                        'x-scal-canonical-id': testArn,
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: testData,
                }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                const newMd = getMetadataToPut(response);
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_ENCRYPTED_BUCKET,
                    objectKey: 'test-updatemd-key',
                    resourceType: 'metadata',
                    queryObj: {
                        versionId: versionIdUtils.encode(testMd.versionId),
                    },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: JSON.stringify(newMd),
                }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                // Don't update the sent metadata since it is sent by
                // backbeat as received from the replication queue,
                // without updated data location or encryption info
                // (since that info is not known by backbeat)
                const newMd = Object.assign({}, testMd);
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_ENCRYPTED_BUCKET,
                    objectKey: 'test-updatemd-key',
                    resourceType: 'metadata',
                    queryObj: {
                        versionId: versionIdUtils.encode(testMd.versionId),
                    },
                    headers: { 'x-scal-replication-content': 'METADATA' },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: JSON.stringify(newMd),
                }, next);
            }, (response, next) => {
                assert.strictEqual(response.statusCode, 200);
                s3.getObject({
                    Bucket: TEST_ENCRYPTED_BUCKET,
                    Key: 'test-updatemd-key',
                }, (err, data) => {
                    assert.ifError(err);
                    assert.strictEqual(data.Body.toString(), testData);
                    next();
                });
            }], err => {
                assert.ifError(err);
                done();
            });
        });

        it('should refuse PUT data if bucket is not versioned',
        done => makeBackbeatRequest({
            method: 'PUT', bucket: NONVERSIONED_BUCKET,
            objectKey: testKey, resourceType: 'data',
            queryObj: { v2: '' },
            headers: {
                'content-length': testData.length,
                'content-md5': testDataMd5,
                'x-scal-canonical-id': testArn,
            },
            authCredentials: backbeatAuthCredentials,
            requestBody: testData,
        },
        err => {
            assert.strictEqual(err.code, 'InvalidBucketState');
            done();
        }));

        it('should refuse PUT metadata if bucket is not versioned',
        done => makeBackbeatRequest({
            method: 'PUT', bucket: NONVERSIONED_BUCKET,
            objectKey: testKey, resourceType: 'metadata',
            queryObj: {
                versionId: versionIdUtils.encode(testMd.versionId),
            },
            authCredentials: backbeatAuthCredentials,
            requestBody: JSON.stringify(testMd),
        },
        err => {
            assert.strictEqual(err.code, 'InvalidBucketState');
            done();
        }));

        it('should refuse PUT data if no x-scal-canonical-id header ' +
           'is provided', done => makeBackbeatRequest({
               method: 'PUT', bucket: TEST_BUCKET,
               objectKey: testKey, resourceType: 'data',
               queryObj: { v2: '' },
               headers: {
                   'content-length': testData.length,
                   'content-md5': testDataMd5,
               },
               authCredentials: backbeatAuthCredentials,
               requestBody: testData,
           },
           err => {
               assert.strictEqual(err.code, 'BadRequest');
               done();
           }));

        it('should refuse PUT data if no content-md5 header is provided',
        done => makeBackbeatRequest({
            method: 'PUT', bucket: TEST_BUCKET,
            objectKey: testKey, resourceType: 'data',
            queryObj: { v2: '' },
            headers: {
                'content-length': testData.length,
                'x-scal-canonical-id': testArn,
            },
            authCredentials: backbeatAuthCredentials,
            requestBody: testData,
        },
        err => {
            assert.strictEqual(err.code, 'BadRequest');
            done();
        }));

        it('should refuse PUT in metadata-only mode if object does not exist',
        done => {
            async.waterfall([next => {
                const newMd = Object.assign({}, testMd);
                makeBackbeatRequest({
                    method: 'PUT', bucket: TEST_BUCKET,
                    objectKey: 'does-not-exist',
                    resourceType: 'metadata',
                    queryObj: {
                        versionId: versionIdUtils.encode(testMd.versionId),
                    },
                    headers: { 'x-scal-replication-content': 'METADATA' },
                    authCredentials: backbeatAuthCredentials,
                    requestBody: JSON.stringify(newMd),
                }, next);
            }], err => {
                assert.strictEqual(err.statusCode, 404);
                done();
            });
        });
    });
    describe('backbeat authorization checks', () => {
        [{ method: 'PUT', resourceType: 'metadata' },
         { method: 'PUT', resourceType: 'data' }].forEach(test => {
             const queryObj = test.resourceType === 'data' ? { v2: '' } : {};
             it(`${test.method} ${test.resourceType} should respond with ` +
             '403 Forbidden if no credentials are provided',
             done => {
                 makeBackbeatRequest({
                     method: test.method, bucket: TEST_BUCKET,
                     objectKey: TEST_KEY, resourceType: test.resourceType,
                     queryObj,
                 },
                 err => {
                     assert(err);
                     assert.strictEqual(err.statusCode, 403);
                     assert.strictEqual(err.code, 'AccessDenied');
                     done();
                 });
             });
             it(`${test.method} ${test.resourceType} should respond with ` +
                '403 Forbidden if wrong credentials are provided',
                done => {
                    makeBackbeatRequest({
                        method: test.method, bucket: TEST_BUCKET,
                        objectKey: TEST_KEY, resourceType: test.resourceType,
                        queryObj,
                        authCredentials: {
                            accessKey: 'wrong',
                            secretKey: 'still wrong',
                        },
                    },
                    err => {
                        assert(err);
                        assert.strictEqual(err.statusCode, 403);
                        assert.strictEqual(err.code, 'InvalidAccessKeyId');
                        done();
                    });
                });
             it(`${test.method} ${test.resourceType} should respond with ` +
                '403 Forbidden if the account does not match the ' +
                'backbeat user',
                done => {
                    makeBackbeatRequest({
                        method: test.method, bucket: TEST_BUCKET,
                        objectKey: TEST_KEY, resourceType: test.resourceType,
                        queryObj,
                        authCredentials: {
                            accessKey: 'accessKey2',
                            secretKey: 'verySecretKey2',
                        },
                    },
                    err => {
                        assert(err);
                        assert.strictEqual(err.statusCode, 403);
                        assert.strictEqual(err.code, 'AccessDenied');
                        done();
                    });
                });
             it(`${test.method} ${test.resourceType} should respond with ` +
                '403 Forbidden if backbeat user has wrong secret key',
                done => {
                    makeBackbeatRequest({
                        method: test.method, bucket: TEST_BUCKET,
                        objectKey: TEST_KEY, resourceType: test.resourceType,
                        queryObj,
                        authCredentials: {
                            accessKey: backbeatAuthCredentials.accessKey,
                            secretKey: 'hastalavista',
                        },
                    },
                    err => {
                        assert(err);
                        assert.strictEqual(err.statusCode, 403);
                        assert.strictEqual(err.code, 'SignatureDoesNotMatch');
                        done();
                    });
                });
         });
    });

    describe('GET Metadata route', () => {
        beforeEach(done => makeBackbeatRequest({
            method: 'PUT', bucket: TEST_BUCKET,
            objectKey: TEST_KEY,
            resourceType: 'metadata',
            authCredentials: backbeatAuthCredentials,
            requestBody: JSON.stringify(testMd),
        }, done));

        it('should return metadata blob for a versionId', done => {
            makeBackbeatRequest({
                method: 'GET', bucket: TEST_BUCKET,
                objectKey: TEST_KEY, resourceType: 'metadata',
                authCredentials: backbeatAuthCredentials,
                queryObj: {
                    versionId: versionIdUtils.encode(testMd.versionId),
                },
            }, (err, data) => {
                const parsedBody = JSON.parse(JSON.parse(data.body).Body);
                assert.strictEqual(data.statusCode, 200);
                assert.deepStrictEqual(parsedBody, testMd);
                done();
            });
        });

        it('should return error if bucket does not exist', done => {
            makeBackbeatRequest({
                method: 'GET', bucket: 'blah',
                objectKey: TEST_KEY, resourceType: 'metadata',
                authCredentials: backbeatAuthCredentials,
                queryObj: {
                    versionId: versionIdUtils.encode(testMd.versionId),
                },
            }, (err, data) => {
                assert.strictEqual(data.statusCode, 404);
                assert.strictEqual(JSON.parse(data.body).code, 'NoSuchBucket');
                done();
            });
        });

        it('should return error if object does not exist', done => {
            makeBackbeatRequest({
                method: 'GET', bucket: TEST_BUCKET,
                objectKey: 'blah', resourceType: 'metadata',
                authCredentials: backbeatAuthCredentials,
                queryObj: {
                    versionId: versionIdUtils.encode(testMd.versionId),
                },
            }, (err, data) => {
                assert.strictEqual(data.statusCode, 404);
                assert.strictEqual(JSON.parse(data.body).code, 'ObjNotFound');
                done();
            });
        });
    });
    describe('Batch Delete Route', () => {
        it('should batch delete a location', done => {
            let versionId;
            let location;

            async.series([
                done => s3.putObject({
                    Bucket: TEST_BUCKET,
                    Key: 'batch-delete-test-key',
                    Body: new Buffer('hello'),
                }, done),
                done => s3.getObject({
                    Bucket: TEST_BUCKET,
                    Key: 'batch-delete-test-key',
                }, (err, data) => {
                    assert.ifError(err);
                    assert.strictEqual(data.Body.toString(), 'hello');
                    versionId = data.VersionId;
                    done();
                }),
                done => {
                    makeBackbeatRequest({
                        method: 'GET', bucket: TEST_BUCKET,
                        objectKey: 'batch-delete-test-key',
                        resourceType: 'metadata',
                        authCredentials: backbeatAuthCredentials,
                        queryObj: {
                            versionId,
                        },
                    }, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data.statusCode, 200);
                        const metadata = JSON.parse(
                            JSON.parse(data.body).Body);
                        location = metadata.location;
                        done();
                    });
                },
                done => {
                    const options = {
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path: '/_/backbeat/batchdelete',
                        requestBody:
                        `{"Locations":${JSON.stringify(location)}}`,
                        jsonResponse: true,
                    };
                    makeRequest(options, done);
                },
                done => s3.getObject({
                    Bucket: TEST_BUCKET,
                    Key: 'batch-delete-test-key',
                }, err => {
                    // should error out as location shall no longer exist
                    assert(err);
                    done();
                }),
            ], done);
        });
        it('should fail with error if given malformed JSON', done => {
            async.series([
                done => {
                    const options = {
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path: '/_/backbeat/batchdelete',
                        requestBody: 'NOTJSON',
                        jsonResponse: true,
                    };
                    makeRequest(options, done);
                },
            ], err => {
                assert(err);
                done();
            });
        });
        it('should skip batch delete of a non-existent location', done => {
            async.series([
                done => {
                    const options = {
                        authCredentials: backbeatAuthCredentials,
                        hostname: ipAddress,
                        port: 8000,
                        method: 'POST',
                        path: '/_/backbeat/batchdelete',
                        requestBody:
                        '{"Locations":' +
                            '[{"key":"abcdef","dataStoreName":"us-east-1"}]}',
                        jsonResponse: true,
                    };
                    makeRequest(options, done);
                },
            ], done);
        });
    });
});
