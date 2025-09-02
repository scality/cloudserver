const assert = require('assert');
const sinon = require('sinon');
const async = require('async');
const http = require('http');
const { promisify } = require('util');
const metadataUtils = require('../../../lib/metadata/metadataUtils');
const storeObject = require('../../../lib/api/apiUtils/object/storeObject');
const metadata = require('../../../lib/metadata/wrapper');
const { routeBackbeat } = require('../../../lib/routes/routeBackbeat');
const { DummyRequestLogger, versioningTestUtils, makeAuthInfo } = require('../helpers');
const dataWrapper = require('../../../lib/data/wrapper');
const DummyRequest = require('../DummyRequest');
const { auth, errors } = require('arsenal');
const AuthInfo = auth.AuthInfo;
const { config } = require('../../../lib/Config');
const quotaUtils = require('../../../lib/api/apiUtils/quotas/quotaUtils');
const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketDelete = require('../../../lib/api/bucketDelete');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const objectPut = require('../../../lib/api/objectPut');
const { objectDelete } = require('../../../lib/api/objectDelete');
const bucketPutPolicy = require('../../../lib/api/bucketPutPolicy');

const log = new DummyRequestLogger();

function prepareDummyRequest(headers = {}, body = '') {
    const request = new DummyRequest({
        hostname: 'localhost',
        method: 'PUT',
        url: '/_/backbeat/metadata/bucket0/key0',
        port: 80,
        headers,
        socket: {
            remoteAddress: '0.0.0.0',
            destroy: () => {},
            on: () => {},
            removeListener: () => {},
        },
    }, body || '{"replicationInfo":"{}"}');
    return request;
}

describe('routeBackbeat', () => {
    let mockResponse;
    let mockRequest;
    let sandbox;
    let endPromise;
    let resolveEnd;
    let routeBackbeat;

    beforeEach(() => {
        sandbox = sinon.createSandbox();

        // create a Promise that resolves when response.end is called
        endPromise = new Promise(resolve => { resolveEnd = resolve; });

        mockResponse = {
            statusCode: null,
            body: null,
            setHeader: () => {},
            writeHead: sandbox.spy(statusCode => {
                mockResponse.statusCode = statusCode;
            }),
            end: sandbox.spy((body, encoding, callback) => {
                mockResponse.body = JSON.parse(body);
                if (callback) {
                    callback();
                }
                resolveEnd(); // Resolve the Promise when end is called
            }),
        };

        mockRequest = prepareDummyRequest();

        sandbox.stub(metadataUtils, 'standardMetadataValidateBucketAndObj');
        sandbox.stub(storeObject, 'dataStore');

        // Clear require cache for routeBackbeat to make sure fresh module with stubbed dependencies
        delete require.cache[require.resolve('../../../lib/routes/routeBackbeat')];
        routeBackbeat = require('../../../lib/routes/routeBackbeat').routeBackbeat;
    });

    afterEach(() => {
        sandbox.restore();
    });

    const rejectionTests = [
        {
            description: 'should reject CRR destination (putData) requests when versioning is disabled',
            method: 'PUT',
            url: '/_/backbeat/data/bucket0/key0',
        },
        {
            description: 'should reject CRR destination (putMetadata) requests when versioning is disabled',
            method: 'PUT',
            url: '/_/backbeat/metadata/bucket0/key0',
        },
    ];

    rejectionTests.forEach(({ description, method, url }) => {
        it(description, async () => {
            mockRequest.method = method;
            mockRequest.url = url;
            mockRequest.headers = {
                'x-scal-versioning-required': 'true',
            };
            metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
                const bucketInfo = {
                    getVersioningConfiguration: () => ({ Status: 'Disabled' }),
                };
                const objMd = {};
                callback(null, bucketInfo, objMd);
            });

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

            void await endPromise;

            assert.strictEqual(mockResponse.statusCode, 409);
            assert.strictEqual(mockResponse.body.code, 'InvalidBucketState');
        });
    });

    it('should allow non-CRR destination (getMetadata) requests regardless of versioning', async () => {
        mockRequest.method = 'GET';

        metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
            const bucketInfo = {
                getVersioningConfiguration: () => ({ Status: 'Disabled' }),
            };
            const objMd = {};
            callback(null, bucketInfo, objMd);
        });

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

        void await endPromise;

        assert.strictEqual(mockResponse.statusCode, 200);
        assert.deepStrictEqual(mockResponse.body, { Body: '{}' });
    });

    it('should allow CRR destination requests (putData) when versioning is enabled', async () => {
        const md5 = '1234';
        mockRequest.method = 'PUT';
        mockRequest.url = '/_/backbeat/data/bucket0/key0';
        mockRequest.headers = {
            'x-scal-canonical-id': 'id',
            'content-md5': md5,
            'content-length': '0',
            'x-scal-versioning-required': 'true',
        };
        mockRequest.destroy = () => {};

        metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
            const bucketInfo = {
                getVersioningConfiguration: () => ({ Status: 'Enabled' }),
                isVersioningEnabled: () => true,
                getLocationConstraint: () => undefined,
            };
            const objMd = {};
            callback(null, bucketInfo, objMd);
        });
        storeObject.dataStore.callsFake((objectContext, cipherBundle, stream, size,
            streamingV4Params, backendInfo, log, callback) => {
            callback(null, {}, md5);
        });

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

        void await endPromise;

        assert.strictEqual(mockResponse.statusCode, 200);
        assert.deepStrictEqual(mockResponse.body, [{}]);
    });

    describe('putMetadata', () => {
        const bucketInfo = {
            getVersioningConfiguration: () => ({ Status: 'Enabled' }),
            isVersioningEnabled: () => true,
        };
        let dataDeleteSpy;

        function preparePutMetadataRequest(body = {}) {
            const req = prepareDummyRequest({
                'x-scal-versioning-required': 'true',
            }, JSON.stringify({
                replicationInfo: {},
                ...body,
            }));
            req.method = 'PUT';
            req.url = '/_/backbeat/metadata/bucket0/key0';
            req.destroy = () => { };
            return req;
        }

        beforeEach(() => {
            mockRequest = preparePutMetadataRequest();
            dataDeleteSpy = sandbox.stub(dataWrapper.data, 'delete').callsFake((loc, log, cb) => cb(null));

            // Setup default version enabled bucket and empty object metadata
            metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
                callback(null, bucketInfo, {});
            });
        });

        it('should allow CRR destination requests (putMetadata) when versioning is enabled', async () => {
            sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
                cb(null, {});
            });

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;
            assert.strictEqual(mockResponse.statusCode, 200);
            assert.deepStrictEqual(mockResponse.body, {});
        });

        it('should put non versioned metadata', async () => {
            sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
                cb(null, {});
            });

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;

            assert.strictEqual(mockResponse.statusCode, 200);
            assert.deepStrictEqual(mockResponse.body, {});
        });

        it('should put metadata after updating account info', async () => {
            mockRequest.url = '/_/backbeat/metadata/bucket0/key0?accountId=123456789012';
            const putObjectMDStub = sandbox.stub(metadata, 'putObjectMD');
            putObjectMDStub.onCall(0).callsFake(
                (_bucketName, _objectKey, _omVal, _options, _logParam, cb) => cb(null, {})
            );
            putObjectMDStub.onCall(1).callsFake((_bucketName, _objectKey, omVal, _options, _logParam, cb) => {
                assert.strictEqual(omVal['owner-display-name'], 'Bart');
                assert.strictEqual(omVal['owner-id'],
                    '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be');
                cb(null, {});
            });

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;

            assert.strictEqual(mockResponse.statusCode, 200);
            assert.deepStrictEqual(mockResponse.body, {});
        });

        it('should fail to put metadata when accountId is invalid', async () => {
            mockRequest.url = '/_/backbeat/metadata/bucket0/key0?accountId=invalid';

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;

            assert.strictEqual(mockResponse.statusCode, 404);
            assert.deepStrictEqual(mockResponse.body.code, 'AccountNotFound');
        });

        it('should repair master when putting metadata of a new version', async () => {
            mockRequest.url = '/_/backbeat/metadata/bucket0/key0' +
                '?accountId=123456789012&versionId=aIXVkw5Tw2Pd00000000001I4j3QKsvf';

            const putObjectMDStub = sandbox.stub(metadata, 'putObjectMD');
            putObjectMDStub.onCall(0).callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
                assert.strictEqual(options.repairMaster, undefined);
                cb(null, {});
            });
            putObjectMDStub.onCall(1).callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
                assert.strictEqual(options.repairMaster, true);
                cb(null, {});
            });

            metadataUtils.standardMetadataValidateBucketAndObj.onCall(1).callsFake((params, denies, log, callback) => {
                callback(null, bucketInfo, {});
            });

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;

            assert.strictEqual(mockResponse.statusCode, 200);
            assert.deepStrictEqual(mockResponse.body, {});
        });

        it('should not repair master when updating metadata of an existing version', async () => {
            mockRequest.url = '/_/backbeat/metadata/bucket0/key0' +
                '?accountId=123456789012&versionId=aIXVkw5Tw2Pd00000000001I4j3QKsvf';

            sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
                assert.strictEqual(options.repairMaster, undefined);
                cb(null, {});
            });

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;
            assert.strictEqual(mockResponse.statusCode, 200);
            assert.deepStrictEqual(mockResponse.body, {});
        });

        it('should handle error when putting metadata', async () => {
            const putObjectMDStub = sandbox.stub(metadata, 'putObjectMD');
            putObjectMDStub.onCall(0).callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
                cb(new Error('error'));
            });

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;

            assert.strictEqual(mockResponse.statusCode, 500);
        });

        it('should be rejected when using a wrong route', async () => {
            mockRequest.url = '/_/backbeat/metadata/bucket0';

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;
            assert.strictEqual(mockResponse.statusCode, 405);
        });

        it('should delete data when replacing with empty object', async () => {
            const existingMd = {
                location: [{
                    key: 'key0',
                    dataStoreName: 'location1',
                    size: 100,
                }],
            };
            metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
                callback(null, bucketInfo, existingMd);
            });

            const putObjectMDStub = sandbox.stub(metadata, 'putObjectMD');
            putObjectMDStub.onCall(0).callsFake(
                (_bucketName, _objectKey, _omVal, _options, _logParam, cb) => cb(null, {})
            );
            putObjectMDStub.onCall(1).callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
                assert.deepStrictEqual(omVal.location, undefined);
                cb(null, {});
            });

            mockRequest = preparePutMetadataRequest({
                'content-length': 0,
            });
            mockRequest.url += '?versionId=aIXVkw5Tw2Pd00000000001I4j3QKsvf';

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;

            assert.strictEqual(mockResponse.statusCode, 200);
            assert.deepStrictEqual(mockResponse.body, {});
            assert.strictEqual(dataDeleteSpy.calledOnce, true);
            sinon.assert.calledWith(dataDeleteSpy, existingMd.location[0]);
        });

        it('should preserve existing locations when x-scal-replication-content is METADATA', async () => {
            const existingLocations = [{
                key: 'key0',
                dataStoreName: 'location1',
                size: 100,
            }];
            metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
                callback(null, bucketInfo, {
                    location: existingLocations,
                    'x-amz-server-side-encryption': 'AES256',
                });
            });

            sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
                // Verify that original locations and encryption info are preserved
                assert.deepStrictEqual(omVal.location, existingLocations);
                assert.strictEqual(omVal['x-amz-server-side-encryption'], 'AES256');
                cb(null, {});
            });

            // New metadata without location info
            mockRequest = preparePutMetadataRequest({
                tags: { tag1: 'value1' },
            });
            mockRequest.headers = {
                'x-scal-versioning-required': 'true',
                'x-scal-replication-content': 'METADATA',
            };
            mockRequest.url += '?versionId=aIXVkw5Tw2Pd00000000001I4j3QKsvf';

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;

            assert.strictEqual(mockResponse.statusCode, 200);
            assert.deepStrictEqual(mockResponse.body, {});
            assert.strictEqual(dataDeleteSpy.called, false);
        });

        it('should delete data when no more locations', async () => {
            const existingMd = {
                location: [{
                    key: 'key0',
                    dataStoreName: 'location1',
                    size: 100,
                }],
            };
            metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
                callback(null, bucketInfo, existingMd);
            });

            const putObjectMDStub = sandbox.stub(metadata, 'putObjectMD');
            putObjectMDStub.onCall(0).callsFake(
                (_bucketName, _objectKey, _omVal, _options, _logParam, cb) => cb(null, {})
            );
            putObjectMDStub.onCall(1).callsFake((_bucketName, _objectKey, omVal, _options, _logParam, cb) => {
                // Verify that the location array is empty, indicating data deletion
                assert.deepStrictEqual(omVal.location, []);
                cb(null, {});
            });

            mockRequest = preparePutMetadataRequest({ location: [] });
            mockRequest.url += '?versionId=aIXVkw5Tw2Pd00000000001I4j3QKsvf';

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;

            assert.strictEqual(mockResponse.statusCode, 200);
            assert.deepStrictEqual(mockResponse.body, {});
            assert.strictEqual(dataDeleteSpy.calledOnce, true);
            sinon.assert.calledWith(dataDeleteSpy, existingMd.location[0]);
        });

        it('should delete data when locations change', async () => {
            const existingMd = {
                location: [{
                    key: 'key0',
                    dataStoreName: 'location1',
                    size: 100,
                }],
                'content-length': 100,
            };
            metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
                callback(null, bucketInfo, existingMd);
            });

            // New metadata has different locations
            const reqBody = {
                location: [{
                    key: 'key1',
                    dataStoreName: 'location1',
                    size: 100,
                }],
                'content-length': 100,
            };
            mockRequest = preparePutMetadataRequest(reqBody);
            mockRequest.url += '?versionId=aIXVkw5Tw2Pd00000000001I4j3QKsvf';

            const putObjectMDStub = sandbox.stub(metadata, 'putObjectMD');
            putObjectMDStub.onCall(0).callsFake(
                (_bucketName, _objectKey, _omVal, _options, _logParam, cb) => cb(null, {})
            );
            putObjectMDStub.onCall(1).callsFake((_bucketName, __objectKey, omVal, _options, _logParam, cb) => {
                // Verify that the location array contains the new location
                assert.deepStrictEqual(omVal.location, reqBody.location);
                cb(null, {});
            });

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;

            assert.strictEqual(mockResponse.statusCode, 200);
            assert.deepStrictEqual(mockResponse.body, {});
            assert.strictEqual(dataDeleteSpy.calledOnce, true);
            sinon.assert.calledWith(dataDeleteSpy, existingMd.location[0]);
        });

        it('should not delete data when some keys are still used', async () => {
            const existingMd = {
                location: [{
                    key: 'key0',
                    dataStoreName: 'location1',
                    size: 100,
                }, {
                    key: 'key1',
                    dataStoreName: 'location1',
                    size: 100,
                }],
                'content-length': 100,
            };
            metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
                callback(null, bucketInfo, existingMd);
            });

            // New metadata has different locations
            const reqBody = {
                location: [{
                    key: 'key1',
                    dataStoreName: 'location1',
                    size: 100,
                }, {
                    key: 'key2',
                    dataStoreName: 'location1',
                    size: 100,
                }],
                'content-length': 100,
            };
            mockRequest = preparePutMetadataRequest(reqBody);
            mockRequest.url += '?versionId=aIXVkw5Tw2Pd00000000001I4j3QKsvf';

            const putObjectMDStub = sandbox.stub(metadata, 'putObjectMD');
            putObjectMDStub.onCall(0).callsFake(
                (_bucketName, _objectKey, _omVal, _options, _logParam, cb) => cb(null, {})
            );
            putObjectMDStub.onCall(1).callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
                // Verify that the location array contains the new location
                assert.deepStrictEqual(omVal.location, reqBody.location);
                cb(null, {});
            });

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;

            assert.strictEqual(mockResponse.statusCode, 200);
            assert.deepStrictEqual(mockResponse.body, {});
            assert.strictEqual(dataDeleteSpy.calledOnce, false);
        });

        it('should not delete data when object is archived', async () => {
            const existingMd = {
                location: [{
                    key: 'key0',
                    dataStoreName: 'location1',
                    size: 100,
                }],
                'content-length': 100,
            };
            metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
                callback(null, bucketInfo, existingMd);
            });

            // New metadata has empty location array but keeps content-length (cold storage case)
            mockRequest = prepareDummyRequest(mockRequest.headers, JSON.stringify({
                location: undefined,
                'content-length': 100,
                replicationInfo: {},
            }));
            mockRequest.url += '?versionId=aIXVkw5Tw2Pd00000000001I4j3QKsvf';

            const putObjectMDStub = sandbox.stub(metadata, 'putObjectMD');
            putObjectMDStub.onCall(0).callsFake(
                (_bucketName, _objectKey, _omVal, _options, _logParam, cb) => cb(null, {})
            );
            putObjectMDStub.onCall(1).callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
                // Verify that the location array is empty
                assert.deepStrictEqual(omVal.location, undefined);
                // Content length should be preserved
                assert.strictEqual(omVal['content-length'], 100);
                cb(null, {});
            });

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;

            assert.strictEqual(mockResponse.statusCode, 200);
            assert.deepStrictEqual(mockResponse.body, {});
            assert.strictEqual(dataDeleteSpy.called, false);
        });
    });

    describe('batchDelete', () => {
        let doAuthStub;
        let validateQuotasSpy;

        const prepareBatchDeleteRequest = (locations = undefined) => {
            const mockRequest = prepareDummyRequest({
                'x-scal-versioning-required': 'true'
            }, JSON.stringify({
                Locations: locations || [{
                    key: 'key0',
                    bucket: 'bucket0',
                    size: 100,
                }],
            }));
            mockRequest.method = 'POST';
            mockRequest.url = '/_/backbeat/batchdelete/bucket0/key0';
            mockRequest.destroy = () => { };
            return mockRequest;
        };

        beforeEach(() => {
            validateQuotasSpy = sandbox.spy(quotaUtils, 'validateQuotas');
            doAuthStub = sandbox.stub(auth.server, 'doAuth');
            doAuthStub.callsFake((req, log, cb) => {
                cb(null, {
                    canonicalID: 'id',
                    getCanonicalID: () => 'id',
                });
            });

            // Initialize mock request with default properties
            mockRequest = prepareBatchDeleteRequest();
        });

        it('should be rejected when trying batchDelete as a public user', async () => {
            doAuthStub.reset();
            doAuthStub.callThrough();

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;
            assert.strictEqual(mockResponse.statusCode, 403);
        });

        it('should update quotas when batch deleting data', async () => {
            sandbox.stub(config, 'isQuotaEnabled').returns(true);
            const bucketMD = {
                getName: () => 'bucket0',
                getQuota: () => 0n,
            };
            sandbox.stub(metadata, 'getBucket').callsFake((bucket, log, cb) => cb(null, bucketMD));

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            await endPromise;

            sinon.assert.calledOnce(validateQuotasSpy);
            sinon.assert.calledWith(validateQuotasSpy,
                mockRequest,
                bucketMD,
                mockRequest.accountQuotas,
                ['objectDelete'],
                'objectDelete',
                -100,
                false,
                log,
                sinon.match.any,
            );

            assert.strictEqual(mockResponse.statusCode, 200);
            assert.deepStrictEqual(mockResponse.body, null);
        });

        it('should skip quota updates when no bucket provided', async () => {
            sandbox.stub(config, 'isQuotaEnabled').returns(true);
            mockRequest.url = '/_/backbeat/batchdelete';

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;

            assert(!validateQuotasSpy.called);

            assert.strictEqual(mockResponse.statusCode, 200);
            assert.deepStrictEqual(mockResponse.body, null);
        });

        it('should skip quota updates when quotas disabled', async () => {
            sandbox.stub(config, 'isQuotaEnabled').returns(false);

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;

            assert(!validateQuotasSpy.called);

            assert.strictEqual(mockResponse.statusCode, 200);
            assert.deepStrictEqual(mockResponse.body, null);
        });

        it('should skip quota updates when content length is 0', async () => {
            sandbox.stub(config, 'isQuotaEnabled').returns(true);
            mockRequest = prepareBatchDeleteRequest([{
                key: 'key0',
                bucket: 'bucket0',
                size: 0,
            }]);

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
            void await endPromise;

            assert(!validateQuotasSpy.called);

            assert.strictEqual(mockResponse.statusCode, 200);
            assert.deepStrictEqual(mockResponse.body, null);
        });

        it('should batchDelete with conditions and azure location', async () => {
            mockRequest.headers = {
                'x-scal-versioning-required': 'true',
                'if-unmodified-since': '1980-01-01T00:00:00.000Z',
                'x-scal-storage-class': 'azurebackend',
            };

            routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
        void await endPromise;
        assert.strictEqual(mockResponse.statusCode, 200);
        assert.deepStrictEqual(mockResponse.body, {});
    });

    it('should not batchDelete with conditions if "if-unmodified-since" header unset', async () => {
        mockRequest.headers = {
            'x-scal-versioning-required': 'true',
            'x-scal-storage-class': 'azurebackend',
        };

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
        void await endPromise;

        assert.strictEqual(mockResponse.statusCode, 200);
    });

    it('should batchDelete with conditions and non-azure location', async () => {
        const putRequest = prepareDummyRequest({
            'x-scal-versioning-required': 'true',
        }, JSON.stringify({
            Locations: [
                {
                    key: 'key0',
                    bucket: 'bucket0',
                    lastModified: '2020-01-01T00:00:00.000Z',
                },
            ],
        }));
        await promisify(dataWrapper.client.put)(putRequest, 91, 1, 'reqUids');

        mockRequest = prepareBatchDeleteRequest([{
            key: '1',
            bucket: 'bucket0',
        }]);
        mockRequest.headers = {
            'if-unmodified-since': '2000-01-01T00:00:00.000Z',
            'x-scal-versioning-required': 'true',
            'x-scal-storage-class': 'gcpbackend',
            'x-scal-tags': JSON.stringify({ key: 'value' }),
        };

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);
        void await endPromise;

        assert.strictEqual(mockResponse.statusCode, 200);
        assert.deepStrictEqual(mockResponse.body, null);
    });
    });

    describe('routeBackbeatAPIProxy', () => {
        let mockBackbeat;

        const request = new DummyRequest({
            method: 'POST',
            url: '/_/backbeat/api/ingestion/pause',
            socket: {
                remoteAddress: '127.0.0.1',
            },
        }, Buffer.from(''));

        beforeEach(() => {
            mockBackbeat = http.createServer((req, res) => {
                res.writeHead(200);
                res.end();
            });
            mockBackbeat.listen(config.backbeat.port);
        });

        afterEach(() => {
            mockBackbeat.close();
            sinon.restore();
        });

        it('should correctly proxy the request to the backbeat API', async () => {
            sinon.stub(auth.server, 'doAuth').yields(null, new AuthInfo({
                canonicalID: 'abcdef/lifecycle',
                accountDisplayName: 'Lifecycle Service Account',
            }), undefined, undefined, undefined);

            endPromise = new Promise(resolve => { resolveEnd = resolve; });
            const response = {
                on: sinon.stub(),
                once: sinon.stub(),
                emit: sinon.stub(),
                setHeader: sinon.stub(),
                end: sinon.stub().callsFake(() => {
                    resolveEnd();
                })
            };

            routeBackbeat('127.0.0.1', request, response, log);

            void await endPromise;

            const proxyReq = response.emit.getCall(0).args[1].req;
            assert.strictEqual(proxyReq.method, 'POST');
            assert.strictEqual(proxyReq.path, '/_/ingestion/pause');
            assert.strictEqual(proxyReq.getHeader('host'), `localhost:${config.backbeat.port}`);
        });
    });
});

describe('routeBackbeat authorization', () => {
    let endPromise;
    let resolveEnd;
    const bucketName = 'bucketname';
    const authInfo = makeAuthInfo('cannonicalID');
    const namespace = 'default';
    const objectName = 'objectName';
    const bucketPutPolicyPromise = promisify(bucketPutPolicy);

    const testBucket = {
        bucketName,
        namespace,
        headers: {
            'host': `${bucketName}.s3.amazonaws.com`,
        },
        url: `/${bucketName}`,
        actionImplicitDenies: false,
    };

    const testObject = new DummyRequest({
        bucketName,
        namespace,
        objectKey: objectName,
        headers: {
            'x-amz-meta-test': 'some metadata',
            'content-length': '12',
        },
        parsedContentLength: 12,
        url: `/${bucketName}/${objectName}`,
    }, Buffer.from('I am a body', 'utf8'));

    let request;
    let response;

    beforeEach(() => {
        // create a Promise that resolves when response.end is called
        endPromise = new Promise(resolve => { resolveEnd = resolve; });

        request = new DummyRequest(
            {
                method: 'GET',
                headers: { 'content-length': '123' },
                url: '/_/backbeat/multiplebackendmetadata/bucketName/objectKey?operation=putobject',
            },
            'body'
        );
        response = {
            setHeader: sinon.stub(),
            writeHead: sinon.stub(),
            end: sinon.stub().callsFake((body, encoding, callback) => {
                resolveEnd();
                callback();
            })
        };
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should reject if the request is invalid', done => {
        request.url = '/_/backbeat//bucketName/objectKey?operation=putobject';
        response.end.callsFake((body, format, cb) => {
            const err = JSON.parse(response.end.getCall(0).args[0]);
            assert.strictEqual(err.code, 'MethodNotAllowed');
            cb();
            return done();
        });
        // Cover the case invalidRequest === true
        routeBackbeat('127.0.0.1', request, response, log);
    });

    it('should reject if the route is invalid', done => {
        request.url = '/_/backbeat/wrong/bucketName/objectKey?operation=putobject';
        response.end.callsFake((body, format, cb) => {
            const err = JSON.parse(response.end.getCall(0).args[0]);
            assert.strictEqual(err.code, 'MethodNotAllowed');
            cb();
            return done();
        });
        // Cover the case invalidRoute === true
        routeBackbeat('127.0.0.1', request, response, log);
    });

    [
        {
            method: 'PUT',
            resourceType: 'metadata',
            target: `${bucketName}/${objectName}`,
            operation: null,
            versionId: false,
            expect: errors.MalformedPOSTRequest,
        },
        {
            method: 'GET',
            resourceType: 'metadata',
            target: `${bucketName}/${objectName}`,
            operation: null,
            versionId: true,
            expect: { code: 200 },
        },
        {
            method: 'PUT',
            resourceType: 'data',
            target: `${bucketName}/${objectName}`,
            operation: null,
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'PUT',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'putobject',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'PUT',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'putpart',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'DELETE',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'deleteobject',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'DELETE',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'abortmpu',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'DELETE',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'deleteobjecttagging',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'POST',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'initiatempu',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'POST',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'completempu',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'POST',
            resourceType: 'multiplebackenddata',
            target: `${bucketName}/${objectName}`,
            operation: 'puttagging',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'GET',
            resourceType: 'multiplebackendmetadata',
            target: `${bucketName}/${objectName}`,
            operation: null,
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'POST',
            resourceType: 'batchdelete',
            target: null,
            operation: null,
            versionId: false,
            expect: errors.MalformedPOSTRequest,
        },
        {
            method: 'GET',
            resourceType: 'lifecycle',
            target: `${bucketName}?list-type=wrong`,
            operation: null,
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'POST',
            resourceType: 'index',
            target: bucketName,
            operation: 'add',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'POST',
            resourceType: 'index',
            target: bucketName,
            operation: 'delete',
            versionId: false,
            expect: errors.BadRequest,
        },
        {
            method: 'GET',
            resourceType: 'index',
            target: null,
            operation: 'delete',
            versionId: false,
            expect: errors.NotImplemented,
        }
    ].forEach(testCase => {
        describe(`${testCase.method} ${testCase.resourceType}`, () => {
            let versionIdParsed = null;
            let hasQuery = false;

            beforeEach(done => {
                hasQuery = false;
                versionIdParsed = null;
                request.method = testCase.method;
                request.url = `/_/backbeat/${testCase.resourceType}/${testCase.target}`;
                if (testCase.operation) {
                    request.url += `?operation=${testCase.operation}`;
                    hasQuery = true;
                }

                const enableVersioningRequest =
                    versioningTestUtils.createBucketPutVersioningReq(bucketName, 'Enabled');

                return async.series([
                    next => bucketPut(authInfo, testBucket, log, next),
                    next => bucketPutVersioning(authInfo, enableVersioningRequest, log, next),
                    next => objectPut(authInfo, testObject, undefined, log, (err, res) => {
                        if (!err && res) {
                            versionIdParsed = res['x-amz-version-id'];
                            if (testCase.versionId) {
                                request.url += `${(hasQuery ? '&' : '?')}&versionId=${versionIdParsed}`;
                            }
                        }
                        next(err);
                    }),
                ], done);
            });

            afterEach(done => {
                async.series([
                    next => {
                        const deleteRequest = {
                            bucketName,
                            objectKey: objectName,
                            headers: {},
                            query: versionIdParsed ? { versionId: versionIdParsed } : {},
                        };
                        objectDelete(authInfo, deleteRequest, log, next);
                    },
                    next => {
                        bucketDelete(authInfo, testBucket, log, next);
                    }
                ], done);
            });

            it('should call method successfully', async () => {
                // Mock auth server to ignore auth in this test
                sinon.stub(auth.server, 'doAuth').yields(null, new AuthInfo({
                    canonicalID: 'abcdef/lifecycle',
                    accountDisplayName: 'Lifecycle Service Account',
                }), undefined, undefined, {
                    accountQuota: 1000,
                });

                routeBackbeat('127.0.0.1', request, response, log);

                void await endPromise;

                if (testCase.expect) {
                    const errCode = response.writeHead.getCall(0).args[0];
                    assert.strictEqual(errCode, testCase.expect.code);
                }
                assert.strictEqual(Array.isArray(request.finalizerHooks), true);
                assert.strictEqual(request.apiMethods[0], 'objectReplicate');
                assert.strictEqual(request.apiMethods.length, 1);
                assert.strictEqual(request.accountQuotas, 1000);
            });

            it('should return access denied user is not authorized', async () => {
                sinon.stub(auth.server, 'doAuth').yields(null, new AuthInfo({
                    canonicalID: '123456789',
                    accountDisplayName: 'user1',
                }), [{
                    isAllowed: false,
                    implicitDeny: true,
                    action: 'objectReplicate',
                }], undefined, undefined);

                routeBackbeat('127.0.0.1', request, response, log);

                void await endPromise;

                const err = JSON.parse(response.end.getCall(0).args[0]);
                assert.strictEqual(err.code, 'AccessDenied');
            });

            [true, false].forEach(bypass => {
                // Bucket policies only affect APIs that call standardMetadataValidateBucketAndObj
                if (testCase.resourceType !== 'metadata' && testCase.resourceType !== 'data') {
                    return;
                }
                it(`should ${bypass ? '' : 'not '}bypass bucket policy evaluation`, async () => {
                    const policyRequest = {
                    bucketName,
                    headers: {
                        host: `${bucketName}.s3.amazonaws.com`,
                    },
                    post: JSON.stringify({
                        Version: '2012-10-17',
                        Statement: [{
                            Effect: 'Deny',
                            Principal: '*',
                            Action: '*',
                            Resource: `arn:aws:s3:::${bucketName}/*`,
                        }],
                    }),
                    actionImplicitDenies: false,
                    };
                    await bucketPutPolicyPromise(authInfo, policyRequest, log);

                    // simulate assume role session user
                    const sessionAuthInfo = new AuthInfo({
                        arn: 'arn:aws:sts::000000000000:assumed-role/session',
                        canonicalID: authInfo.getCanonicalID(),
                        accountDisplayName: authInfo.getAccountDisplayName(),
                    });

                    sinon.stub(auth.server, 'doAuth').yields(null, sessionAuthInfo, [{
                        isAllowed: true,
                        implicitDeny: false,
                        action: 'objectReplicate',
                    }], undefined, undefined);

                    request.bypassUserBucketPolicies = bypass;

                    routeBackbeat('127.0.0.1', request, response, log);

                    void await endPromise;

                    if (bypass) {
                        const errCode = response.writeHead.getCall(0).args[0];
                        assert.strictEqual(errCode, testCase.expect.code);
                    } else {
                        const err = JSON.parse(response.end.getCall(0).args[0]);
                        assert.strictEqual(err.code, 'AccessDenied');
                    }
                });
            });
        });
    });
});
