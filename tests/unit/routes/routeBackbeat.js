const assert = require('assert');
const sinon = require('sinon');
const { promisify } = require('util');
const metadataUtils = require('../../../lib/metadata/metadataUtils');
const storeObject = require('../../../lib/api/apiUtils/object/storeObject');
const metadata = require('../../../lib/metadata/wrapper');
const { DummyRequestLogger } = require('../helpers');
const dataWrapper = require('../../../lib/data/wrapper');
const DummyRequest = require('../DummyRequest');
const { auth } = require('arsenal');
const { config } = require('../../../lib/Config');
const quotaUtils = require('../../../lib/api/apiUtils/quotas/quotaUtils');

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
        routeBackbeat = require('../../../lib/routes/routeBackbeat');
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

            sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
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

            sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
                assert.strictEqual(options.repairMaster, true);
                cb(null, {});
            });

            // Override default callback to return undefined for objMd to simulate new version
            metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
                callback(null, bucketInfo, undefined);
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
            sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
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

            sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
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

            sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
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

            sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
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

            sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
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

            sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
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
});
