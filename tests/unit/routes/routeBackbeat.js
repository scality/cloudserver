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

    it('should allow CRR destination requests (putMetadata) when versioning is enabled', async () => {
        mockRequest.method = 'PUT';
        mockRequest.url = '/_/backbeat/metadata/bucket0/key0';
        mockRequest.headers = {
            'x-scal-versioning-required': 'true',
        };
        mockRequest.destroy = () => {};

        sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
            cb(null, {});
        });

        metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
            const bucketInfo = {
                getVersioningConfiguration: () => ({ Status: 'Enabled' }),
                isVersioningEnabled: () => true,
            };
            const objMd = {};
            callback(null, bucketInfo, objMd);
        });

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

        void await endPromise;

        assert.strictEqual(mockResponse.statusCode, 200);
        assert.deepStrictEqual(mockResponse.body, {});
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

    it('should put non versioned metadata', async () => {
        mockRequest.method = 'PUT';
        mockRequest.url = '/_/backbeat/metadata/bucket0/key0';
        mockRequest.headers = {
            'x-scal-versioning-required': 'true',
        };
        mockRequest.destroy = () => {};

        sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
            cb(null, {});
        });

        metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
            const bucketInfo = {
                getVersioningConfiguration: () => ({ Status: 'Enabled' }),
                isVersioningEnabled: () => true,
            };
            const objMd = {};
            callback(null, bucketInfo, objMd);
        });

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

        void await endPromise;

        assert.strictEqual(mockResponse.statusCode, 200);
        assert.deepStrictEqual(mockResponse.body, {});
    });

    it('should put metadata after updating account info', async () => {
        mockRequest.method = 'PUT';
        mockRequest.url = '/_/backbeat/metadata/bucket0/key0'+
            '?accountId=123456789012';
        mockRequest.headers = {
            'x-scal-versioning-required': 'true',
        };
        mockRequest.destroy = () => {};

        sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
            assert.strictEqual(omVal['owner-display-name'], 'Bart');
            assert.strictEqual(omVal['owner-id'], '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be');
            cb(null, {});
        });

        metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
            const bucketInfo = {
                getVersioningConfiguration: () => ({ Status: 'Enabled' }),
                isVersioningEnabled: () => true,
            };
            const objMd = {};
            callback(null, bucketInfo, objMd);
        });

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

        void await endPromise;

        assert.strictEqual(mockResponse.statusCode, 200);
        assert.deepStrictEqual(mockResponse.body, {});
    });

    it('should fail to put metadata when accountId is invalid', async () => {
        mockRequest.method = 'PUT';
        mockRequest.url = '/_/backbeat/metadata/bucket0/key0'+
            '?accountId=invalid';
        mockRequest.headers = {
            'x-scal-versioning-required': 'true',
        };
        mockRequest.destroy = () => {};

        metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
            const bucketInfo = {
                getVersioningConfiguration: () => ({ Status: 'Enabled' }),
                isVersioningEnabled: () => true,
            };
            const objMd = {};
            callback(null, bucketInfo, objMd);
        });

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

        void await endPromise;

        assert.strictEqual(mockResponse.statusCode, 404);
        assert.deepStrictEqual(mockResponse.body.code, 'AccountNotFound');
    });

    it('should repair master when putting metadata of a new version', async () => {
        mockRequest.method = 'PUT';
        mockRequest.url = '/_/backbeat/metadata/bucket0/key0'+
            '?accountId=123456789012&versionId=aIXVkw5Tw2Pd00000000001I4j3QKsvf';
        mockRequest.headers = {
            'x-scal-versioning-required': 'true',
        };
        mockRequest.destroy = () => {};

        sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
            assert.strictEqual(options.repairMaster, true);
            cb(null, {});
        });

        metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
            const bucketInfo = {
                getVersioningConfiguration: () => ({ Status: 'Enabled' }),
                isVersioningEnabled: () => true,
            };
            callback(null, bucketInfo, undefined);
        });

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

        void await endPromise;

        assert.strictEqual(mockResponse.statusCode, 200);
        assert.deepStrictEqual(mockResponse.body, {});
    });

    it('should not repair master when updating metadata of an existing version', async () => {
        mockRequest.method = 'PUT';
        mockRequest.url = '/_/backbeat/metadata/bucket0/key0'+
            '?accountId=123456789012&versionId=aIXVkw5Tw2Pd00000000001I4j3QKsvf';
        mockRequest.headers = {
            'x-scal-versioning-required': 'true',
        };
        mockRequest.destroy = () => {};

        sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
            assert.strictEqual(options.repairMaster, undefined);
            cb(null, {});
        });

        metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
            const bucketInfo = {
                getVersioningConfiguration: () => ({ Status: 'Enabled' }),
                isVersioningEnabled: () => true,
            };
            callback(null, bucketInfo, {});
        });

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

        void await endPromise;

        assert.strictEqual(mockResponse.statusCode, 200);
        assert.deepStrictEqual(mockResponse.body, {});
    });

    it('should handle error when putting metadata', async () => {
        mockRequest.method = 'PUT';
        mockRequest.url = '/_/backbeat/metadata/bucket0/key0';
        mockRequest.headers = {
            'x-scal-versioning-required': 'true',
        };
        mockRequest.destroy = () => {};

        sandbox.stub(metadata, 'putObjectMD').callsFake((bucketName, objectKey, omVal, options, logParam, cb) => {
            cb(new Error('error'));
        });

        metadataUtils.standardMetadataValidateBucketAndObj.callsFake((params, denies, log, callback) => {
            const bucketInfo = {
                getVersioningConfiguration: () => ({ Status: 'Enabled' }),
                isVersioningEnabled: () => true,
            };
            const objMd = {};
            callback(null, bucketInfo, objMd);
        });

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

        void await endPromise;

        assert.strictEqual(mockResponse.statusCode, 500);
    });

    it('should be rejected when using a wrong route', async () => {
        mockRequest.method = 'PUT';
        mockRequest.url = '/_/backbeat/metadata/bucket0';
        mockRequest.headers = {
            'x-scal-versioning-required': 'true',
        };
        mockRequest.destroy = () => {};

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

        void await endPromise;

        assert.strictEqual(mockResponse.statusCode, 405);
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
