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

    it('should put metadata', async () => {
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

        /* eslint-disable-next-line no-void */
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

        /* eslint-disable-next-line no-void */
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

        /* eslint-disable-next-line no-void */
        void await endPromise;

        assert.strictEqual(mockResponse.statusCode, 405);
    });

    it('should be rejected when trying batchDelete as a public user', async () => {
        mockRequest = prepareDummyRequest(null, JSON.stringify({
            Locations: [
                {
                    key: 'key0',
                    bucket: 'bucket0',
                },
            ],
        }));

        mockRequest.method = 'POST';
        mockRequest.url = '/_/backbeat/batchdelete';
        mockRequest.headers = {
            'x-scal-versioning-required': 'true',
        };
        mockRequest.destroy = () => {};
        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

        /* eslint-disable-next-line no-void */
        void await endPromise;
        assert.strictEqual(mockResponse.statusCode, 403);
    });

    it('should batchDelete', async () => {
        mockRequest = prepareDummyRequest(null, JSON.stringify({
            Locations: [
                {
                    key: 'key0',
                    bucket: 'bucket0',
                },
            ],
        }));

        mockRequest.method = 'POST';
        mockRequest.url = '/_/backbeat/batchdelete';
        mockRequest.headers = {
            'x-scal-versioning-required': 'true',
        };
        mockRequest.destroy = () => {};
        const doAuthStub = sandbox.stub(auth.server, 'doAuth');
        doAuthStub.callsFake((req, log, cb) => {
            cb(null, {
                canonicalID: 'id',
                getCanonicalID: () => 'id',
            });
        });

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

        /* eslint-disable-next-line no-void */
        void await endPromise;
        assert.strictEqual(mockResponse.statusCode, 200);
        assert.deepStrictEqual(mockResponse.body, null);
    });

    it('should batchDelete with conditions and azure location', async () => {
        mockRequest = prepareDummyRequest({
            'if-unmodified-since': '1980-01-01T00:00:00.000Z',
            'x-scal-versioning-required': 'true',
            'x-scal-storage-class': 'azurebackend',
        }, JSON.stringify({
            Locations: [
                {
                    key: 'key0',
                    bucket: 'bucket0',
                },
            ],
        }));

        mockRequest.method = 'POST';
        mockRequest.url = '/_/backbeat/batchdelete';
        mockRequest.destroy = () => {};
        const doAuthStub = sandbox.stub(auth.server, 'doAuth');
        doAuthStub.callsFake((req, log, cb) => {
            cb(null, {
                canonicalID: 'id',
                getCanonicalID: () => 'id',
            });
        });

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

        /* eslint-disable-next-line no-void */
        void await endPromise;
        assert.strictEqual(mockResponse.statusCode, 200);
        assert.deepStrictEqual(mockResponse.body, {});
    });

    it('should not batchDelete with conditions if "if-unmodified-since" header unset', async () => {
        mockRequest = prepareDummyRequest({
            'x-scal-versioning-required': 'true',
            'x-scal-storage-class': 'azurebackend',
        }, JSON.stringify({
            Locations: [
                {
                    key: 'key0',
                    bucket: 'bucket0',
                },
            ],
        }));

        mockRequest.method = 'POST';
        mockRequest.url = '/_/backbeat/batchdelete';
        mockRequest.destroy = () => {};
        const doAuthStub = sandbox.stub(auth.server, 'doAuth');
        doAuthStub.callsFake((req, log, cb) => {
            cb(null, {
                canonicalID: 'id',
                getCanonicalID: () => 'id',
            });
        });

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

        /* eslint-disable-next-line no-void */
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
        mockRequest = prepareDummyRequest({
            'if-unmodified-since': '2000-01-01T00:00:00.000Z',
            'x-scal-versioning-required': 'true',
            'x-scal-storage-class': 'gcpbackend',
            'x-scal-tags': JSON.stringify({ key: 'value' }),
        }, JSON.stringify({
            Locations: [
                {
                    key: 1,
                    bucket: 'bucket0',
                },
            ],
        }));
        mockRequest.method = 'POST';
        mockRequest.url = '/_/backbeat/batchdelete';
        mockRequest.destroy = () => {};
        const doAuthStub = sandbox.stub(auth.server, 'doAuth');
        doAuthStub.callsFake((req, log, cb) => {
            cb(null, {
                canonicalID: 'id',
                getCanonicalID: () => 'id',
            });
        });

        routeBackbeat('127.0.0.1', mockRequest, mockResponse, log);

        /* eslint-disable-next-line no-void */
        void await endPromise;
        assert.strictEqual(mockResponse.statusCode, 200);
        assert.deepStrictEqual(mockResponse.body, null);
    });
});
