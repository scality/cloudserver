const assert = require('assert');
const AwsClient = require('../../../lib/data/external/AwsClient');
const GcpClient = require('../../../lib/data/external/GcpClient');
const DummyService = require('../DummyService');
const { DummyRequestLogger } = require('../helpers');

const backendClients = [
    {
        Class: AwsClient,
        name: 'AwsClient',
        config: {
            s3Params: {},
            bucketName: 'awsTestBucketName',
            dataStoreName: 'awsDataStore',
            serverSideEncryption: false,
            bucketMatch: true,
            type: 'aws',
        },
    },
    {
        Class: GcpClient,
        name: 'GcpClient',
        config: {
            s3Params: {},
            bucketName: 'gcpTestBucketName',
            mpuBucket: 'gcpTestMpuBucketName',
            dataStoreName: 'gcpDataStore',
            bucketMatch: true,
            type: 'gcp',
        },
    },
];

backendClients.forEach(backend => {
    let testClient;

    before(() => {
        testClient = new backend.Class(backend.config);
        testClient._client = new DummyService(backend.config);
    });

    describe(`${backend.name} completeMPU:`, () => {
        it('should return correctly typed mpu results', done => {
            const jsonList = {
                Part: [
                    {
                        PartNumber: [1],
                        ETag: ['testpart0001etag'],
                    },
                    {
                        PartNumber: [2],
                        ETag: ['testpart0002etag'],
                    },
                    {
                        PartNumber: [3],
                        ETag: ['testpart0003etag'],
                    },
                ],
            };
            const key = 'externalBackendTestKey';
            const bucketName = 'externalBackendTestBucket';
            const uploadId = 'externalBackendTestUploadId';
            const log = new DummyRequestLogger();

            testClient.completeMPU(jsonList, null, key, uploadId, bucketName,
            log, (err, res) => {
                assert.strictEqual(typeof res.key, 'string');
                assert.strictEqual(typeof res.eTag, 'string');
                assert.strictEqual(typeof res.dataStoreVersionId, 'string');
                assert.strictEqual(typeof res.contentLength, 'number');
                return done();
            });
        });
    });
    // To-Do: test the other external client methods
});

describe('Test GCP versioning delete marker', () => {
    let testClient;

    before(() => {
        const backend = backendClients[1];
        testClient = new backend.Class(backend.config);
        testClient._client = new DummyService(backend.config);
    });

    it('should return "0" as delete marker versionId', done => {
        const stream = 'testValue';
        const size = stream.length;
        const keyContext = {
            objectKey: 'testKeyValue',
            isDeleteMarker: true,
        };
        const reqUids = '1234';

        testClient.put(stream, size, keyContext, reqUids,
        (err, key, versionId) => {
            assert.strictEqual(key, keyContext.objectKey);
            assert.strictEqual(versionId, '0');
            return done();
        });
    });
});
