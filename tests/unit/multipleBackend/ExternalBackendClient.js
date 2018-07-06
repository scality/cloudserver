const assert = require('assert');

const AwsClient = require('../../../lib/data/external/AwsClient');
const GcpClient = require('../../../lib/data/external/GcpClient');
const AzureClient = require('../../../lib/data/external/AzureClient');
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
            type: 'gcp',
        },
    },
    {
        Class: AzureClient,
        name: 'AzureClient',
        config: {
            azureStorageEndpoint: '',
            azureStorageCredentials: {
                storageAccountName: 'scality',
                storageAccessKey: 'Zm9vCg==',
            },
            azureContainerName: 'azureTestBucketName',
            dataStoreName: 'azureDataStore',
            type: 'azure',
        },
    },
];
const log = new DummyRequestLogger();

describe('external backend clients', () => {
    backendClients.forEach(backend => {
        let testClient;

        before(() => {
            testClient = new backend.Class(backend.config);
            testClient._client = new DummyService({ versioning: true });
        });

        if (backend.config.type !== 'azure') {
            it(`${backend.name} completeMPU should return correctly ` +
            'typed mpu results', done => {
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
                testClient.completeMPU(jsonList, null, key,
                uploadId, bucketName, log, (err, res) => {
                    assert.strictEqual(typeof res.key, 'string');
                    assert.strictEqual(typeof res.eTag, 'string');
                    assert.strictEqual(typeof res.dataStoreVersionId,
                                       'string');
                    assert.strictEqual(typeof res.contentLength, 'number');
                    return done();
                });
            });
        }

        it(`${backend.name} toObjectGetInfo should return correct ` +
        'objectGetInfo object', () => {
            const key = 'externalBackendTestKey';
            const bucketName = 'externalBackendTestBucket';
            const objectGetInfo = testClient.toObjectGetInfo(key, bucketName);
            assert.deepStrictEqual(objectGetInfo, {
                // bucketMatch === false => expect bucket name to be
                // prefixed to the backend key
                key: 'externalBackendTestBucket/externalBackendTestKey',
                dataStoreName: backend.config.dataStoreName,
            });
        });
        // To-Do: test the other external client methods
    });
});
