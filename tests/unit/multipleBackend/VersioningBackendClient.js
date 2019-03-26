const assert = require('assert');
const { errors, storage } = require('arsenal');

const AwsClient = storage.data.external.AwsClient;
const { config } = require('../../../lib/Config');
const DummyService = require('../DummyService');
const { DummyRequestLogger } = require('../helpers');

const missingVerIdInternalError = errors.InternalError.customizeDescription(
    'Invalid state. Please ensure versioning is enabled ' +
    'in AWS for the location constraint and try again.'
);

const log = new DummyRequestLogger();
const copyObjectRequest = {
    bucketName: 'copyobjecttestbucket',
    objectKey: 'copyobjecttestkey',
    headers: {
        'x-amz-metadata-directive': 'COPY',
    },
};

const sourceLocationConstraint = 'awsbackend';
const key = 'externalBackendTestKey';
const bucket = 'externalBackendTestBucket';
const reqUID = '42';
const jsonList = {
    Part: [
        { PartNumber: [1], ETag: ['testpart0001etag'] },
        { PartNumber: [2], ETag: ['testpart0002etag'] },
        { PartNumber: [3], ETag: ['testpart0003etag'] },
    ],
};

const s3Config = {
    s3Params: {},
    bucketMatch: true,
    bucketName: 'awsTestBucketName',
    dataStoreName: 'awsDataStore',
    serverSideEncryption: false,
    supportsVersioning: true,
    type: 'aws',
};

const assertSuccess = (err, cb) => {
    assert.ifError(err,
        `Expected success, but got error ${err}`);
    cb();
};

const assertFailure = (err, cb) => {
    assert.deepStrictEqual(err, missingVerIdInternalError);
    cb();
};
const genTests = [
    {
        msg: 'should return success if supportsVersioning === true ' +
        'and backend versioning is enabled',
        input: { supportsVersioning: true, enableMockVersioning: true },
        callback: assertSuccess,
    },
    {
        msg: 'should return success if supportsVersioning === false ' +
        'and backend versioning is enabled',
        input: { supportsVersioning: false, enableMockVersioning: true },
        callback: assertSuccess,
    },
    {
        msg: 'should return error if supportsVersioning === true ' +
        'and backend versioning is disabled',
        input: { supportsVersioning: true, enableMockVersioning: false },
        callback: assertFailure,
    },
    {
        msg: 'should return success if supportsVersioning === false ' +
        'and backend versioning is disabled',
        input: { supportsVersioning: false, enableMockVersioning: false },
        callback: assertSuccess,
    },
];

describe('AwsClient::putObject', () => {
    let testClient;

    beforeAll(() => {
        testClient = new AwsClient(s3Config);
        testClient._client = new DummyService({ versioning: true });
    });
    genTests.forEach(test => test(test.msg, done => {
        testClient._supportsVersioning = test.input.supportsVersioning;
        testClient._client.versioning = test.input.enableMockVersioning;
        testClient.put('', 0, { bucketName: bucket, objectKey: key },
        reqUID, err => test.callback(err, done));
    }));
});

describe('AwsClient::copyObject', () => {
    let testClient;

    beforeAll(() => {
        testClient = new AwsClient(s3Config);
        testClient._client = new DummyService({ versioning: true });
    });

    genTests.forEach(test => test(test.msg, done => {
        testClient._supportsVersioning = test.input.supportsVersioning;
        testClient._client.versioning = test.input.enableMockVersioning;
        testClient.copyObject(copyObjectRequest, null, key,
        sourceLocationConstraint, null, config, log,
        err => test.callback(err, done));
    }));
});

describe('AwsClient::completeMPU', () => {
    let testClient;

    beforeAll(() => {
        testClient = new AwsClient(s3Config);
        testClient._client = new DummyService({ versioning: true });
    });
    genTests.forEach(test => test(test.msg, done => {
        testClient._supportsVersioning = test.input.supportsVersioning;
        testClient._client.versioning = test.input.enableMockVersioning;
        const uploadId = 'externalBackendTestUploadId';
        testClient.completeMPU(jsonList, null, key, uploadId,
        bucket, log, err => test.callback(err, done));
    }));
});

describe('AwsClient::healthcheck', () => {
    let testClient;

    function assertSuccessVersioned(resp, cb) {
        assert.deepStrictEqual(resp, {
            versioningStatus: 'Enabled',
            message: 'Congrats! You own the bucket',
        });
        cb();
    }
    function assertSuccessNonVersioned(resp, cb) {
        assert.deepStrictEqual(resp, {
            message: 'Congrats! You own the bucket',
        });
        cb();
    }
    function assertFailure(resp, cb) {
        expect(!resp.Status || resp.Status === 'Suspended').toBe(true);
        if (resp.Status) {
            expect(resp.message).toBe('Versioning must be enabled');
        }
        expect(resp.external).toBe(true);
        cb();
    }

    beforeAll(() => {
        testClient = new AwsClient(s3Config);
        testClient._client = new DummyService({ versioning: true });
    });

    const tests = [
        {
            msg: 'should return success if supportsVersioning === true ' +
            'and backend versioning is enabled',
            input: { supportsVersioning: true, enableMockVersioning: true },
            callback: assertSuccessVersioned,
        },
        {
            msg: 'should return success if supportsVersioning === false ' +
            'and backend versioning is enabled',
            input: { supportsVersioning: false, enableMockVersioning: true },
            callback: assertSuccessNonVersioned,
        },
        {
            msg: 'should return error if supportsVersioning === true ' +
            ' and backend versioning is disabled',
            input: { supportsVersioning: true, enableMockVersioning: false },
            callback: assertFailure,
        },
        {
            msg: 'should return success if supportsVersioning === false ' +
            'and backend versioning is disabled',
            input: { supportsVersioning: false, enableMockVersioning: false },
            callback: assertSuccessNonVersioned,
        },
    ];
    tests.forEach(test => test(test.msg, done => {
        testClient._supportsVersioning = test.input.supportsVersioning;
        testClient._client.versioning = test.input.enableMockVersioning;
        testClient.healthcheck('backend',
        (err, resp) => test.callback(resp.backend, done));
    }));
});
