const assert = require('assert');
const validateSystemSchema = require('../../../../../lib/routes/veeam/schemas/system');
const { errors } = require('arsenal');

const modelName = '"ARTESCA"';

describe('RouteVeeam: validateSystemSchema 1.0', () => {
    const protocolVersion = '"1.0"';
    [
        null,
        undefined,
        '',
        {
            SystemInfo: {
                ProtocolVersion: protocolVersion,
                ModelName: modelName,
                ProtocolCapabilities: {
                    CapacityInfo: true,
                    UploadSessions: true,
                    IAMSTS: true,
                }
            },
            SystemRecommendations: {
                S3ConcurrentTaskLimit: 0,
                S3MultiObjectDeleteLimit: 1,
                StorageCurrentTasksLimit: 0,
                KbBlockSize: 256,
            },
        },
    ].forEach(test => {
        it(`should return MalformedXML for ${JSON.stringify(test)}`, () => {
            assert.throws(() => validateSystemSchema(test).message, errors.MalformedXML.message);
        });
    });

    [
        {
            SystemInfo: {
                ProtocolVersion: protocolVersion,
                ModelName: modelName,
                ProtocolCapabilities: {
                    CapacityInfo: true,
                    UploadSessions: true,
                },
            },
        },
        {
            SystemInfo: {
                ProtocolVersion: protocolVersion,
                ModelName: modelName,
                ProtocolCapabilities: {
                    CapacityInfo: true,
                    UploadSessions: true,
                    IAMSTS: false,
                },
                SystemRecommendations: {
                    S3ConcurrentTaskLimit: 0,
                    S3MultiObjectDeleteLimit: 1,
                    StorageCurrentTasksLimit: 0,
                    KbBlockSize: 256,
                },
            },
        },
        {
            SystemInfo: {
                ProtocolVersion: protocolVersion,
                ModelName: modelName,
                ProtocolCapabilities: {
                    CapacityInfo: true,
                    UploadSessions: true,
                    IAMSTS: true,
                },
                APIEndpoints: {
                    IAMEndpoint: 'testUrl',
                    STSEndpoint: 'testUrl',
                },
                SystemRecommendations: {
                    S3ConcurrentTaskLimit: 0,
                    S3MultiObjectDeleteLimit: 1,
                    StorageCurrentTasksLimit: 0,
                    KbBlockSize: 256,
                },
            },
        },
    ].forEach(test => {
        it(`should validate XML for ${JSON.stringify(test)}`, () => {
            assert.doesNotThrow(() => validateSystemSchema(test).message);
        });
    });
});


describe('RouteVeeam: validateSystemSchema unknown version', () => {
    const protocolVersion = '"1.1"';
    [
        {
            SystemInfo: {
                ProtocolVersion: protocolVersion,
                ModelName: modelName,
                ProtocolCapabilities: {
                    CapacityInfo: true,
                    UploadSessions: true,
                    IAMSTS: true,
                }
            },
            SystemRecommendations: {
                S3ConcurrentTaskLimit: 0,
                S3MultiObjectDeleteLimit: 1,
                StorageCurrentTasksLimit: 0,
                KbBlockSize: 256,
            },
        },
        {
            SystemInfo: {
                ProtocolVersion: protocolVersion,
                ModelName: modelName,
                ProtocolCapabilities: {
                    CapacityInfo: true,
                    UploadSessions: true,
                },
            },
        },
        {
            SystemInfo: {
                ProtocolVersion: protocolVersion,
                ModelName: modelName,
                ProtocolCapabilities: {
                    CapacityInfo: true,
                    UploadSessions: true,
                    IAMSTS: false,
                },
                SystemRecommendations: {
                    S3ConcurrentTaskLimit: 0,
                    S3MultiObjectDeleteLimit: 1,
                    StorageCurrentTasksLimit: 0,
                    KbBlockSize: 256,
                },
            },
        },
        {
            SystemInfo: {
                ProtocolVersion: protocolVersion,
                ModelName: modelName,
                ProtocolCapabilities: {
                    CapacityInfo: true,
                    UploadSessions: true,
                    IAMSTS: true,
                },
                APIEndpoints: {
                    IAMEndpoint: 'testUrl',
                    STSEndpoint: 'testUrl',
                },
                SystemRecommendations: {
                    S3ConcurrentTaskLimit: 0,
                    S3MultiObjectDeleteLimit: 1,
                    StorageCurrentTasksLimit: 0,
                    KbBlockSize: 256,
                },
            },
        },
    ].forEach(test => {
        it(`should accept anything for ${JSON.stringify(test)}`, () => {
            assert.doesNotThrow(() => validateSystemSchema(test));
        });
    });
});
