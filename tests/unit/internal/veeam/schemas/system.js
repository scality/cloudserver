const assert = require('assert');
const validateSystemSchema = require('../../../../../lib/routes/veeam/schemas/system');
const { errorInstances } = require('@scality/arsenal');
const { config } = require('../../../../../lib/Config');

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
                },
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
            assert.throws(() => validateSystemSchema(test).message, errorInstances.MalformedXML.message);
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
        {
            SystemInfo: {
                ProtocolVersion: protocolVersion,
                ModelName: modelName,
                ProtocolCapabilities: {
                    CapacityInfo: true,
                    UploadSessions: true,
                    EnableOnPremArchiveTier: false,
                },
            },
        },
    ].forEach(test => {
        it(`should validate XML for ${JSON.stringify(test)}`, () => {
            assert.doesNotThrow(() => validateSystemSchema(test).message);
        });
    });

    describe('EnableOnPremArchiveTier support', () => {
        const systemInfo = {
            SystemInfo: {
                ProtocolVersion: protocolVersion,
                ModelName: modelName,
                ProtocolCapabilities: {
                    CapacityInfo: true,
                    UploadSessions: true,
                },
            },
        };
        const withEnableOnPremArchiveTier = value => ({
            SystemInfo: {
                ...systemInfo.SystemInfo,
                ProtocolCapabilities: {
                    ...systemInfo.SystemInfo.ProtocolCapabilities,
                    EnableOnPremArchiveTier: value,
                },
            },
        });
        let originalEnableDirectToCold;

        const setSupported = supported => {
            config.enableDirectToCold = supported;
            if (supported) {
                config.locationConstraints.glacier = { ...config.locationConstraints['location-dmf-v1'] };
            }
        };

        beforeEach(() => {
            originalEnableDirectToCold = config.enableDirectToCold;
        });

        afterEach(() => {
            config.enableDirectToCold = originalEnableDirectToCold;
            delete config.locationConstraints.glacier;
        });

        [true, false].forEach(supported => {
            it(`should not add EnableOnPremArchiveTier when not provided (supported: ${supported})`, () => {
                setSupported(supported);
                const validated = validateSystemSchema(systemInfo);
                assert.strictEqual(validated.SystemInfo.ProtocolCapabilities.EnableOnPremArchiveTier, undefined);
            });
        });

        it('should accept true when supported', () => {
            setSupported(true);
            const validated = validateSystemSchema(withEnableOnPremArchiveTier(true));
            assert.strictEqual(validated.SystemInfo.ProtocolCapabilities.EnableOnPremArchiveTier, true);
        });

        it('should accept false when not supported', () => {
            setSupported(false);
            const validated = validateSystemSchema(withEnableOnPremArchiveTier(false));
            assert.strictEqual(validated.SystemInfo.ProtocolCapabilities.EnableOnPremArchiveTier, false);
        });

        it('should reject true when not supported', () => {
            setSupported(false);
            assert.throws(
                () => validateSystemSchema(withEnableOnPremArchiveTier(true)),
                err => {
                    assert.strictEqual(err.message, 'InvalidRequest');
                    assert.match(err.description, /EnableOnPremArchiveTier is not supported/);
                    return true;
                },
            );
        });
    });

    it('should convert EnableOnPremArchiveTier from the XML string value', () => {
        const validated = validateSystemSchema({
            SystemInfo: {
                ProtocolVersion: protocolVersion,
                ModelName: modelName,
                ProtocolCapabilities: {
                    CapacityInfo: 'true',
                    UploadSessions: 'false',
                    EnableOnPremArchiveTier: 'false',
                },
            },
        });
        assert.strictEqual(validated.SystemInfo.ProtocolCapabilities.EnableOnPremArchiveTier, false);
    });

    it('should reject an invalid EnableOnPremArchiveTier value', () => {
        assert.throws(() =>
            validateSystemSchema({
                SystemInfo: {
                    ProtocolVersion: protocolVersion,
                    ModelName: modelName,
                    ProtocolCapabilities: {
                        CapacityInfo: true,
                        UploadSessions: true,
                        EnableOnPremArchiveTier: 'maybe',
                    },
                },
            }),
        );
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
                },
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
