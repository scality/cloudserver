const assert = require('assert');
const sinon = require('sinon');
const { getCapabilities } = require('../../../lib/utilities/reportHandler');

describe('reportHandler.getCapabilities', () => {
    const sandbox = sinon.createSandbox();
    const originalEnv = process.env.LOCAL_VOLUME_CAPABILITY;

    afterEach(() => {
        sandbox.restore();
        if (originalEnv === undefined) {
            delete process.env.LOCAL_VOLUME_CAPABILITY;
        } else {
            process.env.LOCAL_VOLUME_CAPABILITY = originalEnv;
        }
    });

    describe('getCapabilities', () => {
        it('should return default capabilities when config.capabilities is not set', () => {
            const cfg = {
                supportedLifecycleRules: ['Expiration', 'Transition', 'NoncurrentVersionExpiration'],
            };
            const caps = getCapabilities(cfg);

            assert.deepStrictEqual(caps, {
                locationTypeAzure: true,
                locationTypeGCP: true,
                locationTypeDigitalOcean: true,
                locationTypeS3Custom: true,
                locationTypeSproxyd: true,
                locationTypeNFS: true,
                locationTypeCephRadosGW: true,
                locationTypeHyperdriveV2: true,
                locationTypeLocal: true,
                preferredReadLocation: true,
                managedLifecycle: true,
                managedLifecycleTransition: true,
                secureChannelOptimizedPath: true,
                s3cIngestLocation: true,
                nfsIngestLocation: false,
                cephIngestLocation: false,
                awsIngestLocation: false,
            });
        });

        it('should use capabilities from config when specified', () => {
            const cfg = {
                capabilities: {
                    locationTypeAzure: false,
                    locationTypeGCP: false,
                    locationTypeDigitalOcean: true,
                    locationTypeS3Custom: false,
                    customCapability: 'test-value',
                },
                supportedLifecycleRules: ['Expiration'],
            };
            const caps = getCapabilities(cfg);

            assert.deepStrictEqual(caps, {
                locationTypeAzure: false,
                locationTypeGCP: false,
                locationTypeDigitalOcean: true,
                locationTypeS3Custom: false,
                customCapability: 'test-value',
            });
        });

        it('should apply LOCAL_VOLUME_CAPABILITY env when set to false', () => {
            process.env.LOCAL_VOLUME_CAPABILITY = 'false';
            const cfg = {
                capabilities: {
                    locationTypeLocal: true,
                },
                supportedLifecycleRules: ['Expiration'],
            };
            const caps = getCapabilities(cfg);

            // locationTypeLocal should be forced to false due to env variable
            assert.strictEqual(caps.locationTypeLocal, false);
        });

        it('should apply LOCAL_VOLUME_CAPABILITY env when set to "0"', () => {
            process.env.LOCAL_VOLUME_CAPABILITY = '0';
            const cfg = {
                capabilities: {
                    locationTypeLocal: true,
                },
                supportedLifecycleRules: ['Expiration'],
            };
            const caps = getCapabilities(cfg);

            // locationTypeLocal should be forced to false due to env variable
            assert.strictEqual(caps.locationTypeLocal, false);
        });

        it('should apply LOCAL_VOLUME_CAPABILITY env when set to true', () => {
            process.env.LOCAL_VOLUME_CAPABILITY = '1';
            const cfg = {
                capabilities: {
                    locationTypeLocal: true,
                },
                supportedLifecycleRules: ['Expiration'],
            };
            const caps = getCapabilities(cfg);

            // locationTypeLocal should remain true
            assert.strictEqual(caps.locationTypeLocal, true);
        });

        it('should not apply LOCAL_VOLUME_CAPABILITY env if locationTypeLocal disabled', () => {
            process.env.LOCAL_VOLUME_CAPABILITY = true;
            const cfg = {
                capabilities: {
                    locationTypeLocal: false,
                },
                supportedLifecycleRules: ['Expiration'],
            };
            const caps = getCapabilities(cfg);

            // locationTypeLocal should remain true
            assert.strictEqual(caps.locationTypeLocal, false);
        });

        it('should disable managedLifecycle when Expiration is not in supportedLifecycleRules', () => {
            const cfg = {
                capabilities: {
                    managedLifecycle: true,
                },
                supportedLifecycleRules: ['Transition', 'NoncurrentVersionExpiration'],
            };
            const caps = getCapabilities(cfg);

            // managedLifecycle should be false
            assert.strictEqual(caps.managedLifecycle, false);
        });

        it('should keep managedLifecycle when Expiration is in supportedLifecycleRules', () => {
            const cfg = {
                capabilities: {
                    managedLifecycle: true,
                },
                supportedLifecycleRules: ['Expiration', 'Transition'],
            };
            const caps = getCapabilities(cfg);

            // managedLifecycle should remain true
            assert.strictEqual(caps.managedLifecycle, true);
        });

        it('should not enable managedLifecycle if managedLifecycle is disabled', () => {
            const cfg = {
                capabilities: {
                    managedLifecycle: false,
                },
                supportedLifecycleRules: ['Expiration', 'Transition'],
            };
            const caps = getCapabilities(cfg);

            // managedLifecycle should remain false
            assert.strictEqual(caps.managedLifecycle, false);
        });

        it('should disable managedLifecycleTransition when Transition is not in supportedLifecycleRules', () => {
            const cfg = {
                capabilities: {
                    managedLifecycleTransition: true,
                },
                supportedLifecycleRules: ['Expiration', 'NoncurrentVersionExpiration'],
            };
            const caps = getCapabilities(cfg);

            // managedLifecycleTransition should be false
            assert.strictEqual(caps.managedLifecycleTransition, false);
        });

        it('should keep managedLifecycleTransition when Transition is in supportedLifecycleRules', () => {
            const cfg = {
                capabilities: {
                    managedLifecycleTransition: true,
                },
                supportedLifecycleRules: ['Expiration', 'Transition'],
            };
            const caps = getCapabilities(cfg);

            // managedLifecycleTransition should remain true
            assert.strictEqual(caps.managedLifecycleTransition, true);
        });

        it('should not enable managedLifecycleTransition if managedLifecycleTransition is disabled', () => {
            const cfg = {
                capabilities: {
                    managedLifecycleTransition: true,
                },
                supportedLifecycleRules: ['Expiration', 'Transition'],
            };
            const caps = getCapabilities(cfg);

            // managedLifecycleTransition should remain true
            assert.strictEqual(caps.managedLifecycleTransition, true);
        });

        it('should override lifecycleRules from supportedLifecycleRules', () => {
            const supportedRules = ['Expiration', 'Transition', 'NoncurrentVersionExpiration'];
            const cfg = {
                capabilities: {
                    lifecycleRules: ['Expiration'],
                },
                supportedLifecycleRules: supportedRules,
            };
            const caps = getCapabilities(cfg);

            // lifecycleRules should be set to supportedLifecycleRules
            assert.deepStrictEqual(caps.lifecycleRules, supportedRules);
        });

        it('should update locationTypes capabilities based on locationTypes map', () => {
            const cfg = {
                capabilities: {
                    locationTypeAzure: true,
                    locationTypeGCP: true,
                    locationTypeDigitalOcean: true,
                    locationTypeS3Custom: true,
                    locationTypeSproxyd: true,
                    locationTypeNFS: true,
                    locationTypeCephRadosGW: true,
                    locationTypeHyperdriveV2: true,
                    locationTypeLocal: true,
                    locationTypes: [
                        'location-gcp-v1',
                        'location-scality-sproxyd-v1',
                        'location-ceph-radosgw-s3-v1',
                        'location-file-v1',
                        'location-scality-artesca-s3-v1',
                    ],
                },
                supportedLifecycleRules: ['Expiration'],
            };
            const caps = getCapabilities(cfg);

            // Verify locationTypes override the legacy flags
            assert.strictEqual(caps.locationTypeAzure, false);
            assert.strictEqual(caps.locationTypeGCP, true);
            assert.strictEqual(caps.locationTypeDigitalOcean, false);
            assert.strictEqual(caps.locationTypeSproxyd, true);
            assert.strictEqual(caps.locationTypeNFS, false);
            assert.strictEqual(caps.locationTypeCephRadosGW, true);
            assert.strictEqual(caps.locationTypeHyperdriveV2, false);
            assert.strictEqual(caps.locationTypeLocal, true);
        });

        it('should handle multiple consistency checks together', () => {
            process.env.LOCAL_VOLUME_CAPABILITY = '0';
            const cfg = {
                capabilities: {
                    locationTypeLocal: true,
                    secureChannelOptimizedPath: true,
                    managedLifecycle: true,
                    managedLifecycleTransition: true,
                    locationTypeAzure: true,
                    locationTypeGCP: true,
                    locationTypes: ['location-azure-v1'],
                },
                supportedLifecycleRules: ['Expiration'], // Missing Transition
            };
            const caps = getCapabilities(cfg);

            // All consistency checks should be applied
            assert.strictEqual(caps.locationTypeLocal, false); // env override
            assert.strictEqual(caps.managedLifecycle, true); // Expiration present
            assert.strictEqual(caps.managedLifecycleTransition, false); // Transition missing
            assert.strictEqual(caps.locationTypeAzure, true); // locationTypes override
            assert.strictEqual(caps.locationTypeGCP, false); // locationTypes override
        });

        it('should not modify capabilities when locationTypes is not defined', () => {
            const cfg = {
                capabilities: {
                    locationTypeAzure: true,
                    locationTypeGCP: false,
                },
                supportedLifecycleRules: ['Expiration'],
            };
            const caps = getCapabilities(cfg);

            // Original values should be preserved
            assert.strictEqual(caps.locationTypeAzure, true);
            assert.strictEqual(caps.locationTypeGCP, false);
        });
    });
});
