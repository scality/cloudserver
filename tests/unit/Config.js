const assert = require('assert');
const fs = require('fs');
const sinon = require('sinon');
const path = require('path');
const {
    azureGetStorageAccountName,
    azureGetLocationCredentials,
    locationConstraintAssert,
    parseSupportedLifecycleRules,
    parseIntegrityChecks,
    ConfigObject,
} = require('../../lib/Config');

const { LOCATION_NAME_DMF } = require('../constants');
const constants = require('../../constants');
const allOptsQuota = require('./testConfigs/allOptsConfig/config.json').quota;

const { ValidLifecycleRules: supportedLifecycleRules } = require('arsenal').models;

describe('Config', () => {
    const defaultConfig = JSON.parse(fs.readFileSync('config.json'), { encoding: 'utf-8' });

    const envToRestore = [];
    const setEnv = (key, value) => {
        if (key in process.env) {
            const v = process.env[key];
            envToRestore.push(() => {
                process.env[key] = v;
            });
        } else {
            envToRestore.push(() => {
                delete process.env[key];
            });
        }
        process.env[key] = value;
    };

    beforeEach(() => {
        envToRestore.length = 0;
    });
    afterEach(() => {
        envToRestore.reverse().forEach(cb => cb());
    });

    it('should load default config.json without errors', done => {
        require('../../lib/Config');
        done();
    });

    it('should emit an event when auth data is updated', done => {
        const config = new ConfigObject();
        let emitted = false;
        config.on('authdata-update', () => {
            emitted = true;
        });
        config.setAuthDataAccounts([]);
        if (emitted) {
            return done();
        }
        return done(new Error('authdata-update event was not emitted'));
    });

    describe('azureGetStorageAccountName', () => {
        it('should return the azureStorageAccountName', done => {
            const accountName = azureGetStorageAccountName('us-west-1', {
                azureStorageAccountName: 'someaccount',
            });
            assert.deepStrictEqual(accountName, 'someaccount');
            return done();
        });

        it('should use the azureStorageAccountName', done => {
            setEnv('us-west-1_AZURE_STORAGE_ACCOUNT_NAME', 'other');
            setEnv('fr-east-2_AZURE_STORAGE_ACCOUNT_NAME', 'wrong');
            const accountName = azureGetStorageAccountName('us-west-1', {
                azureStorageAccountName: 'someaccount',
            });
            assert.deepStrictEqual(accountName, 'other');
            return done();
        });
    });

    describe('azureGetLocationCredentials', () => {
        const locationDetails = {
            azureStorageAccountName: 'someaccount',
            azureStorageAccessKey: 'ZW5jcnlwdGVkCg==',
            sasToken: '?sig=pouygfcxvbnom&sp=09876',
            tenantId: 'mytenant',
            clientId: 'myclient',
            clientKey: 'sgrecvavwegreqv4t24efeqvqc',
        };

        it('should return shared-key credentials from details', () => {
            const creds = azureGetLocationCredentials('us-west-1', {
                azureStorageAccountName: 'someaccount',
                azureStorageAccessKey: 'ZW5jcnlwdGVkCg==',
            });
            assert.deepStrictEqual(creds, {
                authMethod: 'shared-key',
                storageAccountName: 'someaccount',
                storageAccessKey: 'ZW5jcnlwdGVkCg==',
            });
        });

        it('should return shared-key credentials from env', () => {
            setEnv('us-west-1_AZURE_STORAGE_ACCOUNT_NAME', 'something');
            setEnv('us-west-1_AZURE_STORAGE_ACCESS_KEY', 'ZW5jcnlwdGVkCg==');
            const creds = azureGetLocationCredentials('us-west-1', {});
            assert.deepStrictEqual(creds, {
                authMethod: 'shared-key',
                storageAccountName: 'something',
                storageAccessKey: 'ZW5jcnlwdGVkCg==',
            });
        });

        it('should return shared-key credentials with authMethod from details', () => {
            const creds = azureGetLocationCredentials('us-west-1', {
                authMode: 'shared-key',
                ...locationDetails,
            });
            assert.deepStrictEqual(creds, {
                authMethod: 'shared-key',
                storageAccountName: 'someaccount',
                storageAccessKey: 'ZW5jcnlwdGVkCg==',
            });
        });

        it('should return shared-key credentials with authMethod from env', () => {
            setEnv('us-west-1_AZURE_AUTH_METHOD', 'shared-key');
            const creds = azureGetLocationCredentials('us-west-1', locationDetails);
            assert.deepStrictEqual(creds, {
                authMethod: 'shared-key',
                storageAccountName: 'someaccount',
                storageAccessKey: 'ZW5jcnlwdGVkCg==',
            });
        });

        it('should return shared-access-signature-token credentials from details', () => {
            const creds = azureGetLocationCredentials('us-west-1', {
                sasToken: '?sig=pouygfcxvbnom&sp=09876',
            });
            assert.deepStrictEqual(creds, {
                authMethod: 'shared-access-signature',
                sasToken: '?sig=pouygfcxvbnom&sp=09876',
            });
        });

        it('should return shared-access-signature-token credentials from env', () => {
            setEnv('us-west-1_AZURE_SAS_TOKEN', '?sig=pouygfcxvbnom&sp=09876');
            const creds = azureGetLocationCredentials('us-west-1', {});
            assert.deepStrictEqual(creds, {
                authMethod: 'shared-access-signature',
                sasToken: '?sig=pouygfcxvbnom&sp=09876',
            });
        });

        it('should return shared-access-signature-token credentials with authMethod from details', () => {
            const creds = azureGetLocationCredentials('us-west-1', {
                authMethod: 'shared-access-signature',
                ...locationDetails,
            });
            assert.deepStrictEqual(creds, {
                authMethod: 'shared-access-signature',
                sasToken: '?sig=pouygfcxvbnom&sp=09876',
            });
        });

        it('should return shared-access-signature token credentials with authMethod from env', () => {
            setEnv('us-west-1_AZURE_AUTH_METHOD', 'shared-access-signature');
            const creds = azureGetLocationCredentials('us-west-1', locationDetails);
            assert.deepStrictEqual(creds, {
                authMethod: 'shared-access-signature',
                sasToken: '?sig=pouygfcxvbnom&sp=09876',
            });
        });

        it('should return client-secret credentials from details', () => {
            const creds = azureGetLocationCredentials('us-west-1', {
                tenantId: 'mytenant',
                clientId: 'myclient',
                clientKey: 'sgrecvavwegreqv4t24efeqvqc',
            });
            assert.deepStrictEqual(creds, {
                authMethod: 'client-secret',
                tenantId: 'mytenant',
                clientId: 'myclient',
                clientKey: 'sgrecvavwegreqv4t24efeqvqc',
            });
        });

        it('should return client-secret credentials from env', () => {
            setEnv('us-west-1_AZURE_TENANT_ID', 'mytenant');
            setEnv('us-west-1_AZURE_CLIENT_ID', 'myclient');
            setEnv('us-west-1_AZURE_CLIENT_KEY', 'sgrecvavwegreqv4t24efeqvqc');
            const creds = azureGetLocationCredentials('us-west-1', {});
            assert.deepStrictEqual(creds, {
                authMethod: 'client-secret',
                tenantId: 'mytenant',
                clientId: 'myclient',
                clientKey: 'sgrecvavwegreqv4t24efeqvqc',
            });
        });

        it('should return client-secret credentials with authMethod from details', () => {
            const creds = azureGetLocationCredentials('us-west-1', {
                authMethod: 'client-secret',
                ...locationDetails,
            });
            assert.deepStrictEqual(creds, {
                authMethod: 'client-secret',
                tenantId: 'mytenant',
                clientId: 'myclient',
                clientKey: 'sgrecvavwegreqv4t24efeqvqc',
            });
        });

        it('should return client-secret credentials with authMethod from env', () => {
            setEnv('us-west-1_AZURE_AUTH_METHOD', 'client-secret');
            const creds = azureGetLocationCredentials('us-west-1', locationDetails);
            assert.deepStrictEqual(creds, {
                authMethod: 'client-secret',
                tenantId: 'mytenant',
                clientId: 'myclient',
                clientKey: 'sgrecvavwegreqv4t24efeqvqc',
            });
        });
    });

    describe('getAzureStorageAccountName', () => {
        it('should return account name from config', () => {
            setEnv('azurebackend_AZURE_STORAGE_ACCOUNT_NAME', '');
            const config = new ConfigObject();
            assert.deepStrictEqual(config.getAzureStorageAccountName('azurebackend'), 'fakeaccountname');
        });

        it('should return account name from env', () => {
            setEnv('azurebackend_AZURE_STORAGE_ACCOUNT_NAME', 'foooo');
            const config = new ConfigObject();
            assert.deepStrictEqual(config.getAzureStorageAccountName('azurebackend'), 'foooo');
        });

        it('should return account name from shared-access-signature auth', () => {
            setEnv('S3_LOCATION_FILE', 'tests/locationConfig/locationConfigTests.json');
            const config = new ConfigObject();
            assert.deepStrictEqual(config.getAzureStorageAccountName('azurebackend3'), 'fakeaccountname3');
        });

        it('should return account name from client-secret auth', () => {
            setEnv('S3_LOCATION_FILE', 'tests/locationConfig/locationConfigTests.json');
            const config = new ConfigObject();
            assert.deepStrictEqual(config.getAzureStorageAccountName('azurebackend4'), 'fakeaccountname4');
        });

        it('should return account name from endpoint', () => {
            setEnv('S3_LOCATION_FILE', 'tests/locationConfig/locationConfigTests.json');
            const config = new ConfigObject();
            assert.deepStrictEqual(config.getAzureStorageAccountName('azuritebackend'), 'myfakeaccount');
        });
    });

    describe('locationConstraintAssert', () => {
        const memLocation = {
            details: {},
            isCold: false,
            isTransient: false,
            legacyAwsBehavior: false,
            locationType: 'location-mem-v1',
            objectId: 'a9d9b632-5fa5-11ef-8715-b21941dbc3ea',
            type: 'mem',
        };

        it('should parse tlp location', () => {
            const locationConstraints = {
                'dmf-1': {
                    details: {},
                    isCold: true,
                    legacyAwsBehavior: false,
                    locationType: LOCATION_NAME_DMF,
                    objectId: 'b9d9b632-5fa5-11ef-8715-b21941dbc3ea',
                    type: 'tlp',
                },
                'us-east-1': memLocation,
            };
            locationConstraintAssert(locationConstraints);
        });

        it('should fail tlp location is not cold', () => {
            const locationConstraints = {
                'dmf-1': {
                    details: {},
                    isCold: false,
                    legacyAwsBehavior: false,
                    locationType: LOCATION_NAME_DMF,
                    objectId: 'b9d9b632-5fa5-11ef-8715-b21941dbc3ea',
                    type: 'tlp',
                },
                'us-east-1': memLocation,
            };
            assert.throws(() => locationConstraintAssert(locationConstraints));
        });

        it('should fail if tlp location has details', () => {
            const locationConstraints = {
                'dmf-1': {
                    details: {
                        endpoint: 'http://localhost:8000',
                    },
                    isCold: true,
                    legacyAwsBehavior: false,
                    locationType: LOCATION_NAME_DMF,
                    objectId: 'b9d9b632-5fa5-11ef-8715-b21941dbc3ea',
                    type: 'tlp',
                },
                'us-east-1': memLocation,
            };
            assert.throws(() => locationConstraintAssert(locationConstraints));
        });

        it('should fail if there is no us-east-1 location', () => {
            const locationConstraints = {
                'us-west-1': memLocation,
            };
            assert.throws(() => locationConstraintAssert(locationConstraints));
        });
    });

    describe('time options', () => {
        it('should getTimeOptions', () => {
            const config = new ConfigObject();
            const expectedOptions = {
                expireOneDayEarlier: false,
                transitionOneDayEarlier: false,
                timeProgressionFactor: 1,
                scaledMsPerDay: 86400000,
            };
            const timeOptions = config.getTimeOptions();
            assert.deepStrictEqual(timeOptions, expectedOptions);
        });

        it('should getTimeOptions with TIME_PROGRESSION_FACTOR', () => {
            setEnv('TIME_PROGRESSION_FACTOR', 2);

            const config = new ConfigObject();
            const expectedOptions = {
                expireOneDayEarlier: false,
                transitionOneDayEarlier: false,
                timeProgressionFactor: 2,
                scaledMsPerDay: 43200000,
            };
            const timeOptions = config.getTimeOptions();
            assert.deepStrictEqual(timeOptions, expectedOptions);
        });

        it('should getTimeOptions with EXPIRE_ONE_DAY_EARLIER', () => {
            setEnv('EXPIRE_ONE_DAY_EARLIER', true);

            const config = new ConfigObject();
            const expectedOptions = {
                expireOneDayEarlier: true,
                transitionOneDayEarlier: false,
                timeProgressionFactor: 1,
                scaledMsPerDay: 86400000,
            };
            const timeOptions = config.getTimeOptions();
            assert.deepStrictEqual(timeOptions, expectedOptions);
        });

        it('should getTimeOptions with TRANSITION_ONE_DAY_EARLIER', () => {
            setEnv('TRANSITION_ONE_DAY_EARLIER', true);

            const config = new ConfigObject();
            const expectedOptions = {
                expireOneDayEarlier: false,
                transitionOneDayEarlier: true,
                timeProgressionFactor: 1,
                scaledMsPerDay: 86400000,
            };
            const timeOptions = config.getTimeOptions();
            assert.deepStrictEqual(timeOptions, expectedOptions);
        });

        it('should getTimeOptions with both EXPIRE_ONE_DAY_EARLIER and TRANSITION_ONE_DAY_EARLIER', () => {
            setEnv('EXPIRE_ONE_DAY_EARLIER', true);
            setEnv('TRANSITION_ONE_DAY_EARLIER', true);

            const config = new ConfigObject();
            const expectedOptions = {
                expireOneDayEarlier: true,
                transitionOneDayEarlier: true,
                timeProgressionFactor: 1,
                scaledMsPerDay: 86400000,
            };
            const timeOptions = config.getTimeOptions();
            assert.deepStrictEqual(timeOptions, expectedOptions);
        });

        it('should throw error if EXPIRE_ONE_DAY_EARLIER and TIME_PROGRESSION_FACTOR', () => {
            setEnv('EXPIRE_ONE_DAY_EARLIER', true);
            setEnv('TIME_PROGRESSION_FACTOR', 2);

            assert.throws(() => new ConfigObject());
        });

        it('should throw error if TRANSITION_ONE_DAY_EARLIER and TIME_PROGRESSION_FACTOR', () => {
            setEnv('TRANSITION_ONE_DAY_EARLIER', true);
            setEnv('TIME_PROGRESSION_FACTOR', 2);

            assert.throws(() => new ConfigObject());
        });

        it('should throw error if both EXPIRE/TRANSITION_ONE_DAY_EARLIER and TIME_PROGRESSION_FACTOR', () => {
            setEnv('EXPIRE_ONE_DAY_EARLIER', true);
            setEnv('TRANSITION_ONE_DAY_EARLIER', true);
            setEnv('TIME_PROGRESSION_FACTOR', 2);

            assert.throws(() => new ConfigObject());
        });
    });

    describe('nullVersionCompatMode', () => {
        it('should be true when metadata backend is mongodb', () => {
            setEnv('S3METADATA', 'mongodb');
            setEnv('ENABLE_NULL_VERSION_COMPAT_MODE', 'false'); // should be ignored
            const config = new ConfigObject();
            assert.strictEqual(config.nullVersionCompatMode, true);
        });

        it('should be true when ENABLE_NULL_VERSION_COMPAT_MODE is true', () => {
            setEnv('S3METADATA', 'file'); // Not MongoDB
            setEnv('ENABLE_NULL_VERSION_COMPAT_MODE', 'true');
            const config = new ConfigObject();
            assert.strictEqual(config.nullVersionCompatMode, true);
        });

        it('should be false when ENABLE_NULL_VERSION_COMPAT_MODE is false', () => {
            setEnv('S3METADATA', 'file'); // Not MongoDB
            setEnv('ENABLE_NULL_VERSION_COMPAT_MODE', 'false');
            const config = new ConfigObject();
            assert.strictEqual(config.nullVersionCompatMode, false);
        });

        it('should be false when metadata backend is not mongodb and env var not set', () => {
            setEnv('S3METADATA', 'file'); // Not MongoDB
            const config = new ConfigObject();
            assert.strictEqual(config.nullVersionCompatMode, false);
        });
    });

    describe('multiObjectDeleteEnableOptimizations', () => {
        let sandbox;
        let readFileStub;

        beforeEach(() => {
            sandbox = sinon.createSandbox();
            readFileStub = sandbox.stub(fs, 'readFileSync');
            readFileStub.callThrough();
        });

        afterEach(() => {
            sandbox.restore();
        });

        it('should be true by default when metadata backend is mongodb', () => {
            process.env.S3METADATA = 'mongodb';
            const config = new ConfigObject();
            assert.strictEqual(config.multiObjectDeleteEnableOptimizations, true);
        });

        it('should be false by default when metadata backend is not mongodb', () => {
            process.env.S3METADATA = 'file';
            const config = new ConfigObject();
            assert.strictEqual(config.multiObjectDeleteEnableOptimizations, false);
        });

        it('should respect config file setting when set to false', () => {
            process.env.S3METADATA = 'mongodb';
            const modifiedConfig = { ...defaultConfig, multiObjectDeleteEnableOptimizations: false };
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(modifiedConfig));

            const config = new ConfigObject();
            assert.strictEqual(config.multiObjectDeleteEnableOptimizations, false);
        });

        it('should respect config file setting when set to true', () => {
            process.env.S3METADATA = 'mongodb';
            const modifiedConfig = { ...defaultConfig, multiObjectDeleteEnableOptimizations: true };
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(modifiedConfig));

            const config = new ConfigObject();
            assert.strictEqual(config.multiObjectDeleteEnableOptimizations, true);
        });

        it('should ignore config file setting for non-mongodb backend', () => {
            process.env.S3METADATA = 'file';
            const modifiedConfig = { ...defaultConfig, multiObjectDeleteEnableOptimizations: true };
            readFileStub.withArgs('config.json').returns(JSON.stringify(modifiedConfig));

            const config = new ConfigObject();
            assert.strictEqual(config.multiObjectDeleteEnableOptimizations, false);
        });
    });

    describe('scuba option setup', () => {
        let oldConfig;

        before(() => {
            oldConfig = process.env.S3_CONFIG_FILE;
            process.env.S3_CONFIG_FILE = 'tests/unit/testConfigs/allOptsConfig/config.json';
        });

        after(() => {
            process.env.S3_CONFIG_FILE = oldConfig;
        });

        it('should set up scuba', () => {
            const config = new ConfigObject();

            assert.deepStrictEqual(config.scuba, {
                host: 'localhost',
                port: 8100,
            });
        });

        it('should use environment variables for scuba', () => {
            setEnv('SCUBA_HOST', 'scubahost');
            setEnv('SCUBA_PORT', 1234);

            const config = new ConfigObject();

            assert.deepStrictEqual(config.scuba, {
                host: 'scubahost',
                port: 1234,
            });
        });
    });

    describe('quota option setup', () => {
        let oldConfig;

        before(() => {
            oldConfig = process.env.S3_CONFIG_FILE;
            process.env.S3_CONFIG_FILE = 'tests/unit/testConfigs/allOptsConfig/config.json';
        });

        after(() => {
            process.env.S3_CONFIG_FILE = oldConfig;
        });

        it('should set up quota from the config file', () => {
            assert.notStrictEqual(allOptsQuota.maxStalenessMS, constants.defaultQuotaMaxStalenessMS);

            const config = new ConfigObject();

            assert.deepStrictEqual(config.quota, {
                maxStaleness: allOptsQuota.maxStalenessMS,
                enableInflights: false,
            });
        });

        it('should use the default when the config file does not set maxStalenessMS', () => {
            setEnv('S3_CONFIG_FILE', 'config.json');

            const config = new ConfigObject();

            assert.deepStrictEqual(config.quota, {
                maxStaleness: constants.defaultQuotaMaxStalenessMS,
                enableInflights: false,
            });
        });

        it('should use environment variables for quota', () => {
            setEnv('QUOTA_MAX_STALENESS_MS', 1234);
            setEnv('QUOTA_ENABLE_INFLIGHTS', 'true');

            const config = new ConfigObject();

            assert.deepStrictEqual(config.quota, {
                maxStaleness: 1234,
                enableInflights: true,
            });
        });

        it('should fall back to the config file when QUOTA_MAX_STALENESS_MS is empty', () => {
            setEnv('QUOTA_MAX_STALENESS_MS', '');

            const config = new ConfigObject();

            assert.deepStrictEqual(config.quota, {
                maxStaleness: allOptsQuota.maxStalenessMS,
                enableInflights: false,
            });
        });

        ['notanumber', '0', '-1', '1.5'].forEach(value => {
            it(`should reject a maxStaleness of '${value}'`, () => {
                setEnv('QUOTA_MAX_STALENESS_MS', value);

                assert.throws(() => new ConfigObject(), /bad config: quota.maxStalenessMS must be a positive integer/);
            });
        });
    });

    describe('utapi option setup', () => {
        let oldConfig;

        before(() => {
            oldConfig = process.env.S3_CONFIG_FILE;
            process.env.S3_CONFIG_FILE = 'tests/unit/testConfigs/allOptsConfig/config.json';
        });

        after(() => {
            process.env.S3_CONFIG_FILE = oldConfig;
        });

        it('should set up utapi local cache', () => {
            const config = new ConfigObject();

            assert.deepStrictEqual(config.localCache, {
                name: 'zenko',
                sentinels: [{ host: 'localhost', port: 6379 }],
            });
            assert.deepStrictEqual(config.utapi.localCache, config.localCache);
        });

        it('should set up utapi redis', () => {
            const config = new ConfigObject();

            assert.deepStrictEqual(config.utapi.redis, {
                host: 'localhost',
                port: 6379,
                retry: {
                    connectBackoff: {
                        min: 10,
                        max: 1000,
                        factor: 1.5,
                        jitter: 0.1,
                        deadline: 10000,
                    },
                },
            });
        });
    });

    it('should have a default overlay version', () => {
        const { config } = require('../../lib/Config');
        assert.strictEqual(config.overlayVersion, 0);
    });

    describe('parseSupportedLifecycleRules', () => {
        it('should throw when an empty array is provided', () => {
            assert.throws(() => parseSupportedLifecycleRules([]));
        });

        it('should use default values when no value is provided', () => {
            const rules = parseSupportedLifecycleRules(undefined);
            assert.deepStrictEqual(rules, [...supportedLifecycleRules]);
        });

        it('should throw when rule name is not valid', () => {
            const rules = ['invalid'];
            assert.throws(() => parseSupportedLifecycleRules(rules));
        });

        it('should return the rules provided when they are valid', () => {
            const rules = ['Expiration', 'NoncurrentVersionExpiration', 'AbortIncompleteMultipartUpload'];
            const parsedRules = parseSupportedLifecycleRules(rules);
            assert.deepStrictEqual(parsedRules, rules);
        });
    });

    describe('port and internalPort configuration', () => {
        // same as `defaultConfig` (i.e. config.json) but without the port settings
        const baseConfig = (() => {
            const config = { ...defaultConfig };
            delete config.port;
            delete config.listenOn;
            delete config.internalPort;
            delete config.internalListenOn;
            delete config.metricsPort;
            delete config.metricsListenOn;
            return config;
        })();

        let sandbox;
        let readFileStub;

        beforeEach(() => {
            sandbox = sinon.createSandbox();
            readFileStub = sandbox.stub(fs, 'readFileSync');
            readFileStub.callThrough();
        });

        afterEach(() => {
            sandbox.restore();
        });

        it('should throw an error for negative port number', () => {
            const testConfig = { ...baseConfig, port: -1 };
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(testConfig));

            assert.throws(() => new ConfigObject(), /bad config: port must be a positive integer/);
        });

        it('should throw an error for non-integer port number', () => {
            const testConfig = { ...baseConfig, port: 8000.5 };
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(testConfig));

            assert.throws(() => new ConfigObject(), /bad config: port must be a positive integer/);
        });

        it('should throw an error for negative internalPort number', () => {
            const testConfig = { ...baseConfig, internalPort: -1 };
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(testConfig));

            assert.throws(() => new ConfigObject(), /bad config: internalPort must be a positive integer/);
        });

        it('should throw an error for non-integer internalPort number', () => {
            const testConfig = { ...baseConfig, internalPort: 9000.5 };
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(testConfig));

            assert.throws(() => new ConfigObject(), /bad config: internalPort must be a positive integer/);
        });

        it('should accept a valid port number', () => {
            const testConfig = { ...baseConfig, port: 8765 };
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(testConfig));

            const config = new ConfigObject();
            assert.strictEqual(config.port, 8765);
            assert.strictEqual(config.internalPort, undefined);
        });

        it('should accept a valid internalPort number', () => {
            const testConfig = { ...baseConfig, internalPort: 9000 };
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(testConfig));

            const config = new ConfigObject();
            assert.strictEqual(config.port, undefined);
            assert.strictEqual(config.internalPort, 9000);
        });

        it('should accept both port and internalPort when provided', () => {
            const testConfig = { ...baseConfig, port: 8000, internalPort: 9000 };
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(testConfig));

            const config = new ConfigObject();
            assert.strictEqual(config.port, 8000);
            assert.strictEqual(config.internalPort, 9000);
        });

        it('should use default port 8000 if no port or internalPort specified', () => {
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(baseConfig));

            const config = new ConfigObject();
            assert.strictEqual(config.port, 8000);
            assert.strictEqual(config.internalPort, undefined);
        });

        it('should default port if listenOn is provided', () => {
            const testConfig = {
                ...baseConfig,
                listenOn: ['127.0.0.1:9000'],
            };
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(testConfig));

            const config = new ConfigObject();
            assert.strictEqual(config.port, 8000);
            assert.strictEqual(config.internalPort, undefined);
            assert.deepEqual(config.listenOn, [{ ip: '127.0.0.1', port: 9000 }]);
        });

        it('should not default port if internalListenOn is provided', () => {
            const testConfig = {
                ...baseConfig,
                internalListenOn: ['127.0.0.1:9000'],
            };
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(testConfig));

            const config = new ConfigObject();
            assert.strictEqual(config.port, undefined);
            assert.strictEqual(config.internalPort, undefined);
            assert.deepEqual(config.internalListenOn, [{ ip: '127.0.0.1', port: 9000 }]);
        });

        it('should properly handle listenOn and internalListenOn configuration', () => {
            const testConfig = {
                ...baseConfig,
                listenOn: ['0.0.0.0:8000'],
                internalListenOn: ['127.0.0.1:9000'],
            };
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(testConfig));

            const config = new ConfigObject();
            assert.strictEqual(config.port, 8000);
            assert.deepEqual(config.listenOn, [{ ip: '0.0.0.0', port: 8000 }]);
            assert.deepEqual(config.internalListenOn, [{ ip: '127.0.0.1', port: 9000 }]);
        });
    });

    describe('instanceId', () => {
        let defaultConfig;

        before(() => {
            setEnv('S3_CONFIG_FILE', 'tests/unit/testConfigs/allOptsConfig/config.json');
            // Import config
            const defaultConfigPath = path.join(__dirname, './testConfigs/allOptsConfig/config.json');
            defaultConfig = require(defaultConfigPath);
        });

        afterEach(() => {
            sinon.restore();
        });

        it('should use the instance id set in the config', () => {
            // Stub fs.readFileSync to return a specific config.json content that includes instanceId
            const originalReadFileSync = fs.readFileSync;
            const readFileSyncStub = sinon.stub(fs, 'readFileSync');
            // Mock only config.json file
            readFileSyncStub
                .withArgs(sinon.match(/\/config\.json$/))
                .returns(JSON.stringify({ ...defaultConfig, instanceId: 'test' }));
            // For all other files, use the original readFileSync
            readFileSyncStub.callsFake((filePath, ...args) => originalReadFileSync(filePath, ...args));
            // Create a new ConfigObject instance
            const config = new ConfigObject();
            assert.strictEqual(config.instanceId, 'test');
        });

        it('should assert if instanceId is not a string', () => {
            // Stub fs.readFileSync to return a specific config.json content that includes instanceId
            const originalReadFileSync = fs.readFileSync;
            const readFileSyncStub = sinon.stub(fs, 'readFileSync');
            // Mock only config.json file
            readFileSyncStub
                .withArgs(sinon.match(/\/config\.json$/))
                .returns(JSON.stringify({ ...defaultConfig, instanceId: 1234 }));
            // For all other files, use the original readFileSync
            readFileSyncStub.callsFake((filePath, ...args) => originalReadFileSync(filePath, ...args));
            // Create a new ConfigObject instance
            assert.throws(() => new ConfigObject());
        });

        it('should use the instance id set in the env var', () => {
            setEnv('CLOUDSERVER_INSTANCE_ID', '123456');
            const config = new ConfigObject();
            assert.strictEqual(config.instanceId, '123456');
        });

        it('should assert if instanceId is longer than 6 characters', () => {
            setEnv('CLOUDSERVER_INSTANCE_ID', '1234567');
            assert.throws(() => new ConfigObject());
        });

        it('should generate a random instanceId if not set', () => {
            const config = new ConfigObject();
            assert.strictEqual(config.instanceId.length, 6);
        });
    });

    describe('apisLengthLimits configuration', () => {
        let sandbox;
        let readFileStub;

        beforeEach(() => {
            sandbox = sinon.createSandbox();
            readFileStub = sandbox.stub(fs, 'readFileSync');
            readFileStub.callThrough();
        });

        afterEach(() => {
            sandbox.restore();
        });

        it('should use default API and overwrite when config is provided', () => {
            const multiObjectDeleteSize = 42;
            const modifiedConfig = {
                ...defaultConfig,
                apiBodySizeLimits: { multiObjectDelete: multiObjectDeleteSize },
            };
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(modifiedConfig));
            const config = new ConfigObject();

            assert.deepStrictEqual(config.apiBodySizeLimits, {
                multiObjectDelete: multiObjectDeleteSize, // Configured: overwrites default
                bucketPutPolicy: constants.defaultApiBodySizeLimits['bucketPutPolicy'], // Not configured: default
            });
        });

        it('should fail if a user tries to modify a non-existent API', () => {
            const modifiedConfig = {
                ...defaultConfig,
                apiBodySizeLimits: { 'anApiNotSetInConstants.js': 42 },
            };
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(modifiedConfig));

            assert.throws(
                () => new ConfigObject(),
                /bad config: apiBodySizeLimits for "anApiNotSetInConstants.js" cannot be configured/,
            );
        });
    });

    describe('objectKeyByteLimit', () => {
        it('should use default objectKeyByteLimit (915) from arsenal constants', () => {
            const config = new ConfigObject();
            assert.strictEqual(config.objectKeyByteLimit, 915);
        });

        it('should override objectKeyByteLimit from environment variable', () => {
            setEnv('OVERRIDE_OBJECT_KEY_BYTE_LIMIT', '2048');
            const config = new ConfigObject();
            assert.strictEqual(config.objectKeyByteLimit, 2048);
        });

        it('should allow setting objectKeyByteLimit to 0 (no limit)', () => {
            setEnv('OVERRIDE_OBJECT_KEY_BYTE_LIMIT', '0');
            const config = new ConfigObject();
            assert.strictEqual(config.objectKeyByteLimit, 0);
        });

        it('should throw error for negative objectKeyByteLimit override', () => {
            setEnv('OVERRIDE_OBJECT_KEY_BYTE_LIMIT', '-1');
            assert.throws(() => new ConfigObject());
        });

        it('should throw error for invalid string objectKeyByteLimit override', () => {
            setEnv('OVERRIDE_OBJECT_KEY_BYTE_LIMIT', 'invalid');
            assert.throws(() => new ConfigObject());
        });
    });

    describe('parse integrity checks', () => {
        afterEach(() => {
            delete process.env.S3_INTEGRITY_CHECKS_ENABLED;
        });

        it('should default to enabled when not configured', () => {
            assert.deepStrictEqual(parseIntegrityChecks(null), { enabled: true });
            assert.deepStrictEqual(parseIntegrityChecks({}), { enabled: true });
        });

        it('should read the configured value', () => {
            assert.deepStrictEqual(parseIntegrityChecks({ integrityChecks: { enabled: false } }), { enabled: false });
            assert.deepStrictEqual(parseIntegrityChecks({ integrityChecks: { enabled: true } }), { enabled: true });
        });

        it('should throw if integrityChecks is not an object', () => {
            assert.throws(() => parseIntegrityChecks({ integrityChecks: 'yes' }), /must be an object/);
            assert.throws(() => parseIntegrityChecks({ integrityChecks: [true] }), /must be an object/);
        });

        it('should throw if enabled is not a boolean', () => {
            assert.throws(() => parseIntegrityChecks({ integrityChecks: { enabled: 'false' } }), /must be a boolean/);
            assert.throws(() => parseIntegrityChecks({ integrityChecks: { enabled: 0 } }), /must be a boolean/);
        });

        it('should let S3_INTEGRITY_CHECKS_ENABLED override the config file', () => {
            process.env.S3_INTEGRITY_CHECKS_ENABLED = 'false';
            assert.deepStrictEqual(parseIntegrityChecks({ integrityChecks: { enabled: true } }), { enabled: false });
            process.env.S3_INTEGRITY_CHECKS_ENABLED = 'true';
            assert.deepStrictEqual(parseIntegrityChecks({ integrityChecks: { enabled: false } }), { enabled: true });
        });

        it('should throw on a non-boolean S3_INTEGRITY_CHECKS_ENABLED', () => {
            process.env.S3_INTEGRITY_CHECKS_ENABLED = 'nope';
            assert.throws(() => parseIntegrityChecks(null), /S3_INTEGRITY_CHECKS_ENABLED/);
        });

        it('should expose integrityChecks on the config object', () => {
            const config = new ConfigObject();
            assert.deepStrictEqual(config.integrityChecks, { enabled: true });
        });
    });

    describe('serverHeader', () => {
        let sandbox;
        let readFileStub;

        beforeEach(() => {
            sandbox = sinon.createSandbox();
            readFileStub = sandbox.stub(fs, 'readFileSync');
            readFileStub.callThrough();
        });

        afterEach(() => {
            sandbox.restore();
        });

        it('should default to "S3 Server" when not configured', () => {
            const config = new ConfigObject();
            assert.strictEqual(config.serverHeader, 'S3 Server');
        });

        it('should use the configured value when set', () => {
            const modifiedConfig = { ...defaultConfig, serverHeader: 'ScalityS3' };
            readFileStub.withArgs(sinon.match(/config.json$/)).returns(JSON.stringify(modifiedConfig));
            const config = new ConfigObject();
            assert.strictEqual(config.serverHeader, 'ScalityS3');
        });
    });
});
