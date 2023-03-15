const assert = require('assert');
const { azureArchiveLocationConstraintAssert } = require('../../lib/Config');

describe('Config', () => {
    const envToRestore = [];
    const setEnv = (key, value) => {
        if (key in process.env) {
            const v = process.env[key];
            envToRestore.push(() => { process.env[key] = v; });
        } else {
            envToRestore.push(() => { delete process.env[key]; });
        }
        process.env[key] = value;
    };

    beforeEach(() => { envToRestore.length = 0; });
    afterEach(() => { envToRestore.reverse().forEach(cb => cb()); });

    it('should load default config.json without errors', done => {
        require('../../lib/Config');
        done();
    });

    it('should emit an event when auth data is updated', done => {
        const { ConfigObject } = require('../../lib/Config');
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
        const { azureGetStorageAccountName } = require('../../lib/Config');

        it('should return the azureStorageAccountName', done => {
            const accountName = azureGetStorageAccountName('us-west-1', {
                azureStorageAccountName: 'someaccount'
            });
            assert.deepStrictEqual(accountName, 'someaccount');
            return done();
        });

        it('should use the azureStorageAccountName', done => {
            setEnv('us-west-1_AZURE_STORAGE_ACCOUNT_NAME', 'other');
            setEnv('fr-east-2_AZURE_STORAGE_ACCOUNT_NAME', 'wrong');
            const accountName = azureGetStorageAccountName('us-west-1', {
                azureStorageAccountName: 'someaccount'
            });
            assert.deepStrictEqual(accountName, 'other');
            return done();
        });
    });

    describe('azureGetLocationCredentials', () => {
        const { azureGetLocationCredentials } = require('../../lib/Config');

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
                ...locationDetails
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
                ...locationDetails
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
                ...locationDetails
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
        const { ConfigObject } = require('../../lib/Config');

        it('should return account name from config', () => {
            setEnv('azurebackend_AZURE_STORAGE_ACCOUNT_NAME', '');
            const config = new ConfigObject();
            assert.deepStrictEqual(
                config.getAzureStorageAccountName('azurebackend'),
                'fakeaccountname'
            );
        });

        it('should return account name from env', () => {
            setEnv('azurebackend_AZURE_STORAGE_ACCOUNT_NAME', 'foooo');
            const config = new ConfigObject();
            assert.deepStrictEqual(
                config.getAzureStorageAccountName('azurebackend'),
                'foooo'
            );
        });

        it('should return account name from shared-access-signature auth', () => {
            setEnv('S3_LOCATION_FILE', 'tests/locationConfig/locationConfigTests.json');
            const config = new ConfigObject();
            assert.deepStrictEqual(
                config.getAzureStorageAccountName('azurebackend3'),
                'fakeaccountname3'
            );
        });

        it('should return account name from client-secret auth', () => {
            setEnv('S3_LOCATION_FILE', 'tests/locationConfig/locationConfigTests.json');
            const config = new ConfigObject();
            assert.deepStrictEqual(
                config.getAzureStorageAccountName('azurebackend4'),
                'fakeaccountname4',
            );
        });

        it('should return account name from endpoint', () => {
            setEnv('S3_LOCATION_FILE', 'tests/locationConfig/locationConfigTests.json');
            const config = new ConfigObject();
            assert.deepStrictEqual(
                config.getAzureStorageAccountName('azuritebackend'),
                'myfakeaccount',
            );
        });
    });

    describe('utapi option setup', () => {
        let oldConfig;

        before(() => {
            oldConfig = process.env.S3_CONFIG_FILE;
            process.env.S3_CONFIG_FILE =
                'tests/unit/testConfigs/allOptsConfig/config.json';
        });

        after(() => {
            process.env.S3_CONFIG_FILE = oldConfig;
        });

        it('should set up utapi local cache', () => {
            const { ConfigObject } = require('../../lib/Config');
            const config = new ConfigObject();

            assert.deepStrictEqual(
                config.localCache,
                { name: 'zenko', sentinels: [{ host: 'localhost', port: 6379 }] },
            );
            assert.deepStrictEqual(
                config.utapi.localCache,
                config.localCache,
            );
        });

        it('should set up utapi redis', () => {
            const { ConfigObject } = require('../../lib/Config');
            const config = new ConfigObject();

            assert.deepStrictEqual(
                config.redis,
                { name: 'zenko', sentinels: [{ host: 'localhost', port: 6379 }] },
            );
            assert.deepStrictEqual(
                config.utapi.redis,
                {
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
                },
            );
        });
    });

    it('should have a default overlay version', () => {
        const { config } = require('../../lib/Config');
        assert.strictEqual(config.overlayVersion, 0);
    });

    describe('azureArchiveLocationConstraintAssert', () => {
        it('should succeed azureStorageEndpoint is missing', () => {
            const locationObj = {
                details: {
                    azureContainerName: 'mycontainer',
                }
            };
            assert.strictEqual(azureArchiveLocationConstraintAssert(locationObj), undefined);
        });

        it('should succeed if azureContainerName is missing', () => {
            const locationObj = {
                details: {
                    azureStorageEndpoint: 'http://test.com/',
                }
            };
            assert.strictEqual(azureArchiveLocationConstraintAssert(locationObj), undefined);
        });

        it('should succeed if both azureStorageEndpoint and azureContainerName are missing', () => {
            const locationObj = {
                details: {
                }
            };
            assert.strictEqual(azureArchiveLocationConstraintAssert(locationObj), undefined);
        });

        it('should fail assert if azureStorageEndpoint is not a string', () => {
            const locationObj = {
                details: {
                    azureContainerName: 'mycontainer',
                    azureStorageEndpoint: true,
                }
            };
            assert.throws(() => azureArchiveLocationConstraintAssert(locationObj));
        });

        it('should assert if azureContainerName is not a string', () => {
            const locationObj = {
                details: {
                    azureContainerName: 42,
                    azureStorageEndpoint: 'endpoint',
                }
            };
            assert.throws(() => azureArchiveLocationConstraintAssert(locationObj));
        });

        it('should assert if missing an authentication method', () => {
            const locationObj = {
                details: {
                    azureContainerName: 'mycontainer',
                    azureStorageEndpoint: 'endpoint',
                }
            };
            assert.throws(() => azureArchiveLocationConstraintAssert(locationObj));
        });

        it('should assert if sasToken is not a string', () => {
            const locationObj = {
                details: {
                    azureContainerName: 'mycontainer',
                    azureStorageEndpoint: 'endpoint',
                    sasToken: 42,
                }
            };
            assert.throws(() => azureArchiveLocationConstraintAssert(locationObj));
        });

        it('should assert if missing azureStorageAccountName but azureStorageAccessKey defined', () => {
            const locationObj = {
                details: {
                    azureContainerName: 'mycontainer',
                    azureStorageEndpoint: 'endpoint',
                    azureStorageAccessKey: 'key',
                }
            };
            assert.throws(() => azureArchiveLocationConstraintAssert(locationObj));
        });

        it('should assert if azureStorageAccountName is not a string', () => {
            const locationObj = {
                details: {
                    azureContainerName: 'mycontainer',
                    azureStorageEndpoint: 'endpoint',
                    azureStorageAccountName: 42,
                    azureStorageAccessKey: 'key',
                }
            };
            assert.throws(() => azureArchiveLocationConstraintAssert(locationObj));
        });

        it('should assert if azureStorageAccessKey is not a string', () => {
            const locationObj = {
                details: {
                    azureContainerName: 'mycontainer',
                    azureStorageEndpoint: 'endpoint',
                    azureStorageAccountName: 'account',
                    azureStorageAccessKey: false,
                }
            };
            assert.throws(() => azureArchiveLocationConstraintAssert(locationObj));
        });

        it('should assert if tenantId is not a string', () => {
            const locationObj = {
                details: {
                    azureContainerName: 'mycontainer',
                    azureStorageEndpoint: 'endpoint',
                    tenantId: 42,
                    clientId: 'client',
                    clientKey: 'key',
                }
            };
            assert.throws(() => azureArchiveLocationConstraintAssert(locationObj));
        });

        it('should assert if tenantId is not a string', () => {
            const locationObj = {
                details: {
                    azureContainerName: 'mycontainer',
                    azureStorageEndpoint: 'endpoint',
                    tenantId: 'tenant',
                    clientId: 42,
                    clientKey: 'key',
                }
            };
            assert.throws(() => azureArchiveLocationConstraintAssert(locationObj));
        });

        it('should assert if tenantId is not a string', () => {
            const locationObj = {
                details: {
                    azureContainerName: 'mycontainer',
                    azureStorageEndpoint: 'endpoint',
                    tenantId: 'tenant',
                    clientId: 'client',
                    clientKey: 42,
                }
            };
            assert.throws(() => azureArchiveLocationConstraintAssert(locationObj));
        });

        it('should assert if multiple auth method are provided', () => {
            const locationObj = {
                details: {
                    azureContainerName: 'mycontainer',
                    azureStorageEndpoint: 'endpoint',
                    tenantId: 'tenant',
                    clientId: 'client',
                    clientKey: 'key',
                    azureStorageAccountName: 'account',
                    azureStorageAccessKey: 'key',
                }
            };
            assert.throws(() => azureArchiveLocationConstraintAssert(locationObj));
        });
    });
});
