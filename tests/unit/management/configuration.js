const assert = require('assert');

const { DummyRequestLogger } = require('../helpers');
const log = new DummyRequestLogger();

const metadata = require('../../../lib/metadata/wrapper');
const managementDatabaseName = 'PENSIEVE';
const tokenConfigurationKey = 'auth/zenko/remote-management-token';

const { privateKey, accessKey, decryptedSecretKey, secretKey, canonicalId,
    userName } = require('./resources.json');
const shortid = '123456789012';
const email = 'customaccount1@setbyenv.com';
const arn = 'arn:aws:iam::123456789012:root';
const { config } = require('../../../lib/Config');

const {
    remoteOverlayIsNewer,
    patchConfiguration,
} = require('../../../lib/management/configuration');

const {
    initManagementDatabase,
} = require('../../../lib/management/index');

function initManagementCredentialsMock(cb) {
    return metadata.putObjectMD(managementDatabaseName,
        tokenConfigurationKey, { privateKey }, {},
        log, error => cb(error));
}

function getConfig() {
    return config;
}

// Original Config
const overlayVersionOriginal = Object.assign({}, config.overlayVersion);
const authDataOriginal = Object.assign({}, config.authData);
const locationConstraintsOriginal = Object.assign({},
    config.locationConstraints);
const restEndpointsOriginal = Object.assign({}, config.restEndpoints);
const browserAccessEnabledOriginal = config.browserAccessEnabled;
function resetConfig() {
    config.overlayVersion = overlayVersionOriginal;
    config.authData = authDataOriginal;
    config.locationConstraints = locationConstraintsOriginal;
    config.restEndpoints = restEndpointsOriginal;
    config.browserAccessEnabled = browserAccessEnabledOriginal;
}

function assertConfig(actualConf, expectedConf) {
    Object.keys(expectedConf).forEach(key => {
        assert.deepEqual(actualConf[key], expectedConf[key]);
    });
}

function checkNoError(err) {
    assert.strictEqual(err, null, 'Expected success ' +
        `but got error ${err}`);
}

describe('patchConfiguration', () => {
    before(done => initManagementDatabase(log, err => {
        if (err) {
            return done(err);
        }
        return initManagementCredentialsMock(done);
    }));
    beforeEach(() => {
        resetConfig();
    });
    it('should modify config using the new config', done => {
        const newConf = {
            version: 1,
            users: [
                {
                    secretKey,
                    accessKey,
                    canonicalId,
                    userName,
                },
            ],
            endpoints: [
                {
                    hostname: '1.1.1.1',
                    locationName: 'us-east-1',
                },
            ],
            locations: {
                'legacy': {
                    name: 'legacy',
                    locationType: 'location-mem-v1',
                },
                'us-east-1': {
                    name: 'us-east-1',
                    locationType: 'location-file-v1',
                    legacyAwsBehavior: true,
                },
                'azurebackendtest': {
                    name: 'azurebackendtest',
                    locationType: 'location-azure-v1',
                    details: {
                        bucketMatch: 'azurebucketmatch',
                        endpoint: 'azure.end.point',
                        accessKey: 'azureaccesskey',
                        secretKey,
                        bucketName: 'azurebucketname',
                    },
                },
                'awsbackendtest': {
                    name: 'awsbackendtest',
                    locationType: 'location-aws-s3-v1',
                    details: {
                        bucketMatch: 'awsbucketmatch',
                        endpoint: 'aws.end.point',
                        accessKey: 'awsaccesskey',
                        secretKey,
                        bucketName: 'awsbucketname',
                    },
                },
                'gcpbackendtest': {
                    name: 'gcpbackendtest',
                    locationType: 'location-gcp-v1',
                    details: {
                        bucketMatch: 'gcpbucketmatch',
                        endpoint: 'gcp.end.point',
                        accessKey: 'gcpaccesskey',
                        secretKey,
                        bucketName: 'gcpbucketname',
                    },
                },
                'sproxydbackendtest': {
                    name: 'sproxydbackendtest',
                    locationType: 'location-scality-sproxyd-v1',
                    details: {
                        chordCos: 3,
                        bootstrapList: ['localhost:8001', 'localhost:8002'],
                        proxyPath: '/proxy/path',
                    },
                },
                'transienttest': {
                    name: 'transienttest',
                    locationType: 'location-file-v1',
                    isTransient: true,
                },
                'sizelimitedtest': {
                    name: 'sizelimitedtest',
                    locationType: 'location-file-v1',
                    sizeLimitGB: 1024,
                },
                'sizezerotest': {
                    name: 'sizezerotest',
                    locationType: 'location-file-v1',
                    sizeLimitGB: 0,
                },
            },
            browserAccess: {
                enabled: true,
            },
        };
        return patchConfiguration(newConf, log, err => {
            checkNoError(err);
            const actualConf = getConfig();
            const expectedConf = {
                overlayVersion: 1,
                browserAccessEnabled: true,
                authData: {
                    accounts: [{
                        name: userName,
                        email,
                        arn,
                        canonicalID: canonicalId,
                        shortid,
                        keys: [{
                            access: accessKey,
                            secret: decryptedSecretKey,
                        }],
                    }],
                },
                locationConstraints: {
                    'legacy': {
                        type: 'mem',
                        legacyAwsBehavior: false,
                        isTransient: false,
                        sizeLimitGB: null,
                    },
                    'us-east-1': {
                        type: 'file',
                        legacyAwsBehavior: true,
                        isTransient: false,
                        sizeLimitGB: null,
                    },
                    'azurebackendtest': {
                        details: {
                            azureContainerName: 'azurebucketname',
                            azureStorageAccessKey: decryptedSecretKey,
                            azureStorageAccountName: 'azureaccesskey',
                            azureStorageEndpoint: 'azure.end.point',
                            bucketMatch: 'azurebucketmatch',
                        },
                        legacyAwsBehavior: false,
                        isTransient: false,
                        sizeLimitGB: null,
                        type: 'azure',
                    },
                    'awsbackendtest': {
                        details: {
                            awsEndpoint: 'aws.end.point',
                            bucketMatch: 'awsbucketmatch',
                            bucketName: 'awsbucketname',
                            credentials: {
                                accessKey: 'awsaccesskey',
                                secretKey: decryptedSecretKey,
                            },
                            https: true,
                            pathStyle: false,
                            serverSideEncryption: false,
                            supportsVersioning: true,
                        },
                        legacyAwsBehavior: false,
                        isTransient: false,
                        sizeLimitGB: null,
                        type: 'aws_s3',
                    },
                    'gcpbackendtest': {
                        details: {
                            bucketMatch: 'gcpbucketmatch',
                            bucketName: 'gcpbucketname',
                            credentials: {
                                accessKey: 'gcpaccesskey',
                                secretKey: decryptedSecretKey,
                            },
                            gcpEndpoint: 'gcp.end.point',
                            mpuBucketName: undefined,
                        },
                        legacyAwsBehavior: false,
                        isTransient: false,
                        sizeLimitGB: null,
                        type: 'gcp',
                    },
                    'sproxydbackendtest': {
                        details: {
                            connector: {
                                sproxyd: {
                                    chordCos: 3,
                                    bootstrap: [
                                        'localhost:8001',
                                        'localhost:8002',
                                    ],
                                    path: '/proxy/path',
                                },
                            },
                            supportsVersioning: true,
                        },
                        legacyAwsBehavior: false,
                        isTransient: false,
                        sizeLimitGB: null,
                        type: 'scality',
                    },
                    'transienttest': {
                        type: 'file',
                        legacyAwsBehavior: false,
                        isTransient: true,
                        sizeLimitGB: null,
                    },
                    'sizelimitedtest': {
                        type: 'file',
                        legacyAwsBehavior: false,
                        isTransient: false,
                        sizeLimitGB: 1024,
                    },
                    'sizezerotest': {
                        type: 'file',
                        legacyAwsBehavior: false,
                        isTransient: false,
                        sizeLimitGB: null,
                    },
                },
            };
            assertConfig(actualConf, expectedConf);
            assert.deepStrictEqual(actualConf.restEndpoints['1.1.1.1'],
                { read: 'us-east-1', write: 'us-east-1' });
            return done();
        });
    });

    it('should apply second configuration if version (2) is grater than ' +
    'overlayVersion (1)', done => {
        const newConf1 = {
            version: 1,
        };
        const newConf2 = {
            version: 2,
            browserAccess: {
                enabled: true,
            },
        };
        patchConfiguration(newConf1, log, err => {
            checkNoError(err);
            return patchConfiguration(newConf2, log, err => {
                checkNoError(err);
                const actualConf = getConfig();
                const expectedConf = {
                    overlayVersion: 2,
                    browserAccessEnabled: true,
                };
                assertConfig(actualConf, expectedConf);
                return done();
            });
        });
    });

    it('should not apply the second configuration if version equals ' +
    'overlayVersion', done => {
        const newConf1 = {
            version: 1,
        };
        const newConf2 = {
            version: 1,
            browserAccess: {
                enabled: true,
            },
        };
        patchConfiguration(newConf1, log, err => {
            checkNoError(err);
            return patchConfiguration(newConf2, log, err => {
                checkNoError(err);
                const actualConf = getConfig();
                const expectedConf = {
                    overlayVersion: 1,
                    browserAccessEnabled: undefined,
                };
                assertConfig(actualConf, expectedConf);
                return done();
            });
        });
    });
});

describe('remoteOverlayIsNewer', () => {
    it('should return remoteOverlayIsNewer equals false if remote overlay ' +
    'is less than the cached', () => {
        const cachedOverlay = {
            version: 2,
        };
        const remoteOverlay = {
            version: 1,
        };
        const isRemoteOverlayNewer = remoteOverlayIsNewer(cachedOverlay,
            remoteOverlay);
        assert.equal(isRemoteOverlayNewer, false);
    });
    it('should return remoteOverlayIsNewer equals false if remote overlay ' +
    'and the cached one are equal', () => {
        const cachedOverlay = {
            version: 1,
        };
        const remoteOverlay = {
            version: 1,
        };
        const isRemoteOverlayNewer = remoteOverlayIsNewer(cachedOverlay,
            remoteOverlay);
        assert.equal(isRemoteOverlayNewer, false);
    });
    it('should return remoteOverlayIsNewer equals true if remote overlay ' +
    'version is greater than the cached one ', () => {
        const cachedOverlay = {
            version: 0,
        };
        const remoteOverlay = {
            version: 1,
        };
        const isRemoteOverlayNewer = remoteOverlayIsNewer(cachedOverlay,
            remoteOverlay);
        assert.equal(isRemoteOverlayNewer, true);
    });
});
