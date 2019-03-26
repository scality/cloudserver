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
        expect(actualConf[key]).toEqual(expectedConf[key]);
    });
}

function checkNoError(err) {
    expect(err).toBe(null);
}

describe('patchConfiguration', () => {
    beforeAll(done => initManagementDatabase(log, err => {
        if (err) {
            return done(err);
        }
        return initManagementCredentialsMock(done);
    }));
    beforeEach(() => {
        resetConfig();
    });
    test('should modify config using the new config', done => {
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
                    objectId: 'legacy',
                    locationType: 'location-mem-v1',
                    details: {},
                },
                'us-east-1': {
                    name: 'us-east-1',
                    objectId: 'us-east-1',
                    locationType: 'location-file-v1',
                    legacyAwsBehavior: true,
                    details: {},
                },
                'azurebackendtest': {
                    name: 'azurebackendtest',
                    objectId: 'azurebackendtest',
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
                    objectId: 'awsbackendtest',
                    locationType: 'location-aws-s3-v1',
                    details: {
                        bucketMatch: 'awsbucketmatch',
                        endpoint: 'aws.end.point',
                        accessKey: 'awsaccesskey',
                        secretKey,
                        bucketName: 'awsbucketname',
                        region: 'us-west-1',
                    },
                },
                'gcpbackendtest': {
                    name: 'gcpbackendtest',
                    objectId: 'gcpbackendtest',
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
                    objectId: 'sproxydbackendtest',
                    locationType: 'location-scality-sproxyd-v1',
                    details: {
                        chordCos: 3,
                        bootstrapList: ['localhost:8001', 'localhost:8002'],
                        proxyPath: '/proxy/path',
                    },
                },
                'transienttest': {
                    name: 'transienttest',
                    objectId: 'transienttest',
                    locationType: 'location-file-v1',
                    isTransient: true,
                    details: {},
                },
                'sizelimitedtest': {
                    name: 'sizelimitedtest',
                    objectId: 'sizelimitedtest',
                    locationType: 'location-file-v1',
                    sizeLimitGB: 1024,
                    details: {},
                },
                'sizezerotest': {
                    name: 'sizezerotest',
                    objectId: 'sizezerotest',
                    locationType: 'location-file-v1',
                    sizeLimitGB: 0,
                    details: {},
                },
                'httpsawsbackendtest': {
                    name: 'httpsawsbackendtest',
                    objectId: 'httpsawsbackendtest',
                    locationType: 'location-scality-ring-s3-v1',
                    details: {
                        bucketMatch: 'rings3bucketmatch',
                        endpoint: 'https://secure.ring.end.point',
                        accessKey: 'rings3accesskey',
                        secretKey,
                        bucketName: 'rings3bucketname',
                        region: 'us-west-1',
                    },
                },
                'cephbackendtest': {
                    name: 'cephbackendtest',
                    objectId: 'cephbackendtest',
                    locationType: 'location-ceph-radosgw-s3-v1',
                    details: {
                        bucketMatch: 'cephbucketmatch',
                        endpoint: 'https://secure.ceph.end.point',
                        accessKey: 'cephs3accesskey',
                        secretKey,
                        bucketName: 'cephbucketname',
                        region: 'us-west-1',
                    },
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
                        objectId: 'legacy',
                        legacyAwsBehavior: false,
                        isTransient: false,
                        sizeLimitGB: null,
                        details: { supportsVersioning: true },
                    },
                    'us-east-1': {
                        type: 'file',
                        objectId: 'us-east-1',
                        legacyAwsBehavior: true,
                        isTransient: false,
                        sizeLimitGB: null,
                        details: { supportsVersioning: true },
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
                        objectId: 'azurebackendtest',
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
                            region: 'us-west-1',
                            serverSideEncryption: false,
                            supportsVersioning: true,
                        },
                        legacyAwsBehavior: false,
                        isTransient: false,
                        sizeLimitGB: null,
                        type: 'aws_s3',
                        objectId: 'awsbackendtest',
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
                            https: true,
                        },
                        legacyAwsBehavior: false,
                        isTransient: false,
                        sizeLimitGB: null,
                        type: 'gcp',
                        objectId: 'gcpbackendtest',
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
                        objectId: 'sproxydbackendtest',
                    },
                    'transienttest': {
                        type: 'file',
                        objectId: 'transienttest',
                        legacyAwsBehavior: false,
                        isTransient: true,
                        sizeLimitGB: null,
                        details: { supportsVersioning: true },
                    },
                    'sizelimitedtest': {
                        type: 'file',
                        objectId: 'sizelimitedtest',
                        legacyAwsBehavior: false,
                        isTransient: false,
                        sizeLimitGB: 1024,
                        details: { supportsVersioning: true },
                    },
                    'sizezerotest': {
                        type: 'file',
                        objectId: 'sizezerotest',
                        legacyAwsBehavior: false,
                        isTransient: false,
                        sizeLimitGB: null,
                        details: { supportsVersioning: true },
                    },
                    'httpsawsbackendtest': {
                        details: {
                            awsEndpoint: 'secure.ring.end.point',
                            bucketMatch: 'rings3bucketmatch',
                            bucketName: 'rings3bucketname',
                            credentials: {
                                accessKey: 'rings3accesskey',
                                secretKey: decryptedSecretKey,
                            },
                            https: true,
                            pathStyle: true,
                            region: 'us-west-1',
                            serverSideEncryption: false,
                            supportsVersioning: true,
                        },
                        legacyAwsBehavior: false,
                        isTransient: false,
                        sizeLimitGB: null,
                        type: 'aws_s3',
                        objectId: 'httpsawsbackendtest',
                    },
                    'cephbackendtest': {
                        details: {
                            awsEndpoint: 'secure.ceph.end.point',
                            bucketMatch: 'cephbucketmatch',
                            bucketName: 'cephbucketname',
                            credentials: {
                                accessKey: 'cephs3accesskey',
                                secretKey: decryptedSecretKey,
                            },
                            https: true,
                            pathStyle: true,
                            region: 'us-west-1',
                            serverSideEncryption: false,
                            supportsVersioning: true,
                        },
                        legacyAwsBehavior: false,
                        isTransient: false,
                        sizeLimitGB: null,
                        type: 'aws_s3',
                        objectId: 'cephbackendtest',
                    },
                },
            };
            assertConfig(actualConf, expectedConf);
            assert.deepStrictEqual(actualConf.restEndpoints['1.1.1.1'],
                                   'us-east-1');
            return done();
        });
    });

    test('should apply second configuration if version (2) is grater than ' +
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

    test('should not apply the second configuration if version equals ' +
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
    test('should return remoteOverlayIsNewer equals false if remote overlay ' +
    'is less than the cached', () => {
        const cachedOverlay = {
            version: 2,
        };
        const remoteOverlay = {
            version: 1,
        };
        const isRemoteOverlayNewer = remoteOverlayIsNewer(cachedOverlay,
            remoteOverlay);
        expect(isRemoteOverlayNewer).toEqual(false);
    });
    test('should return remoteOverlayIsNewer equals false if remote overlay ' +
    'and the cached one are equal', () => {
        const cachedOverlay = {
            version: 1,
        };
        const remoteOverlay = {
            version: 1,
        };
        const isRemoteOverlayNewer = remoteOverlayIsNewer(cachedOverlay,
            remoteOverlay);
        expect(isRemoteOverlayNewer).toEqual(false);
    });
    test('should return remoteOverlayIsNewer equals true if remote overlay ' +
    'version is greater than the cached one ', () => {
        const cachedOverlay = {
            version: 0,
        };
        const remoteOverlay = {
            version: 1,
        };
        const isRemoteOverlayNewer = remoteOverlayIsNewer(cachedOverlay,
            remoteOverlay);
        expect(isRemoteOverlayNewer).toEqual(true);
    });
});
