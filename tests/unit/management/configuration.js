const assert = require('assert');
const crypto = require('crypto');

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
const instanceId = '19683e55-56f7-4a4c-98a7-706c07e4ec30';
const publicInstanceId = crypto.createHash('sha256')
                               .update(instanceId)
                               .digest('hex');

function resetConfig() {
    config.overlayVersion = overlayVersionOriginal;
    config.authData = authDataOriginal;
    config.locationConstraints = locationConstraintsOriginal;
    config.restEndpoints = restEndpointsOriginal;
    config.browserAccessEnabled = browserAccessEnabledOriginal;
}

function assertConfig(actualConf, expectedConf) {
    Object.keys(expectedConf).forEach(key => {
        assert.deepStrictEqual(actualConf[key], expectedConf[key]);
    });
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
            instanceId,
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
                'us-east-1': {
                    name: 'us-east-1',
                    objectId: 'us-east-1',
                    locationType: 'location-file-v1',
                    legacyAwsBehavior: true,
                    details: {},
                },
            },
            browserAccess: {
                enabled: true,
            },
        };
        return patchConfiguration(newConf, log, err => {
            assert.ifError(err);
            const actualConf = getConfig();
            const expectedConf = {
                overlayVersion: 1,
                publicInstanceId,
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
                    'us-east-1': {
                        type: 'file',
                        objectId: 'us-east-1',
                        legacyAwsBehavior: true,
                        isTransient: false,
                        sizeLimitGB: null,
                        details: { supportsVersioning: true },
                        name: 'us-east-1',
                        locationType: 'location-file-v1',
                    },
                },
            };
            assertConfig(actualConf, expectedConf);
            assert.deepStrictEqual(actualConf.restEndpoints['1.1.1.1'],
                                   'us-east-1');
            return done();
        });
    });

    it('should apply second configuration if version (2) is greater than ' +
    'overlayVersion (1)', done => {
        const newConf1 = {
            version: 1,
            instanceId,
        };
        const newConf2 = {
            version: 2,
            instanceId,
            browserAccess: {
                enabled: true,
            },
        };
        patchConfiguration(newConf1, log, err => {
            assert.ifError(err);
            return patchConfiguration(newConf2, log, err => {
                assert.ifError(err);
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
            instanceId,
        };
        const newConf2 = {
            version: 1,
            instanceId,
            browserAccess: {
                enabled: true,
            },
        };
        patchConfiguration(newConf1, log, err => {
            assert.ifError(err);
            return patchConfiguration(newConf2, log, err => {
                assert.ifError(err);
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
