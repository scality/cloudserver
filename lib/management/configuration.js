const { URL } = require('url');
const arsenal = require('arsenal');

const { buildAuthDataAccount } = require('../auth/in_memory/builder');
const _config = require('../Config').config;
const constants = require('../../constants');
const metadata = require('../metadata/wrapper');

const { getStoredCredentials } = require('./credentials');

const latestOverlayVersionKey = 'configuration/overlay-version';
const managementDatabaseName = 'PENSIEVE';
const replicatorEndpoint = 'zenko-cloudserver-replicator';
const { decryptSecret } = arsenal.pensieve.credentialUtils;

function overlayHasVersion(overlay) {
    return overlay && overlay.version !== undefined;
}

function remoteOverlayIsNewer(cachedOverlay, remoteOverlay) {
    return (overlayHasVersion(remoteOverlay) &&
            (!overlayHasVersion(cachedOverlay) ||
             remoteOverlay.version > cachedOverlay.version));
}

/**
 * Updates the live {Config} object with the new overlay configuration.
 *
 * No-op if this version was already applied to the live {Config}.
 *
 * @param  {object} newConf Overlay configuration to apply
 * @param  {werelogs~Logger} log Request-scoped logger
 * @param  {function} cb Function to call with (error, newConf)
 *
 * @returns {undefined}
 */
function patchConfiguration(newConf, log, cb) {
    if (newConf.version === undefined) {
        log.debug('no remote configuration created yet');
        return process.nextTick(cb, null, newConf);
    }

    if (_config.overlayVersion !== undefined &&
        newConf.version <= _config.overlayVersion) {
        log.debug('configuration version already applied',
            { configurationVersion: newConf.version });
        return process.nextTick(cb, null, newConf);
    }
    return getStoredCredentials(log, (err, creds) => {
        if (err) {
            return cb(err);
        }
        const accounts = [];
        if (newConf.users) {
            newConf.users.forEach(u => {
                if (u.secretKey && u.secretKey.length > 0) {
                    const secretKey = decryptSecret(creds, u.secretKey);
                    // accountType will be service-replication or service-clueso
                    let serviceName;
                    if (u.accountType && u.accountType.startsWith('service-')) {
                        serviceName = u.accountType.split('-')[1];
                    }
                    const newAccount = buildAuthDataAccount(
                        u.accessKey, secretKey, u.canonicalId, serviceName,
                        u.userName);
                    accounts.push(newAccount.accounts[0]);
                }
            });
        }

        const restEndpoints = Object.assign({}, _config.restEndpoints);
        if (newConf.endpoints) {
            newConf.endpoints.forEach(e => {
                restEndpoints[e.hostname] = e.locationName;
            });
        }

        if (!restEndpoints[replicatorEndpoint]) {
            restEndpoints[replicatorEndpoint] = 'us-east-1';
        }

        const locations = {};
        if (newConf.locations) {
            // Object.values() is apparently too recent
            Object.keys(newConf.locations || {}).forEach(k => {
                const l = newConf.locations[k];
                const location = {};
                let supportsVersioning = false;
                let pathStyle = false;

                switch (l.locationType) {
                case 'location-mem-v1':
                    location.type = 'mem';
                    break;
                case 'location-file-v1':
                    location.type = 'file';
                    break;
                case 'location-azure-v1':
                    location.type = 'azure';
                    if (l.details.secretKey && l.details.secretKey.length > 0) {
                        location.details = {
                            bucketMatch: l.details.bucketMatch,
                            azureStorageEndpoint: l.details.endpoint,
                            azureStorageAccountName: l.details.accessKey,
                            azureStorageAccessKey: decryptSecret(creds,
                                l.details.secretKey),
                            azureContainerName: l.details.bucketName,
                        };
                    }
                    break;
                case 'location-scality-ring-s3-v1':
                    pathStyle = true; // fallthrough
                case 'location-aws-s3-v1':
                case 'location-wasabi-v1':
                    supportsVersioning = true; // fallthrough
                case 'location-do-spaces-v1':
                    location.type = 'aws_s3';
                    if (l.details.secretKey && l.details.secretKey.length > 0) {
                        let https = true;
                        let awsEndpoint = l.details.endpoint ||
                            's3.amazonaws.com';
                        if (awsEndpoint.includes('://')) {
                            const url = new URL(awsEndpoint);
                            awsEndpoint = url.host;
                            https = url.scheme === 'https';
                        }

                        location.details = {
                            credentials: {
                                accessKey: l.details.accessKey,
                                secretKey: decryptSecret(creds,
                                    l.details.secretKey),
                            },
                            bucketName: l.details.bucketName,
                            bucketMatch: l.details.bucketMatch,
                            serverSideEncryption:
                                Boolean(l.details.serverSideEncryption),
                            awsEndpoint,
                            supportsVersioning,
                            pathStyle,
                            https,
                        };
                    }
                    break;
                case 'location-gcp-v1':
                    location.type = 'gcp';
                    if (l.details.secretKey && l.details.secretKey.length > 0) {
                        location.details = {
                            credentials: {
                                accessKey: l.details.accessKey,
                                secretKey: decryptSecret(creds,
                                    l.details.secretKey),
                            },
                            bucketName: l.details.bucketName,
                            mpuBucketName: l.details.mpuBucketName,
                            bucketMatch: l.details.bucketMatch,
                            gcpEndpoint: l.details.endpoint ||
                                'storage.googleapis.com',
                        };
                    }
                    break;
                default:
                    log.info('unknown location type', { locationType:
                        l.locationType });
                    return;
                }
                location.legacyAwsBehavior = Boolean(l.legacyAwsBehavior);
                locations[l.name] = location;
            });
            try {
                _config.setLocationConstraints(locations);
            } catch (error) {
                log.info('could not apply configuration version location ' +
                    'constraints', { error });
                return cb(error);
            }
            try {
                const locationsWithReplicationBackend = Object.keys(locations)
                // NOTE: In Orbit, we don't need to have Scality location in our
                // replication endpoind config, since we do not replicate to
                // any Scality Instance yet.
                .filter(key => constants.replicationBackends
                  [locations[key].type])
                .reduce((obj, key) => {
                    /* eslint no-param-reassign:0 */
                    obj[key] = locations[key];
                    return obj;
                }, {});
                _config.setReplicationEndpoints(
                  locationsWithReplicationBackend);
            } catch (error) {
                log.info('could not apply replication endpoints', { error });
                return cb(error);
            }
        }

        _config.setAuthDataAccounts(accounts);
        _config.setRestEndpoints(restEndpoints);

        if (newConf.browserAccess) {
            if (Boolean(_config.browserAccessEnabled) !==
                Boolean(newConf.browserAccess.enabled)) {
                _config.browserAccessEnabled =
                    Boolean(newConf.browserAccess.enabled);
                _config.emit('browser-access-enabled-change');
            }
        }

        _config.overlayVersion = newConf.version;

        log.info('applied configuration version',
            { configurationVersion: _config.overlayVersion });

        return cb(null, newConf);
    });
}

/**
 * Writes configuration version to the management database
 *
 * @param  {object} cachedOverlay Latest stored configuration version
 *  for freshness comparison purposes
 * @param  {object} remoteOverlay New configuration version
 * @param  {werelogs~Logger} log Request-scoped logger
 * @param  {function} cb Function to call with (error, remoteOverlay)
 *
 * @returns {undefined}
 */
function saveConfigurationVersion(cachedOverlay, remoteOverlay, log, cb) {
    if (remoteOverlayIsNewer(cachedOverlay, remoteOverlay)) {
        const objName = `configuration/overlay/${remoteOverlay.version}`;
        metadata.putObjectMD(managementDatabaseName, objName, remoteOverlay,
            {}, log, error => {
                if (error) {
                    log.error('could not save configuration version',
                        { configurationVersion: remoteOverlay.version });
                }
                metadata.putObjectMD(managementDatabaseName,
                    latestOverlayVersionKey, remoteOverlay.version, {}, log,
                    error => cb(error, remoteOverlay));
            });
    } else {
        log.debug('no remote configuration to cache yet');
        process.nextTick(cb, null, remoteOverlay);
    }
}

/**
 * Loads the latest cached configuration overlay from the management
 * database, without contacting the Orbit API.
 *
 * @param  {werelogs~Logger} log Request-scoped logger
 * @param  {function} callback Function called with (error, cachedOverlay)
 *
 * @returns {undefined}
 */
function loadCachedOverlay(log, callback) {
    return metadata.getObjectMD(managementDatabaseName,
        latestOverlayVersionKey, {}, log, (err, version) => {
            if (err) {
                if (err.NoSuchKey) {
                    return process.nextTick(callback, null, {});
                }
                return callback(err);
            }
            return metadata.getObjectMD(managementDatabaseName,
                `configuration/overlay/${version}`, {}, log, (err, conf) => {
                    if (err) {
                        if (err.NoSuchKey) {
                            return process.nextTick(callback, null, {});
                        }
                        return callback(err);
                    }
                    return callback(null, conf);
                });
        });
}

module.exports = {
    loadCachedOverlay,
    managementDatabaseName,
    patchConfiguration,
    saveConfigurationVersion,
    remoteOverlayIsNewer,
};
