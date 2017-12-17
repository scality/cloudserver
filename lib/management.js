const arsenal = require('arsenal');
const async = require('async');
const forge = require('node-forge');
const request = require('request');

const { buildAuthDataAccount } = require('./auth/in_memory/builder');
const metadata = require('./metadata/wrapper');
const _config = require('./Config').config;
const logger = require('./utilities/logger');

const managementEndpointRoot =
    process.env.MANAGEMENT_ENDPOINT ||
    'https://api-dev.private.zenko.io';

const managementEndpoint = `${managementEndpointRoot}/api/v1/instance`;

const managementDatabaseName = 'PENSIEVE';
const initRemoteManagementRetryDelay = 10000;
const pushReportDelay = 30000;
const pullConfigurationOverlayDelay = 10000;

const tokenRotationDelay = 3600 * 24 * 7 * 1000; // 7 days
const tokenConfigurationKey = 'auth/zenko/remote-management-token';
const latestOverlayVersionKey = 'configuration/overlay-version';

function decryptSecret(instanceCredentials, secret) {
    // XXX don't forget to use u.encryptionKeyVersion if present
    const privateKey = forge.pki.privateKeyFromPem(
        instanceCredentials.privateKey);
    const encryptedSecretKey = forge.util.decode64(secret);
    return privateKey.decrypt(encryptedSecretKey, 'RSA-OAEP', {
        md: forge.md.sha256.create(),
    });
}

function getStoredCredentials(instanceId, log, callback) {
    metadata.getObjectMD(managementDatabaseName, tokenConfigurationKey, {},
        log, callback);
}

function patchConfiguration(instanceId, newConf, log, cb) {
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

    return getStoredCredentials(instanceId, log, (err, creds) => {
        if (err) {
            return cb(err);
        }

        const accounts = [];
        if (newConf.users) {
            newConf.users.forEach(u => {
                if (u.secretKey && u.secretKey.length > 0) {
                    const secretKey = decryptSecret(creds, u.secretKey);
                    // accountType will be service-replication or service-clueso
                    const serviceName = u.accountType.startsWith('service-') ?
                        u.accountType.split('-')[1] : undefined;
                    const newAccount = buildAuthDataAccount(
                        u.accessKey, secretKey, serviceName);
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

        const locations = {};
        if (newConf.locations) {
            // Object.values() is apparently too recent
            Object.keys(newConf.locations || {}).forEach(k => {
                const l = newConf.locations[k];
                const location = {};
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
                            azureStorageAccessKey: decryptSecret(creds, l.details.secretKey),
                            azureContainerName: l.details.bucketName,
                        };
                    }
                    break;
                case 'location-aws-s3-v1':
                    location.type = 'aws_s3';
                    if (l.details.secretKey && l.details.secretKey.length > 0) {
                        location.details = {
                            credentials: {
                                accessKey: l.details.accessKey,
                                secretKey: decryptSecret(creds, l.details.secretKey),
                            },
                            bucketName: l.details.bucketName,
                            bucketMatch: l.details.bucketMatch,
                            serverSideEncryption: Boolean(l.details.serverSideEncryption),
                            awsEndpoint: l.details.endpoint || 's3.amazonaws.com',
                        };
                    }
                    break;
                default:
                    return;
                }
                location.legacyAwsBehavior = Boolean(l.legacyAwsBehavior);
                locations[l.name] = location;
            });
            try {
                _config.setLocationConstraints(locations);
            } catch (error) {
                log.info('could not apply configuration version location constraints',
                    { error });
                return cb(error);
            }
        }

        _config.setAuthDataAccounts(accounts);
        _config.setRestEndpoints(restEndpoints);
        _config.overlayVersion = newConf.version;

        log.info('applied configuration version',
            { configurationVersion: _config.overlayVersion });

        return cb(null, newConf);
    });
}

function loadRemoteOverlay(instanceId, remoteToken, cachedOverlay, log, cb) {
    log.debug('loading remote overlay');
    const opts = {
        headers: {
            'x-instance-authentication-token': remoteToken,
            'x-scal-request-id': log.getSerializedUids(),
        },
    };
    request(`${managementEndpoint}/${instanceId}/config/overlay`, opts,
        (error, response, body) => {
            if (error) {
                return cb(error);
            }
            if (response.statusCode === 200) {
                return cb(null, cachedOverlay, body);
            }
            if (response.statusCode === 404) {
                return cb(null, cachedOverlay, {});
            }
            return cb(arsenal.errors.AccessForbidden, cachedOverlay, {});
        }).json();
}

function loadCachedOverlay(log, callback) {
    log.debug('returning stub cached config overlay');
    return process.nextTick(callback, null, {});
}

function overlayHasVersion(overlay) {
    return overlay && overlay.version !== undefined;
}

function remoteOverlayIsNewer(cachedOverlay, remoteOverlay) {
    return (overlayHasVersion(remoteOverlay) &&
            (!overlayHasVersion(cachedOverlay) ||
             remoteOverlay.version > cachedOverlay.version));
}

function saveConfigurationVersion(cachedOverlay, remoteOverlay, log, cb) {
    if (remoteOverlayIsNewer(cachedOverlay, remoteOverlay)) {
        const objName = `configuration/overlay/${remoteOverlay.version}`;
        metadata.putObjectMD(managementDatabaseName, objName, remoteOverlay,
            {}, log, error => {
                if (error) {
                    log.error('could not save configuration version',
                        { configurationVersion: remoteOverlay.version });
                }
                metadata.putObjectMD(managementDatabaseName, latestOverlayVersionKey,
                    remoteOverlay.version, {}, log, error => {
                        return cb(error, remoteOverlay);
                    });
            });
    } else {
        log.debug('no remote configuration to cache yet');
        process.nextTick(cb, null, remoteOverlay);
    }
}

// TODO save only after successful patch
function applyConfigurationOverlay(instanceId, remoteToken, log, cb) {
    async.waterfall([
        wcb => loadCachedOverlay(log, wcb),
        (cachedOverlay, wcb) => patchConfiguration(instanceId, cachedOverlay, log, wcb),
        (cachedOverlay, wcb) =>
            loadRemoteOverlay(instanceId, remoteToken, cachedOverlay, log, wcb),
        (cachedOverlay, remoteOverlay, wcb) =>
            saveConfigurationVersion(cachedOverlay, remoteOverlay, log, wcb),
        (remoteOverlay, wcb) => patchConfiguration(instanceId, remoteOverlay, log, wcb),
    ], error => {
        if (error) {
            log.error('could not apply managed configuration', { error });
        }
        if (cb) {
            cb(null, instanceId, remoteToken);
        }
        setTimeout(applyConfigurationOverlay, pullConfigurationOverlayDelay,
            instanceId, remoteToken, logger.newRequestLogger());
    });
}

function getStats() {
    const fromURL = `http://localhost:${_config.port}/_/report`;
    const fromOptions = {
        headers: {
            'x-scal-report-token': process.env.REPORT_TOKEN,
        },
    };
    return request(fromURL, fromOptions).json();
}

function postStats(instanceId, remoteToken, next) {
    const toURL = `${managementEndpoint}/${instanceId}/stats`;
    const toOptions = {
        headers: {
            'x-instance-authentication-token': remoteToken,
        },
    };
    const toCallback = (err, response, body) => {
        if (err) {
            console.log('STAT PUSH ERR', err);
        }
        if (response && response.statusCode !== 201) {
            console.log('STAT PUSH ERR', response.statusCode, body);
        }
        if (next) {
            next(null, instanceId, remoteToken);
        }
    };
    return request.post(toURL, toOptions, toCallback).json();
}

function pushStats(instanceId, remoteToken, next) {
    getStats().pipe(postStats(instanceId, remoteToken, next));
    setTimeout(pushStats, pushReportDelay, instanceId, remoteToken);
}

function issueCredentials(instanceId, log, callback) {
    log.info('registering with API to get token');

    const keyPair = forge.pki.rsa.generateKeyPair({ bits: 2048, e: 0x10001 });
    const privateKey = forge.pki.privateKeyToPem(keyPair.privateKey);
    const publicKey = forge.pki.publicKeyToPem(keyPair.publicKey);

    const postData = {
        publicKey,
    };

    request.post(`${managementEndpoint}/${instanceId}/register`,
        (error, response, body) => {
            if (error) {
                return callback(error);
            }
            if (response.statusCode !== 201) {
                log.error('could not register instance', {
                    statusCode: response.statusCode,
                });
                return callback(arsenal.errors.InternalError);
            }
            body.privateKey = privateKey;
            return callback(null, body);
        }).json(postData);
}

function confirmInstanceCredentials(instanceId, creds, log, callback) {
    const opts = {
        headers: {
            'x-instance-authentication-token': creds.token,
        },
    };
    const postData = {
        serial: creds.serial || 0,
        publicKey: creds.publicKey,
    };
    request.post(`${managementEndpoint}/${instanceId}/confirm`,
        opts, (error, response, body) => {
            if (error) {
                return callback(error);
            }
            if (response.statusCode === 200) {
                return callback(null, instanceId, creds.token);
            }
            return callback(arsenal.errors.InternalError);
        }).json(postData);
}

function initManagementCredentials(instanceId, log, callback) {
    getStoredCredentials(instanceId, log, (error, value) => {
        if (error) {
            if (error.NoSuchKey) {
                return issueCredentials(instanceId, log, (error, value) => {
                    if (error) {
                        log.error('could not issue token', { error });
                        return callback(error);
                    }
                    log.debug('saving token');
                    metadata.putObjectMD(managementDatabaseName,
                        tokenConfigurationKey, value, {}, log, error => {
                            if (error) {
                                log.error('could not save token',
                                    { error });
                                return callback(error);
                            }
                            log.info('saved token locally, ' +
                                'confirming instance');
                            return confirmInstanceCredentials(
                                instanceId, value, log, callback);
                        });
                });
            }
            log.debug('could not get token', { error });
            return callback(error);
        }

        log.info('returning existing token');
        if (Date.now() - value.issueDate > tokenRotationDelay) {
            log.warn('management API token is too old, should re-issue');
        }

        return callback(null, instanceId, value.token);
    });
}

function initManagementDatabase(log, callback) {
    // XXX choose proper owner names
    const md = new arsenal.models.BucketInfo(managementDatabaseName, 'owner',
        'owner display name', new Date().toJSON());

    metadata.createBucket(managementDatabaseName, md, log, error => {
        if (error) {
            if (error.BucketAlreadyExists) {
                log.info('created management database');
                return callback();
            }
            log.error('could not initialize management database',
                { error });
            return callback(error);
        }
        log.info('initialized management database');
        return callback();
    });
}

function initManagement(log) {
    if (process.env.REMOTE_MANAGEMENT_DISABLE &&
        process.env.REMOTE_MANAGEMENT_DISABLE !== '0') {
        log.info('remote management disabled');
        return;
    }
    async.waterfall([
        cb => initManagementDatabase(log, cb),
        cb => metadata.getUUID(log, cb),
        (instanceId, cb) => initManagementCredentials(instanceId, log, cb),
        (instanceId, token, cb) =>
            applyConfigurationOverlay(instanceId, token, log, cb),
        (instanceId, token, cb) => pushStats(instanceId, token, cb),
        (instanceId, token, cb) => {
            metadata.notifyBucketChange(() => {
                pushStats(instanceId, token);
            });
            process.nextTick(cb);
        },
    ], error => {
        if (error) {
            log.error('could not initialize remote management, retrying later',
                { error });
            setTimeout(initManagement,
                initRemoteManagementRetryDelay,
                logger.newRequestLogger());
        } else {
            metadata.getUUID(log, (err, instanceId) => {
                log.info(`this deployment's Instance ID is ${instanceId}`);
                log.end('management init done');
            });
        }
    });
}

module.exports = {
    initManagement,
};
