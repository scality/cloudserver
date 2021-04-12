const arsenal = require('arsenal');
const forge = require('node-forge');
const request = require('../utilities/request');

const metadata = require('../metadata/wrapper');

const managementDatabaseName = 'PENSIEVE';
const tokenConfigurationKey = 'auth/zenko/remote-management-token';
const tokenRotationDelay = 3600 * 24 * 7 * 1000; // 7 days
const { reshapeExceptionError } = arsenal.errorUtils;

/**
 * Retrieves Orbit API token from the management database.
 *
 * The token is used to authenticate stat posting and
 *
 * @param  {werelogs~Logger} log Request-scoped logger to be able to trace
 *  initialization process
 * @param  {function} callback Function called with (error, result)
 *
 * @returns {undefined}
 */
function getStoredCredentials(log, callback) {
    metadata.getObjectMD(managementDatabaseName, tokenConfigurationKey, {},
        log, callback);
}

function issueCredentials(managementEndpoint, instanceId, log, callback) {
    log.info('registering with API to get token');

    const keyPair = forge.pki.rsa.generateKeyPair({ bits: 2048, e: 0x10001 });
    const privateKey = forge.pki.privateKeyToPem(keyPair.privateKey);
    const publicKey = forge.pki.publicKeyToPem(keyPair.publicKey);

    const postData = {
        publicKey,
    };

    request.post(`${managementEndpoint}/${instanceId}/register`,
        { body: postData, json: true }, (error, response, body) => {
            if (error) {
                return callback(error);
            }
            if (response.statusCode !== 201) {
                log.error('could not register instance', {
                    statusCode: response.statusCode,
                });
                return callback(arsenal.errors.InternalError);
            }
            /* eslint-disable no-param-reassign */
            body.privateKey = privateKey;
            /* eslint-enable no-param-reassign */
            return callback(null, body);
        });
}

function confirmInstanceCredentials(
    managementEndpoint, instanceId, creds, log, callback) {
    const postData = {
        serial: creds.serial || 0,
        publicKey: creds.publicKey,
    };

    const opts = {
        headers: {
            'x-instance-authentication-token': creds.token,
        },
        body: postData,
    };

    request.post(`${managementEndpoint}/${instanceId}/confirm`,
        opts, (error, response) => {
            if (error) {
                return callback(error);
            }
            if (response.statusCode === 200) {
                return callback(null, instanceId, creds.token);
            }
            return callback(arsenal.errors.InternalError);
        });
}

/**
 * Initializes credentials and PKI in the management database.
 *
 * In case the management database is new and empty, the instance
 * is registered as new against the Orbit API with newly-generated
 * RSA key pair.
 *
 * @param  {string} managementEndpoint API endpoint
 * @param  {string} instanceId UUID of this deployment
 * @param  {werelogs~Logger} log Request-scoped logger to be able to trace
 *  initialization process
 * @param  {function} callback Function called with (error, result)
 *
 * @returns {undefined}
 */
function initManagementCredentials(
    managementEndpoint, instanceId, log, callback) {
    getStoredCredentials(log, (error, value) => {
        if (error) {
            if (error.NoSuchKey) {
                return issueCredentials(managementEndpoint, instanceId, log,
                (error, value) => {
                    if (error) {
                        log.error('could not issue token',
                            { error: reshapeExceptionError(error),
                              method: 'initManagementCredentials' });
                        return callback(error);
                    }
                    log.debug('saving token');
                    return metadata.putObjectMD(managementDatabaseName,
                        tokenConfigurationKey, value, {}, log, error => {
                            if (error) {
                                log.error('could not save token',
                                    { error: reshapeExceptionError(error),
                                      method: 'initManagementCredentials',
                                    });
                                return callback(error);
                            }
                            log.info('saved token locally, ' +
                                'confirming instance');
                            return confirmInstanceCredentials(
                                managementEndpoint, instanceId, value, log,
                                callback);
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

module.exports = {
    getStoredCredentials,
    initManagementCredentials,
};
