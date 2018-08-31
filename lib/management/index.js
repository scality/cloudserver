const arsenal = require('arsenal');
const async = require('async');

const metadata = require('../metadata/wrapper');
const logger = require('../utilities/logger');

const {
    loadCachedOverlay,
    managementDatabaseName,
    patchConfiguration,
} = require('./configuration');
const { initManagementCredentials } = require('./credentials');
const { startWSManagementClient } = require('./push');
const { startPollingManagementClient } = require('./poll');
const { isManagementAgentUsed } = require('./agentClient');

const initRemoteManagementRetryDelay = 10000;

const managementEndpointRoot =
    process.env.MANAGEMENT_ENDPOINT ||
    'https://api.zenko.io';
const managementEndpoint = `${managementEndpointRoot}/api/v1/instance`;

const pushEndpointRoot =
    process.env.PUSH_ENDPOINT ||
    'https://push.api.zenko.io';
const pushEndpoint = `${pushEndpointRoot}/api/v1/instance`;

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

function startManagementListeners(instanceId, token) {
    const mode = process.env.MANAGEMENT_MODE || 'push';
    if (mode === 'push') {
        startWSManagementClient(pushEndpoint, instanceId, token);
    } else {
        startPollingManagementClient(managementEndpoint, instanceId, token);
    }
}

/**
 * Initializes Orbit-based management by:
 * - creating the management database in metadata
 * - generating a key pair for credentials encryption
 * - generating an instance-unique ID
 * - getting an authentication token for the API
 * - loading and applying the latest cached overlay configuration
 * - starting a configuration update and metrics push background task
 *
 * @param  {werelogs~Logger} log Request-scoped logger to be able to trace
 *  initialization process
 * @param  {function} callback Function to call once the overlay is loaded
 *  (overlay)
 *
 * @returns {undefined}
 */
function initManagement(log, callback) {
    if ((process.env.REMOTE_MANAGEMENT_DISABLE &&
        process.env.REMOTE_MANAGEMENT_DISABLE !== '0')
        || process.env.S3BACKEND === 'mem') {
        log.info('remote management disabled');
        return;
    }
    async.waterfall([
        // eslint-disable-next-line arrow-body-style
        cb => { return isManagementAgentUsed() ? metadata.setup(cb) : cb(); },
        cb => initManagementDatabase(log, cb),
        cb => metadata.getUUID(log, cb),
        (instanceId, cb) => initManagementCredentials(
            managementEndpoint, instanceId, log, cb),
        (instanceId, token, cb) =>
            loadCachedOverlay(log, (err, overlay) => cb(err, instanceId,
                                                        token, overlay)),
        (instanceId, token, overlay, cb) =>
            patchConfiguration(overlay, log,
                               err => cb(err, instanceId, token, overlay)),
    ], (error, instanceId, token, overlay) => {
        if (error) {
            log.error('could not initialize remote management, retrying later',
                { error });
            setTimeout(initManagement,
                initRemoteManagementRetryDelay,
                logger.newRequestLogger());
        } else {
            log.info(`this deployment's Instance ID is ${instanceId}`);
            log.end('management init done');
            startManagementListeners(instanceId, token);
            if (callback) {
                callback(overlay);
            }
        }
    });
}

module.exports = {
    initManagement,
    initManagementDatabase,
};
