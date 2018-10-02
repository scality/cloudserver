const arsenal = require('arsenal');
const async = require('async');
const request = require('request');

const _config = require('../Config').config;
const logger = require('../utilities/logger');
const metadata = require('../metadata/wrapper');
const {
    loadCachedOverlay,
} = require('./configuration');
const { reshapeExceptionError } = arsenal.errorUtils;

const pushReportDelay = 30000;
const pullConfigurationOverlayDelay = 60000;

let overlayMessageListener = null;
let cachedOverlay = null;

function loadRemoteOverlay(
    managementEndpoint, instanceId, remoteToken, log, cb) {
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
                return cb(null, body);
            }
            if (response.statusCode === 404) {
                return cb(null, {});
            }
            return cb(arsenal.errors.AccessForbidden, {});
        }).json();
}

// TODO save only after successful patch
function applyConfigurationOverlay(
    managementEndpoint, instanceId, remoteToken, log) {
    async.waterfall([
        wcb => {
            if (cachedOverlay) {
                wcb(null, cachedOverlay);
            } else {
                loadCachedOverlay(log, wcb);
            }
        },
        overlay => { cachedOverlay = overlay; },
        wcb => loadRemoteOverlay(managementEndpoint, instanceId, remoteToken,
               log, wcb),
        remoteOverlay => {
            cachedOverlay = remoteOverlay;
            if (overlayMessageListener) {
                overlayMessageListener(JSON.stringify(cachedOverlay));
            }
        },
    ], error => {
        if (error) {
            log.error('could not apply managed configuration',
                { error: reshapeExceptionError(error),
                  method: 'applyConfigurationOverlay' });
        }
        setTimeout(applyConfigurationOverlay, pullConfigurationOverlayDelay,
            managementEndpoint, instanceId, remoteToken,
            logger.newRequestLogger());
    });
}

function postStats(managementEndpoint, instanceId, remoteToken, next) {
    const toURL = `${managementEndpoint}/${instanceId}/stats`;
    const toOptions = {
        headers: {
            'x-instance-authentication-token': remoteToken,
        },
    };
    const toCallback = (err, response, body) => {
        if (err) {
            logger.info('could not post stats', { error: err });
        }
        if (response && response.statusCode !== 201) {
            logger.info('could not post stats', {
                body,
                statusCode: response.statusCode,
            });
        }
        if (next) {
            next(null, instanceId, remoteToken);
        }
    };
    return request.post(toURL, toOptions, toCallback).json();
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

function pushStats(managementEndpoint, instanceId, remoteToken, next) {
    if (process.env.PUSH_STATS === 'false') {
        return;
    }
    getStats()
      .on('error', err => {
          /* If the management process launches too quick, the cloud server may
           * not be listening yet. */
          logger.info('failed to get stats', { err });
      })
      .pipe(
        postStats(managementEndpoint, instanceId, remoteToken, next));
    setTimeout(pushStats, pushReportDelay,
        managementEndpoint, instanceId, remoteToken);
}

/**
 * Starts background task that updates configuration and pushes stats.
 *
 * Periodically polls for configuration updates, and pushes stats at
 * a fixed interval.
 *
 * @param {string} managementEndpoint API endpoint
 * @param {string} instanceId UUID of this deployment
 * @param {string} remoteToken API authentication token
 * @param {function} newOverlayCallback Function to call once a new overlay is
 *                                      loaded (overlay)
 *
 * @returns {undefined}
 */
function startPollingManagementClient(
    managementEndpoint, instanceId, remoteToken, newOverlayCallback) {
    overlayMessageListener = newOverlayCallback;

    metadata.notifyBucketChange(() => {
        pushStats(managementEndpoint, instanceId, remoteToken);
    });

    pushStats(managementEndpoint, instanceId, remoteToken);
    applyConfigurationOverlay(managementEndpoint, instanceId, remoteToken,
        logger.newRequestLogger());
}

module.exports = {
    startPollingManagementClient,
};
