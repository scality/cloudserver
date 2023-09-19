const { errors, ipCheck } = require('arsenal');
const _config = require('../Config').config;
const { data } = require('../data/wrapper');
const vault = require('../auth/vault');
const metadata = require('../metadata/wrapper');
const async = require('async');

// current function utility is minimal, but will be expanded
function isHealthy() {
    return true;
}

function writeResponse(res, error, log, results, cb) {
    let statusCode = 200;
    if (error) {
        // error.code can be a string (such as ECONNREFUSED)
        if (Number.isInteger(error.code)) {
            statusCode = error.code;
        } else {
            statusCode = 500;
        }
    }
    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.write(JSON.stringify(results));
    res.end(() => {
        cb(error, results);
    });
}

/**
 * Calls each client's healthcheck and translates responses to
 * InternalError message if appropriate
 * @param {boolean} flightCheckOnStartUp - whether client check is a flight
 * check on startup. If performing a flight check, behavior is:
 *  - return an error to halt start-up if there is any error in the client check
 *  - check all external backend locations
 *  - create a container for Azure location if one is missing
 * @param {object} log - werelogs logger instance
 * @param {function} cb - callback
 * @return {undefined}
 */
function clientCheck(flightCheckOnStartUp, log, cb) {
    // FIXME S3C-4833 KMS healthchecks have been disabled:
    // - they should be reworked to avoid blocking all requests,
    //   including unencrypted requests
    // - they should not prevent Cloudserver from starting up
    const clients = [
        data,
        metadata,
        vault,
        // kms,
    ];
    const clientTasks = [];
        log.debug(`HERE Config ${JSON.stringify(_config.locationConstraints.azurenonexistcontainer.details)}`);
    clients.forEach(client => {
        if (typeof client.checkHealth === 'function') {
            clientTasks.push(done => {
                client.checkHealth(log, done, flightCheckOnStartUp);
            });
        }
    });
    async.parallel(clientTasks, (err, results) => {
        let fail = false;
        // obj will be an object of the healthcheck results of
        // every backends. No errors were returned directly to
        // async.parallel in order to complete the check, so a
        // manual check makes S3 return 500 error if any backend failed
        // other than aws_s3 or azure
        const obj = results.reduce((obj, item) => Object.assign(obj, item), {});
        log.debug(`HERE THE obj ${obj}`);
        fail = Object.keys(obj).some(k =>
            // if there is an error from an external backend,
            // only return a 500 if it is on startup
            // (flightCheckOnStartUp set to true)
            obj[k].error && (flightCheckOnStartUp || !obj[k].external)
        );
        log.debug(`HERE THE FAIL ${fail}`);
        if (fail) {
            return cb(errors.InternalError, obj);
        }
        return cb(null, obj);
    });
}

function routeHandler(deep, req, res, log, statsClient, cb) {
    if (!isHealthy()) {
        return cb(errors.InternalError, []);
    }
    if (req.method !== 'GET' && req.method !== 'POST') {
        return cb(errors.BadRequest, []);
    }
    if (!deep) {
        return statsClient.getStats(log, 's3', cb);
    }
    return clientCheck(false, log, cb);
}

function checkIP(clientIP) {
    return ipCheck.ipMatchCidrList(
        _config.healthChecks.allowFrom, clientIP);
}

/**
 * Checks if client IP address is allowed to make http request to
 * S3 server. Defines function 'healthcheckEndHandler', which is
 * called if IP not allowed or passed as callback.
 * @param {object} clientIP - IP address of client
 * @param {object} req - http request object
 * @param {object} res - http response object
 * @param {object} log - werelogs logger instance
 * @param {object} statsClient - StatsClient Instance
 * @return {undefined}
 */
function healthcheckHandler(clientIP, req, res, log, statsClient) {
    function healthcheckEndHandler(err, results) {
        writeResponse(res, err, log, results, error => {
            if (error) {
                return log.end().warn('healthcheck error', { err: error });
            }
            return log.end();
        });
    }

    if (!checkIP(clientIP)) {
        return healthcheckEndHandler(errors.AccessDenied, []);
    }
    const deep = (req.url === '/_/healthcheck/deep');
    return routeHandler(deep, req, res, log, statsClient,
                        healthcheckEndHandler);
}

module.exports = {
    isHealthy,
    clientCheck,
    healthcheckHandler,
};
