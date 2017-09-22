const { errors, ipCheck } = require('arsenal');
const _config = require('../Config').config;
const data = require('../data/wrapper');
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

function clientCheck(log, cb) {
    const clients = [
        data,
        metadata,
        vault,
    ];
    const clientTasks = [];
    clients.forEach(client => {
        if (typeof client.checkHealth === 'function') {
            clientTasks.push(done => {
                client.checkHealth(log, done);
            });
        }
    });
    async.parallel(clientTasks, (err, results) => {
        process.stdout.write('=======================\n');
        process.stdout.write('results of client check healthcheck\n');
        process.stdout.write(`${JSON.stringify(results, null, 4)}\n`);
        let fail = false;
        // obj will be an object of the healthcheck results of
        // every backends. No errors were returned directly to
        // async.parallel in order to complete the check, so a
        // manual check makes S3 return 500 error if any backend failed
        const obj = results.reduce((obj, item) => Object.assign(obj, item), {});
        fail = Object.keys(obj).some(k => obj[k].error);
        process.stdout.write('obj\n');
        process.stdout.write(`${JSON.stringify(obj, null, 4)}\n`);
        process.stdout.write('======================\n');
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
        return statsClient.getStats(log, cb);
    }
    return clientCheck(log, cb);
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
