import { errors, ipCheck } from 'arsenal';
import _config from '../Config';
import data from '../data/wrapper';
import vault from '../auth/vault';
import metadata from '../metadata/wrapper';
import async from 'async';

// current function utility is minimal, but will be expanded
export function isHealthy() {
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

export function clientCheck(log, cb) {
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
    async.parallel(clientTasks, cb);
}

function routeHandler(deep, req, res, log, statsClient, cb) {
    if (isHealthy()) {
        if (req.method === 'GET' || req.method === 'POST') {
            if (deep) {
                return clientCheck(log, cb);
            }
            return statsClient.getStats(log, cb);
        }
        return cb(errors.BadRequest, []);
    }
    return cb(errors.InternalError, []);
}

function checkIP(clientIP) {
    return ipCheck.ipMatchCidrList(
        _config.healthChecks.allowFrom, clientIP);
}

/*
* Checks if client IP address is allowed to make http request to
* S3 server. Defines function 'healthcheckEndHandler', which is
* called if IP not allowed or passed as callback.
* @param {object} clientIP - IP address of client
* @param {boolean} deep - true if healthcheck will check backends
* @param {object} req - http request object
* @param {object} res - http response object
* @param {object} log - werelogs logger instance
* @param {object} statsClient - StatsClient Instance
*/
export function healthcheckHandler(clientIP, deep, req, res, log,
    statsClient) {
    function healthcheckEndHandler(err, results) {
        writeResponse(res, err, log, results, (error, body) => {
            if (error) {
                return log.end().warn('healthcheck error', { err: error });
            }
            return log.end().info('healthcheck ended', { result: body });
        });
    }

    if (!checkIP(clientIP)) {
        return healthcheckEndHandler(errors.AccessDenied, []);
    }
    return routeHandler(deep, req, res, log, statsClient,
        healthcheckEndHandler);
}
