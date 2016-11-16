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
    // If we're here then s3 is considered ok
    const initial = { S3: { code: 200, message: 'OK' } };
    const merged = results.reduce(
        (prev, value) => Object.assign(prev, value), initial);

    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.write(JSON.stringify(merged));
    res.end(() => {
        cb(error, merged);
    });
}

function clientCheck(req, res, log, cb) {
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

function routeHandler(deep, req, res, log, cb) {
    if (isHealthy()) {
        if (req.method === 'GET' || req.method === 'POST') {
            if (deep) {
                return clientCheck(req, res, log, cb);
            }
            return cb(null, []);
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
*/
export default function healthcheckHandler(clientIP, deep, req, res, log) {
    function healthcheckEndHandler(err, results) {
        writeResponse(res, err, log, results, (error, body) => {
            if (error) {
                return log.end().warn('healthcheck error', {
                    error,
                });
            }
            return log.end().info('healthcheck ended', {
                result: body,
            });
        });
    }

    if (!checkIP(clientIP)) {
        return healthcheckEndHandler(errors.AccessDenied, []);
    }
    return routeHandler(deep, req, res, log, healthcheckEndHandler);
}
