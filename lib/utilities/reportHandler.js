const { errors, ipCheck } = require('arsenal');

const config = require('../Config').config;
const metadata = require('../metadata/wrapper');

function cleanup(obj) {
    const ret = JSON.parse(JSON.stringify(obj));
    delete ret.authData;
    delete ret.reportToken;
    return ret;
}

function isAuthorized(clientIP, req) {
    return ipCheck.ipMatchCidrList(config.healthChecks.allowFrom, clientIP) &&
        req.headers['x-scal-report-token'] === config.reportToken;
}

/**
 * Sends back a report
 *
 * @param {string} clientIP - Client IP address for filtering
 * @param {http~IncomingMessage} req - HTTP request object
 * @param {http~ServerResponse} res - HTTP response object
 * @param {werelogs~RequestLogger} log - request logger
 *
 * @return {undefined}
 */
function reportHandler(clientIP, req, res, log) {
    if (!isAuthorized(clientIP, req)) {
        res.writeHead(403);
        res.write(JSON.stringify(errors.AccessDenied));
        res.end();
        return;
    }

    metadata.getUUID(log, (err, uuid) => {
        if (err) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.write(JSON.stringify(err));
            log.errorEnd('could not gather report', { error: err });
        } else {
            const response = {
                uuid,
                config: cleanup(config),
            };

            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.write(JSON.stringify(response));
            log.end('report handler finished');
        }
        res.end();
    });
}

module.exports = {
    reportHandler,
};
