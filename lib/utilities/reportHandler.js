const fs = require('fs');
const os = require('os');

const { errors, ipCheck } = require('arsenal');
const async = require('async');
const request = require('request');

const config = require('../Config').config;
const data = require('../data/wrapper');
const metadata = require('../metadata/wrapper');

const REPORT_MODEL_VERSION = 1;

function cleanup(obj) {
    return {
        overlayVersion: obj.overlayVersion,
    };
}

function isAuthorized(clientIP, req) {
    return ipCheck.ipMatchCidrList(config.healthChecks.allowFrom, clientIP) &&
        req.headers['x-scal-report-token'] === config.reportToken;
}

function getGitVersion(cb) {
    fs.readFile('.git/HEAD', 'ascii', (err, val) => {
        if (err && err.code === 'ENOENT') {
            return cb(null, 'no-dot-git');
        }
        if (err) {
            return cb(null, 'error-reading-dot-git');
        }
        return cb(null, val);
    });
}

function getSystemStats() {
    const cpuInfo = os.cpus();
    const model = cpuInfo[0].model;
    const speed = cpuInfo[0].speed;
    const times = cpuInfo.
        map(c => c.times).
        reduce((prev, cur) =>
            Object.assign({}, {
                user: prev.user + cur.user,
                nice: prev.nice + cur.nice,
                sys: prev.sys + cur.sys,
                idle: prev.idle + cur.idle,
                irq: prev.irq + cur.irq,
            }), {
                user: 0,
                nice: 0,
                sys: 0,
                idle: 0,
                irq: 0,
            });

    return {
        memory: {
            total: os.totalmem(),
            free: os.freemem(),
        },
        cpu: {
            loadavg: os.loadavg(),
            count: cpuInfo.length,
            model,
            speed,
            times,
        },
        arch: os.arch(),
        platform: os.platform(),
        release: os.release(),
        hostname: os.hostname(),
    };
}

function getCRRStats(log, cb) {
    log.debug('getting CRR stats', { method: 'getCRRStats' });
    // TODO: Reuse metrics code from Backbeat by moving it to Arsenal instead of
    // making an HTTP request to the Backbeat metrics route.
    const params = { url: 'http://localhost:8900/_/metrics/crr/all' };
    return request.get(params, (err, res) => {
        if (err) {
            log.error('failed to get CRR stats', {
                method: 'getCRRStats',
                error: err,
            });
            return cb(err);
        }
        let body;
        try {
            body = JSON.parse(res.body);
        } catch (parseErr) {
            log.error('could not parse backbeat response', {
                method: 'getCRRStats',
                error: parseErr,
            });
            return cb(errors.InternalError
                .customizeDescription('could not parse backbeat response'));
        }
        const { completions, backlog, throughput } = body;
        if (!completions || !backlog || !throughput) {
            log.error('could not get metrics from backbeat', {
                method: 'getCRRStats',
            });
            return cb(errors.InternalError
                .customizeDescription('could not get metrics from backbeat'));
        }
        const stats = {
            completions: {
                count: parseFloat(completions.results.count),
                size: parseFloat(completions.results.size),
            },
            backlog: {
                count: parseFloat(backlog.results.count),
                size: parseFloat(backlog.results.size),
            },
            throughput: {
                count: parseFloat(throughput.results.count),
                size: parseFloat(throughput.results.size),
            },
        };
        return cb(null, stats);
    });
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

    async.parallel({
        getUUID: cb => metadata.getUUID(log, cb),
        getMDDiskUsage: cb => metadata.getDiskUsage(log, cb),
        getDataDiskUsage: cb => data.getDiskUsage(log, cb),
        getVersion: cb => getGitVersion(cb),
        getObjectCount: cb => metadata.countItems(log, cb),
        getCRRStats: cb => getCRRStats(log, cb),
    },
    (err, results) => {
        if (err) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.write(JSON.stringify(err));
            log.errorEnd('could not gather report', { error: err });
        } else {
            const response = {
                utcTime: new Date(),
                uuid: results.getUUID,
                reportModelVersion: REPORT_MODEL_VERSION,

                mdDiskUsage: results.getMDDiskUsage,
                dataDiskUsage: results.getDataDiskUsage,
                serverVersion: results.getVersion,
                systemStats: getSystemStats(),
                itemCounts: results.getObjectCount,
                crrStats: results.getCRRStats,

                config: cleanup(config),
            };

            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.write(JSON.stringify(response));
            log.end().debug('report handler finished');
        }
        res.end();
    });
}

module.exports = {
    reportHandler,
};
