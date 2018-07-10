const fs = require('fs');
const os = require('os');

const { errors, ipCheck, backbeat } = require('arsenal');
const async = require('async');

const config = require('../Config').config;
const data = require('../data/wrapper');
const metadata = require('../metadata/wrapper');
const monitoring = require('../utilities/monitoringHandler');

const REPORT_MODEL_VERSION = 1;

function hasWSOptionalDependencies() {
    try {
        const b = require('bufferutil');
        const u = require('utf-8-validate');
        return !!(b && u);
    } catch (_) {
        return false;
    }
}

function getCapabilities() {
    return {
        locationTypeDigitalOcean: true,
        locationTypeS3Custom: true,
        locationTypeSproxyd: true,
        preferredReadLocation: true,
        managedLifecycle: true,
        secureChannelOptimizedPath: hasWSOptionalDependencies(),
    };
}

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

function _crrRequest(backbeatMetrics, rDetails, site, log, cb) {
    const details = JSON.parse(JSON.stringify(rDetails));
    // Add `site` as we're not using Backbeat's request parser for the API's URI
    details.site = site;
    return backbeatMetrics.getAllMetrics(details, (err, res) => {
        if (err) {
            log.error('error occured when retrieving metrics', {
                method: '_crrRequest',
                error: err,
            });
            return cb(null, {});
        }
        const { completions, failures, backlog, throughput } = res;
        if (!completions || !failures || !backlog || !throughput) {
            log.error('could not get metrics from backbeat', {
                method: 'getCRRStats',
            });
            return cb(null, {});
        }
        const stats = {
            completions: {
                count: parseInt(completions.results.count, 10),
                size: parseInt(completions.results.size, 10),
            },
            failures: {
                count: parseInt(failures.results.count, 10),
                size: parseInt(failures.results.size, 10),
            },
            backlog: {
                count: parseInt(backlog.results.count, 10),
                size: parseInt(backlog.results.size, 10),
            },
            throughput: {
                count: parseInt(throughput.results.count, 10),
                size: parseInt(throughput.results.size, 10),
            },
        };
        return cb(null, stats);
    });
}

function getCRRStats(log, cb, _config) {
    log.debug('getting CRR stats', { method: 'getCRRStats' });
    const { replicationEndpoints, localCache: redis } = _config || config;
    if (!redis) {
        log.debug('redis connection not configured', { method: 'getCRRStats' });
        return process.nextTick(() => cb(null, {}));
    }
    const sites = replicationEndpoints.map(endpoint => endpoint.site);
    const backbeatMetrics = new backbeat.Metrics({
        redisConfig: redis,
        validSites: sites,
        internalStart: Date.now() - 900000, // 15 minutes ago.
    }, log);
    const redisKeys = {
        ops: 'bb:crr:ops',
        bytes: 'bb:crr:bytes',
        opsDone: 'bb:crr:opsdone',
        bytesDone: 'bb:crr:bytesdone',
        opsFail: 'bb:crr:opsfail',
        bytesFail: 'bb:crr:bytesfail',
        failedCRR: 'bb:crr:failed',
    };
    const routes = backbeat.routes(redisKeys, sites);
    const details = routes.find(route =>
        route.category === 'metrics' && route.type === 'all');
    return async.parallel({
        total: done => _crrRequest(backbeatMetrics, details, 'all', log, done),
        byLocation: done => async.map(sites,
            (site, next) => _crrRequest(backbeatMetrics, details, site, log,
                (err, res) => {
                    if (err) {
                        log.debug('Error in retrieving site metrics', {
                            method: 'getCRRStats',
                            error: err,
                            site,
                        });
                        return next(null, { site, stats: {} });
                    }
                    return next(null, { site, stats: res });
                }
            ),
            (err, locStats) => {
                if (err) {
                    log.error('failed to get CRR stats for site', {
                        method: 'getCRRStats',
                        error: err,
                    });
                    return done(null, {});
                }
                const retObj = {};
                locStats.forEach(locStat => {
                    retObj[locStat.site] = locStat.stats;
                });
                return done(null, retObj);
            }
        ),
    }, (err, res) => {
        if (err) {
            log.error('failed to get CRR stats', {
                method: 'getCRRStats',
                error: err,
            });
            return cb(null, {});
        }
        const total = (res && res.total) || {};
        const byLocation = (res && res.byLocation) || {};
        const retObj = {
            completions: total.completions,
            failures: total.failures,
            backlog: total.backlog,
            throughput: total.throughput,
            byLocation,
        };
        return cb(null, retObj);
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

    // TODO propagate value of req.headers['x-scal-report-skip-cache']
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
                capabilities: getCapabilities(),
            };
            monitoring.crrCacheToProm(results);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.write(JSON.stringify(response));
            log.end().debug('report handler finished');
        }
        res.end();
    });
}

module.exports = {
    getCapabilities,
    reportHandler,
    _crrRequest,
    getCRRStats,
};
