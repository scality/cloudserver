const fs = require('fs');
const os = require('os');

const { errors, ipCheck } = require('arsenal');
const async = require('async');
const request = require('request');

const config = require('../Config').config;
const data = require('../data/wrapper');
const metadata = require('../metadata/wrapper');
const monitoring = require('../utilities/monitoringHandler');

const REPORT_MODEL_VERSION = 1;
const ASYNCLIMIT = 5;

const REQ_PATHS = {
    crrSchedules: '/_/crr/resume/all',
    crrStatus: '/_/crr/status',
    crrMetricPrefix: '/_/metrics/crr',
};

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
        locationTypeNFS: true,
        locationTypeCephRadosGW: true,
        preferredReadLocation: true,
        managedLifecycle: true,
        secureChannelOptimizedPath: hasWSOptionalDependencies(),
        s3cIngestLocation: true,
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

const _makeRequest = (endpoint, path, cb) => {
    const url = `${endpoint}${path}`;
    request({ url, json: true }, (error, response, body) => {
        if (error) {
            return cb(error);
        }
        if (response.statusCode >= 400) {
            return cb('responseError', body);
        }
        if (body) {
            return cb(null, body);
        }
        return cb(null, {});
    });
};

function _crrRequest(endpoint, site, log, cb) {
    const path = `${REQ_PATHS.crrMetricPrefix}/${site}`;
    return _makeRequest(endpoint, path, (err, res) => {
        if (err) {
            if (err === 'responseError') {
                log.error('error response from backbeat api', {
                    error: res,
                    method: '_crrRequest',
                });
            } else {
                log.error('unable to perform request to backbeat api', {
                    error: err,
                    method: '_crrRequest',
                });
            }
            return cb(null, {});
        }
        const { completions, failures, backlog, throughput, pending } = res;
        if (!completions || !failures || !backlog || !throughput || !pending) {
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
            pending: {
                count: parseInt(pending.results.count, 10),
                size: parseInt(pending.results.size, 10),
            },
        };
        return cb(null, stats);
    });
}

function getCRRStats(log, cb, _testConfig) {
    log.debug('request CRR metrics from backbeat api', {
        method: 'getCRRStats',
    });
    const { replicationEndpoints, backbeat } = _testConfig || config;
    const { host, port } = backbeat;
    const endpoint = `http://${host}:${port}`;
    const sites = replicationEndpoints.map(endpoint => endpoint.site);
    return async.parallel({
        all: done => _crrRequest(endpoint, 'all', log, done),
        byLocation: done => async.mapLimit(sites, ASYNCLIMIT,
            (site, next) => _crrRequest(endpoint, site, log, (err, res) => {
                if (err) {
                    log.debug('Error in retrieving site metrics', {
                        method: 'getCRRStats',
                        error: err,
                        site,
                    });
                    return next(null, { site, stats: {} });
                }
                return next(null, { site, stats: res });
            }),
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
            }),
    }, (err, res) => {
        if (err) {
            log.error('failed to get CRR stats', {
                method: 'getCRRStats',
                error: err,
            });
            return cb(null, {});
        }
        const all = (res && res.all) || {};
        const byLocation = (res && res.byLocation) || {};
        const retObj = {
            completions: all.completions,
            failures: all.failures,
            pending: all.pending,
            backlog: all.backlog,
            throughput: all.throughput,
            byLocation,
        };
        return cb(null, retObj);
    });
}

function getReplicationStates(log, cb, _testConfig) {
    log.debug('requesting location replications states from backbeat api',
        {
            method: 'getReplicationStates',
        });
    const { host, port } = _testConfig || config.backbeat;
    const endpoint = `http://${host}:${port}`;
    async.parallel({
        states: done => _makeRequest(endpoint, REQ_PATHS.crrStatus, done),
        schedules: done => _makeRequest(endpoint, REQ_PATHS.crrSchedules, done),
    }, (err, res) => {
        if (err) {
            if (err === 'responseError') {
                log.error('error response from backbeat api', {
                    error: res,
                    method: 'getReplicationStates',
                });
            } else {
                log.error('unable to perform request to backbeat api', {
                    error: err,
                    method: 'getReplicationStates',
                });
            }
            return cb(null, {});
        }
        const locationSchedules = {};
        Object.keys(res.schedules).forEach(loc => {
            const val = res.schedules[loc];
            if (!isNaN(Date.parse(val))) {
                locationSchedules[loc] = new Date(val);
            }
        });
        const retObj = {
            states: res.states || {},
            schedules: locationSchedules,
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
        getReplicationStates: cb => getReplicationStates(log, cb),
    },
    (err, results) => {
        if (err) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.write(JSON.stringify(err));
            log.errorEnd('could not gather report', { error: err });
        } else {
            const getObjectCount = results.getObjectCount;
            const crrStatsObj = Object.assign({}, results.getCRRStats);
            crrStatsObj.stalled = { count: getObjectCount.stalled || 0 };
            delete getObjectCount.stalled;
            const response = {
                utcTime: new Date(),
                uuid: results.getUUID,
                reportModelVersion: REPORT_MODEL_VERSION,

                mdDiskUsage: results.getMDDiskUsage,
                dataDiskUsage: results.getDataDiskUsage,
                serverVersion: results.getVersion,
                systemStats: getSystemStats(),
                itemCounts: getObjectCount,
                crrStats: crrStatsObj,
                repStatus: results.getReplicationStates,
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
    getReplicationStates,
};
