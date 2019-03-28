const fs = require('fs');
const os = require('os');

const { errors, ipCheck } = require('arsenal');
const async = require('async');
const request = require('request');

const config = require('../Config').config;
const { data } = require('../data/wrapper');
const metadata = require('../metadata/wrapper');
const monitoring = require('../utilities/monitoringHandler');

const REPORT_MODEL_VERSION = 1;
const ASYNCLIMIT = 5;

const REQ_PATHS = {
    crrSchedules: '/_/crr/resume/all',
    crrStatus: '/_/crr/status',
    crrMetricPrefix: '/_/metrics/crr',
    ingestionSchedules: '/_/ingestion/resume/all',
    ingestionStatus: '/_/ingestion/status',
    ingestionMetricPrefix: '/_/metrics/ingestion',
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
        managedLifecycleTransition: true,
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

function _crrMetricRequest(endpoint, site, log, cb) {
    const path = `${REQ_PATHS.crrMetricPrefix}/${site}`;
    return _makeRequest(endpoint, path, (err, res) => {
        if (err) {
            if (err === 'responseError') {
                log.error('error response from backbeat api', {
                    error: res,
                    method: '_crrMetricRequest',
                });
            } else {
                log.error('unable to perform request to backbeat api', {
                    error: err,
                    method: '_crrMetricRequest',
                });
            }
            return cb(null, {});
        }
        const { completions, failures, backlog, throughput, pending } = res;
        if (!completions || !failures || !backlog || !throughput || !pending) {
            log.error('could not get metrics from backbeat', {
                method: '_crrMetricRequest',
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

function _ingestionMetricRequest(endpoint, site, log, cb) {
    const path = `${REQ_PATHS.ingestionMetricPrefix}/${site}`;
    return _makeRequest(endpoint, path, (err, res) => {
        if (err) {
            if (err === 'responseError') {
                log.error('error response from backbeat api', {
                    error: res,
                    method: '_ingestionMetricRequest',
                });
            } else {
                log.error('unable to perform request to backbeat api', {
                    error: err,
                    method: '_ingestionMetricRequest',
                });
            }
            return cb(null, {});
        }
        const { completions, throughput, pending } = res;
        if (!completions || !throughput || !pending) {
            log.error('could not get metrics from backbeat', {
                method: '_ingestionMetricRequest',
            });
            return cb(null, {});
        }
        const stats = {
            completions: {
                count: parseInt(completions.results.count, 10),
            },
            throughput: {
                count: parseInt(throughput.results.count, 10),
            },
            pending: {
                count: parseInt(pending.results.count, 10),
            },
        };
        return cb(null, stats);
    });
}

function _getMetricsByLocation(endpoint, sites, requestMethod, log, cb) {
    async.mapLimit(
        sites,
        ASYNCLIMIT,
        (site, next) => requestMethod(endpoint, site, log, (err, res) => {
            if (err) {
                log.debug('Error in retrieving site metrics', {
                    method: '_getMetricsByLocation',
                    error: err,
                    site,
                    requestType: requestMethod.name,
                });
                return next(null, { site, stats: {} });
            }
            return next(null, { site, stats: res });
        }),
        (err, locStats) => {
            if (err) {
                log.error('failed to get stats for site', {
                    method: '_getMetricsByLocation',
                    error: err,
                    requestType: requestMethod.name,
                });
                return cb(null, {});
            }
            const retObj = {};
            locStats.forEach(locStat => {
                retObj[locStat.site] = locStat.stats;
            });
            return cb(null, retObj);
        }
    );
}

function _getMetrics(sites, requestMethod, log, cb, _testConfig) {
    const conf = (_testConfig && _testConfig.backbeat) || config.backbeat;
    const { host, port } = conf;
    const endpoint = `http://${host}:${port}`;
    return async.parallel({
        all: done => requestMethod(endpoint, 'all', log, done),
        byLocation: done => _getMetricsByLocation(endpoint, sites,
            requestMethod, log, done),
    }, (err, res) => {
        if (err) {
            return cb(err);
        }
        const all = (res && res.all) || {};
        const byLocation = (res && res.byLocation) || {};
        const retObj = Object.assign({}, all, { byLocation });
        return cb(null, retObj);
    });
}

function getCRRMetrics(log, cb, _testConfig) {
    log.debug('request CRR metrics from backbeat api', {
        method: 'getCRRMetrics',
    });
    const { replicationEndpoints } = _testConfig || config;
    const sites = replicationEndpoints.map(endpoint => endpoint.site);
    return _getMetrics(sites, _crrMetricRequest, log, (err, retObj) => {
        if (err) {
            log.error('failed to get CRR stats', {
                method: 'getCRRMetrics',
                error: err,
            });
            return cb(null, {});
        }
        return cb(null, retObj);
    }, _testConfig);
}

function getIngestionMetrics(sites, log, cb, _testConfig) {
    log.debug('request Ingestion metrics from backbeat api', {
        method: 'getIngestionMetrics',
    });
    return _getMetrics(sites, _ingestionMetricRequest, log, (err, retObj) => {
        if (err) {
            log.error('failed to get Ingestion stats', {
                method: 'getIngestionMetrics',
                error: err,
            });
            return cb(null, {});
        }
        return cb(null, retObj);
    }, _testConfig);
}

function _getStates(statusPath, schedulePath, log, cb, _testConfig) {
    const conf = (_testConfig && _testConfig.backbeat) || config.backbeat;
    const { host, port } = conf;
    const endpoint = `http://${host}:${port}`;
    async.parallel({
        states: done => _makeRequest(endpoint, statusPath, done),
        schedules: done => _makeRequest(endpoint, schedulePath, done),
    }, (err, res) => {
        if (err) {
            return cb(err);
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

function getReplicationStates(log, cb, _testConfig) {
    log.debug('requesting replication location states from backbeat api', {
        method: 'getReplicationStates',
    });
    const { crrStatus, crrSchedules } = REQ_PATHS;
    return _getStates(crrStatus, crrSchedules, log, (err, res) => {
        if (err) {
            if (err === 'responseError') {
                log.error('error response from backbeat api', {
                    error: res,
                    method: 'getReplicationStates',
                    service: 'replication',
                });
            } else {
                log.error('unable to perform request to backbeat api', {
                    error: err,
                    method: 'getReplicationStates',
                    service: 'replication',
                });
            }
            return cb(null, {});
        }
        return cb(null, res);
    }, _testConfig);
}

function getIngestionStates(log, cb, _testConfig) {
    log.debug('requesting location ingestion states from backbeat api', {
        method: 'getIngestionStates',
    });
    const { ingestionStatus, ingestionSchedules } = REQ_PATHS;
    return _getStates(ingestionStatus, ingestionSchedules, log, (err, res) => {
        if (err) {
            if (err === 'responseError') {
                log.error('error response from backbeat api', {
                    error: res,
                    method: 'getIngestionStates',
                    service: 'ingestion',
                });
            } else {
                log.error('unable to perform request to backbeat api', {
                    error: err,
                    method: 'getIngestionStates',
                    service: 'ingestion',
                });
            }
            return cb(null, {});
        }
        return cb(null, res);
    }, _testConfig);
}

function getIngestionInfo(log, cb, _testConfig) {
    log.debug('requesting location ingestion info from backbeat api', {
        method: 'getIngestionInfo',
    });
    async.waterfall([
        done => getIngestionStates(log, done, _testConfig),
        (stateObj, done) => {
            // if getIngestionStates returned an error or the returned object
            // did not return an expected response
            if (Object.keys(stateObj).length === 0 || !stateObj.states) {
                log.debug('no ingestion locations found', {
                    method: 'getIngestionInfo',
                });
                return done(null, stateObj, {});
            }
            const sites = Object.keys(stateObj.states);
            return getIngestionMetrics(sites, log, (err, res) => {
                if (err) {
                    log.error('failed to get Ingestion stats', {
                        method: 'getIngestionInfo',
                        error: err,
                    });
                    return done(null, stateObj, {});
                }
                return done(null, stateObj, res);
            }, _testConfig);
        },
    ], (err, stateObj, metricObj) => {
        if (err) {
            log.error('failed to get ingestion info', {
                method: 'getIngestionInfo',
                error: err,
            });
            return cb(null, {});
        }
        return cb(null, {
            metrics: metricObj,
            status: stateObj,
        });
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
        getCRRMetrics: cb => getCRRMetrics(log, cb),
        getReplicationStates: cb => getReplicationStates(log, cb),
        getIngestionInfo: cb => getIngestionInfo(log, cb),
    },
    (err, results) => {
        if (err) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.write(JSON.stringify(err));
            log.errorEnd('could not gather report', { error: err });
        } else {
            const getObjectCount = results.getObjectCount;
            const crrStatsObj = Object.assign({}, results.getCRRMetrics);
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
                ingestStats: results.getIngestionInfo.metrics,
                ingestStatus: results.getIngestionInfo.status,
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
    _crrMetricRequest,
    getCRRMetrics,
    getReplicationStates,
    _ingestionMetricRequest,
    getIngestionMetrics,
    getIngestionStates,
    getIngestionInfo,
};
