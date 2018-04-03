const fs = require('fs');
const os = require('os');

const { errors, ipCheck, s3middleware } = require('arsenal');
const async = require('async');
const request = require('request');

const escapeForXml = s3middleware.escapeForXml;
const constants = require('../../constants');
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
    const { host, port } = config.backbeat;
    const params = { url: `http://${host}:${port}/_/metrics/crr/all` };
    return request.get(params, (err, res) => {
        if (err) {
            log.error('failed to get CRR stats', {
                method: 'getCRRStats',
                error: err,
            });
            return cb(null, {});
        }
        const { completions, backlog, throughput } = res.body;
        if (!completions || !backlog || !throughput) {
            log.error('could not get metrics from backbeat', {
                method: 'getCRRStats',
            });
            return cb(null, {});
        }
        const stats = {
            completions: {
                count: completions.results.count,
                size: parseFloat(completions.results.size) * 1000,
            },
            backlog: {
                count: backlog.results.count,
                size: parseFloat(backlog.results.size) * 1000,
            },
            throughput: {
                count: parseFloat(throughput.results.count) * 1000,
                size: parseFloat(throughput.results.size) * 1000,
            },
        };
        return cb(null, stats);
    }).json();
}

function listBuckets(bucketUsers, splitter, log, cb) {
    const authData = config.authData;
    const canonicalID = authData.accounts[0].canonicalID;
    const params = {
        prefix: `${canonicalID}${splitter}`,
        maxKeys: 10000,
    };
    return metadata.listObject(bucketUsers, params, log, (err, list) => {
        if (err) {
            return cb(err);
        }
        return cb(null, list);
    });
}

function consolidateData(resultList) {
    const retData = {};
    resultList.forEach(objList => {
        Object.keys(objList).forEach(backend => {
            if (!retData[backend]) {
                retData[backend] = objList[backend];
            } else {
                retData[backend].curr += objList[backend].curr;
                retData[backend].prev += objList[backend].prev;
            }
        });
    });
    return retData;
}

function getTotalDataManaged(log, cb) {
    log.debug('getting total data managed', { method: 'getTotalDataManaged' });
    return async.waterfall([
        function listUserBuckets(next) {
            const bucketUsers = constants.usersBucket;
            const splitter = constants.splitter;
            return listBuckets(bucketUsers, splitter, log, (err, list) => {
                if (err && err.NoSuchBucket) {
                    log.trace('no buckets found');
                    const oldBucketUsers = constants.oldUsersBucket;
                    const oldSplitter = constants.oldSplitter;
                    return listBuckets(oldBucketUsers, oldSplitter, log,
                    (err, list) => {
                        if (err) {
                            return next(err);
                        }
                        return next(null, list, oldSplitter);
                    });
                }
                if (err) {
                    log.debug('unable to retrieve buckets');
                    return next(err);
                }
                return next(null, list, splitter);
            });
        },
        function createDataSummary(listResults, splitter, next) {
            const buckets = listResults.Contents;
            return async.map(buckets, (bucket, done) => {
                const bucketName = bucket.key.split(splitter)[1];
                const listParams = {
                    listingType: 'DelimiterVersions',
                    maxKeys: constants.listingHardLimit,
                };
                let lastKey = listParams.keyMarker ?
                    escapeForXml(listParams.keyMarker) : undefined;
                let isTruncated = true;

                const dataSummary = {
                    total: { curr: 0, prev: 0 },
                };
                Object.keys(config.locationConstraints).forEach(location => {
                    dataSummary[location] = { curr: 0, prev: 0 };
                });

                // create a queue for handling each object
                const q = async.queue((object, callback) => {
                    const getParams = {
                        versionId: object.versionId,
                    };
                    metadata.getObjectMD(bucketName, object.Key,
                    getParams, log, (err, objMD) => {
                        if (err) {
                            return callback(err);
                        }
                        const backends = objMD.replicationInfo.backends;
                        dataSummary.total[object.version] +=
                            objMD['content-length'];
                        backends.forEach(backend => {
                            const { site, status } = backend;
                            if (status === 'COMPLETED') {
                                dataSummary[site][object.version] +=
                                    objMD['content-length'];
                            }
                        });
                        return callback();
                    });
                }, 100);
                let failed = false;
                q.error = (err, task) => {
                    log.debug('failed queue task', { task });
                    if (failed) {
                        return undefined;
                    }
                    failed = true;
                    q.kill();
                    return done(err);
                };
                q.drain = () => {
                    log.debug('data summary completed');
                    q.kill();
                    return done(null, dataSummary);
                };

                return async.whilst(() => isTruncated, done => {
                    metadata.listObject(bucketName, listParams, log,
                    (err, res) => {
                        if (err) {
                            return done(err);
                        }
                        isTruncated = res.IsTruncated;
                        listParams.keyMarker = res.NextKeyMarker;
                        listParams.versionIdMarker = res.NextVersionIdMarker;
                        res.Versions.forEach(object => {
                            const curKey = escapeForXml(object.key);
                            const version = lastKey !== curKey ?
                                'curr' : 'prev';
                            lastKey = curKey;
                            q.push({
                                Key: object.key,
                                versionId: object.value.VersionId,
                                version,
                            }, err => {
                                if (err) {
                                    log.debug('error retrieving object MD');
                                }
                            });
                        });
                        return done();
                    });
                });
            }, (err, results) => {
                if (err) {
                    log.error('failed to list objects');
                    return next(err);
                }
                const totalDataManaged = consolidateData(results);
                return next(null, totalDataManaged);
            });
        },
    ], (err, results) => {
        if (err) {
            return cb(err);
        }
        return cb(null, results);
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
        getTotalDataManaged: cb => getTotalDataManaged(log, cb),
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
                totalDataManaged: results.getTotalDataManaged,
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
