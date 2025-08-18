'use strict';

// Recreated HDProxyd client with dynamic per-request endpoint selection,
// proper keep-alive agent tuning, fixed Range header, and safe streaming.
//
// Performance benefits:
// - Distributes requests across hdproxyd nodes (no per-process hot-spotting)
// - Avoids retrying mid-stream for PUT (correctness + avoids duplicate work)
// - Uses keep-alive and larger socket pools for higher throughput
// - Measures/feeds simple latency/circuit-breaker to avoid slow endpoints

const assert = require('assert');
const async = require('async');
const http = require('http');
const { pipeline } = require('stream');
const werelogs = require('werelogs');
const { http: httpAgent } = require('httpagent');
const { HdBootstrapBalancer } = require('./hdBootstrapBalancer');

class HDProxydError extends Error {
    constructor(message) {
        super(message);
        this.code = undefined;
        this.isExpected = false;
    }
}

/**
 * Create a request and handle common response behavior
 * @param {http.RequestOptions} req
 * @param {werelogs.RequestLogger} log
 * @param {(err?: HDProxydError, res?: http.IncomingMessage) => void} callback
 * @returns {http.ClientRequest}
 */
function _createRequest(req, log, callback) {
    let callbackCalled = false;
    const request = http.request(req, response => {
        callbackCalled = true;
        // Accept 200 OK, 206 Partial Content; allow 423 on DELETE (concurrent delete)
        if (response.statusCode !== 200 && response.statusCode !== 206 &&
            !(response.statusCode === 423 && req.method === 'DELETE')) {
            const error = new HDProxydError();
            error.code = response.statusCode;
            error.isExpected = true;
            log.debug('got expected response code:', { statusCode: response.statusCode });
            response.resume(); // Drain
            return callback(error);
        }
        return callback(undefined, response);
    }).on('error', err => {
        if (!callbackCalled) {
            callbackCalled = true;
            return callback(err);
        }
        if (err && err.code !== 'ERR_SOCKET_TIMEOUT') {
            log.error('got socket error after response', { err });
        }
        return null;
    });
    request.setNoDelay(true);
    return request;
}

function _parseBootstrapList(list) {
    return list.map(value => value.split(':'));
}

class HDProxydClient {
    /**
     * @param {{ bootstrap: string[], logApi?: typeof werelogs }} opts
     */
    constructor(opts) {
        const options = opts || {};
        this.bootstrap = opts.bootstrap === undefined ?
            [['localhost', '18888']] : _parseBootstrapList(opts.bootstrap);
        this.path = '/store/';
        this._balancer = new HdBootstrapBalancer((opts.bootstrap || ['localhost:18888']), {
            alpha: Number(process.env.HD_BLB_ALPHA) || 0.3,
            initialLatencyMs: Number(process.env.HD_BLB_INIT_MS) || 100,
            failureThreshold: Number(process.env.HD_BLB_FAIL_THR) || 2,
            coolDownMs: Number(process.env.HD_BLB_COOLDOWN_MS) || 5000,
            shuffle: true,
        });
        // default agent knobs (overridable via env)
        const keepAlive = process.env.HDCLIENT_KEEPALIVE ? process.env.HDCLIENT_KEEPALIVE === 'true' : true;
        const keepAliveMsecs = Number(process.env.HDCLIENT_KEEPALIVE_MS) || 2000;
        const maxSockets = Number(process.env.HDCLIENT_MAX_SOCKETS) || 512;
        const maxFreeSockets = Number(process.env.HDCLIENT_MAX_FREE_SOCKETS) || 512;
        const timeoutMs = Number(process.env.HDCLIENT_REQUEST_TIMEOUT_MS) || (10 * 60 * 1000);
        const freeSocketTimeout = Number(process.env.HDCLIENT_FREE_SOCKET_TIMEOUT_MS) || (60 * 1000);

        this.httpAgent = new httpAgent.Agent({
            keepAlive,
            keepAliveMsecs,
            maxSockets,
            maxFreeSockets,
            timeout: timeoutMs,
            freeSocketTimeout,
        });
        this._requestTimeoutMs = timeoutMs;
        this.setupLogging(options.logApi);
    }

    destroy() {
        this.httpAgent.destroy();
    }

    setupLogging(logApi) {
        this.logging = new (logApi || werelogs).Logger('HDProxydClient');
    }

    createLogger(reqUids) {
        return reqUids ?
            this.logging.newRequestLoggerFromSerializedUids(reqUids) :
            this.logging.newRequestLogger();
    }

    _getFirstReqUid(log) {
        let reqUids = [];
        if (log) {
            reqUids = log.getUids();
        }
        return reqUids[0];
    }

    _createRequestHeader(method, headers, key, params, log, endpoint) {
        const reqHeaders = headers || {};
        const reqUids = this._getFirstReqUid(log);
        reqHeaders['content-type'] = 'application/octet-stream';
        reqHeaders['X-Scal-Request-Uids'] = reqUids;
        reqHeaders['X-Scal-Trace-Ids'] = reqUids;
        if (params && params.range) {
            reqHeaders.Range = `bytes=${params.range[0]}-${params.range[1]}`;
        }
        const realPath = key === '/job/delete' ? key : (key ? `${this.path}${key}` : this.path);
        return {
            hostname: endpoint.host,
            port: endpoint.port,
            method,
            path: realPath,
            headers: reqHeaders,
            agent: this.httpAgent,
        };
    }

    _failover(method, stream, size, key, tries, log, callback, params, payload, endpoint) {
        const args = params === undefined ? {} : params;
        let counter = tries;
        const start = Date.now();
        const reqOpts = this._createRequestHeader(method, (args.headers || {}), key, args, log, endpoint);
        log.debug('sending request to hdproxyd', { method, key, args, counter, endpoint: endpoint.id });

        let receivedResponse = false;
        const isBatchDelete = key === '/job/delete';
        const onDone = (err, ret) => {
            if (err || !ret) {
                this._balancer.onError(endpoint.id, err || new Error('request_error'));
                if ((err && !err.isExpected) || !ret) {
                    if (receivedResponse === true) {
                        log.fatal('multiple responses from hdproxyd after response started', {
                            error: err, method: '_failover', size, objectKey: key,
                        });
                        return undefined;
                    }
                    // For streaming PUTs, do not retry mid-stream
                    const canRetry = !stream;
                    if (!canRetry || ++counter >= this.bootstrap.length) {
                        log.errorEnd('failover attempts exhausted or non-retriable', {
                            retries: counter, method,
                        });
                        return callback(err);
                    }
                    // pick a new endpoint and retry
                    const nextEp = this._balancer.nextEndpoint();
                    return this._failover(method, stream, size, key, counter, log, callback, params, payload, nextEp);
                }
            }
            receivedResponse = true;
            const latency = Date.now() - start;
            this._balancer.onSuccess(endpoint.id, latency);
            log.end().debug('request received response', { latency });
            return callback(err, ret);
        };

        const req = _createRequest(reqOpts, log, onDone);
        // Bind timeout
        req.setTimeout(this._requestTimeoutMs, () => {
            req.destroy(Object.assign(new Error('request_timeout'), { code: 'ERR_SOCKET_TIMEOUT' }));
        });

        if (stream) {
            // streaming PUT: write once, no mid-stream retry
            req.on('finish', () => log.debug('finished sending PUT chunks to hdproxyd', {
                component: 'hdproxydclient', method: '_failover', contentLength: size,
            }));
            const headers = reqOpts.headers; // eslint-disable-line no-unused-vars
            // Ensure content-length
            reqOpts.headers['content-length'] = size; // eslint-disable-line dot-notation
            pipeline(stream, req, err => {
                if (err) {
                    log.error('error in PUT pipeline', { error: err, method: '_failover' });
                    // onDone will be called from request 'error' handler via _createRequest
                }
            });
            stream.on('error', err => {
                log.error('error from readable stream', { error: err, method: '_failover', component: 'hdproxydclient' });
                req.destroy(err);
            });
        } else {
            // GET/DELETE and batch delete payloads
            const headers = reqOpts.headers; // eslint-disable-line no-unused-vars
            headers['content-length'] = isBatchDelete ? size : 0;
            req.end(payload);
        }
    }

    /**
     * PUT
     */
    put(stream, size, params, reqUids, callback) {
        const log = this.createLogger(reqUids);
        const ep = this._balancer.nextEndpoint();
        this._failover('POST', stream, size, '', 0, log, (err, response) => {
            if (response) {
                response.resume();
            }
            if (err || !response) {
                return callback(err);
            }
            if (!response.headers['scal-key']) {
                return callback(new HDProxydError('no key returned'));
            }
            const key = response.headers['scal-key'];
            response.on('end', () => callback(undefined, key));
            return null;
        }, params, undefined, ep);
    }

    /**
     * GET
     */
    get(key, range, reqUids, callback) {
        assert.strictEqual(typeof key, 'string');
        const log = this.createLogger(reqUids);
        const params = { range };
        const ep = this._balancer.nextEndpoint();
        this._failover('GET', null, 0, key, 0, log, callback, params, undefined, ep);
    }

    /**
     * DELETE
     */
    delete(key, reqUids, callback) {
        assert.strictEqual(typeof key, 'string');
        const log = this.createLogger(reqUids);
        const ep = this._balancer.nextEndpoint();
        this._failover('DELETE', null, 0, key, 0, log, (err, res) => {
            if (res) {
                res.resume();
                res.on('end', () => callback(err));
            } else {
                callback(err);
            }
        }, undefined, undefined, ep);
    }

    /**
     * BATCH DELETE
     */
    batchDelete(list, reqUids, callback) {
        assert.strictEqual(typeof list, 'object');
        assert(list.keys.every(k => typeof k === 'string'));
        const batches = [];
        while (list.keys.length > 0) {
            batches.push({ keys: list.keys.splice(0, 1000) });
        }
        async.eachLimit(batches, 5, (b, done) => {
            const log = this.createLogger(reqUids);
            const payload = Buffer.from(JSON.stringify(b.keys));
            const ep = this._balancer.nextEndpoint();
            this._failover('POST', null, payload.length, '/job/delete', 0, log, (err, res) => {
                if (res) {
                    res.resume();
                    res.on('end', () => done(err));
                } else {
                    done(err);
                }
            }, {}, payload, ep);
        }, err => callback(err || undefined));
    }

    /**
     * Healthcheck
     */
    healthcheck(log, callback) {
        const logger = log || this.createLogger();
        const ep = this._balancer.nextEndpoint();
        const req = {
            hostname: ep.host,
            port: ep.port,
            method: 'GET',
            path: '/metrics',
            headers: {
                'X-Scal-Request-Uids': logger.getSerializedUids(),
            },
            agent: this.httpAgent,
        };
        const request = _createRequest(req, logger, callback);
        request.end();
    }
}

module.exports = { HDProxydClient, HDProxydError };

