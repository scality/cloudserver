/*
 * Dynamic hdproxyd bootstrap balancer
 *
 * Why this brings performance benefits:
 * - Avoids per-process hot-spotting: instead of pinning to a single endpoint until failure,
 *   it distributes requests across all hdproxyd nodes (round-robin with latency awareness).
 * - Reduces queueing on a single node and better uses aggregate backend capacity.
 * - Skips unhealthy/slow endpoints quickly using a simple circuit-breaker + EWMA latency,
 *   improving tail latencies and error rates under partial failures.
 *
 * Integrate by calling nextEndpoint() before each request, and reporting outcomes via
 * onSuccess()/onError(). For streaming PUTs do NOT retry mid-stream; select once, then stream.
 */
'use strict';

const crypto = require('crypto');

class HdBootstrapBalancer {
    /**
     * @param {string[]} bootstrapList - array like ['host:port', ...]
     * @param {object} [options]
     * @param {number} [options.alpha=0.3] - EWMA smoothing factor (0..1]
     * @param {number} [options.initialLatencyMs=100] - default EWMA latency
     * @param {number} [options.failureThreshold=2] - consecutive failures to open circuit
     * @param {number} [options.coolDownMs=5000] - how long to avoid a failing endpoint
     * @param {boolean} [options.shuffle=true] - shuffle endpoints at init
     */
    constructor(bootstrapList, options = {}) {
        if (!Array.isArray(bootstrapList) || bootstrapList.length === 0) {
            throw new Error('HdBootstrapBalancer: bootstrapList must be a non-empty array');
        }
        const {
            alpha = 0.3,
            initialLatencyMs = 100,
            failureThreshold = 2,
            coolDownMs = 5000,
            shuffle = true,
        } = options;

        this._alpha = Math.min(1, Math.max(0.01, alpha));
        this._initialLatencyMs = initialLatencyMs;
        this._failureThreshold = Math.max(1, failureThreshold);
        this._coolDownMs = Math.max(100, coolDownMs);
        this._rr = 0;

        const list = shuffle ? HdBootstrapBalancer._shuffle(bootstrapList) : bootstrapList.slice();
        this._nodes = list.map((s, idx) => {
            const [host, port] = s.split(':');
            return {
                id: `${host}:${port}`,
                host,
                port,
                ewmaMs: initialLatencyMs,
                failures: 0,
                nextRetryAt: 0,
                seed: idx,
            };
        });
        // Randomize RR start per process to spread initial picks across nodes
        this._rr = crypto.randomBytes(1)[0] % this._nodes.length;
    }

    /**
     * Pick the next endpoint for a request, preferring healthy + lowest EWMA latency.
     * @returns {{ id: string, host: string, port: string }} endpoint
     */
    nextEndpoint() {
        const now = Date.now();
        const eligible = this._nodes.filter(n => now >= n.nextRetryAt);
        const pool = eligible.length > 0 ? eligible : this._nodes;

        // Select by min EWMA, with RR tie-break to avoid lockstep
        let best = null;
        for (let i = 0; i < pool.length; i++) {
            const idx = (this._rr + i) % pool.length;
            const n = pool[idx];
            if (!best || n.ewmaMs < best.ewmaMs) {
                best = n;
            }
        }
        // Advance rr pointer among full set to prevent bias
        this._rr = (this._rr + 1) % this._nodes.length;
        return { id: best.id, host: best.host, port: best.port };
    }

    /**
     * Report a successful request duration so EWMA can adapt.
     * @param {string} id - endpoint id "host:port"
     * @param {number} durationMs - observed end-to-end or connect-to-first-byte latency
     */
    onSuccess(id, durationMs) {
        const n = this._byId(id);
        if (!n) { return; }
        const sample = Math.max(1, durationMs || this._initialLatencyMs);
        n.ewmaMs = this._alpha * sample + (1 - this._alpha) * n.ewmaMs;
        n.failures = 0;
        n.nextRetryAt = 0;
    }

    /**
     * Report an error so the balancer can temporarily avoid this endpoint.
     * @param {string} id - endpoint id "host:port"
     * @param {Error} err
     */
    onError(id, err) {
        const n = this._byId(id);
        if (!n) { return; }
        n.failures += 1;
        // On immediate connect errors/timeouts, penalize more by bumping EWMA
        if (HdBootstrapBalancer._isSevere(err)) {
            n.ewmaMs *= 2;
        }
        if (n.failures >= this._failureThreshold) {
            n.nextRetryAt = Date.now() + this._coolDownMs;
            n.failures = 0; // reset counter after opening circuit
        }
    }

    _byId(id) {
        return this._nodes.find(n => n.id === id);
    }

    static _isSevere(err) {
        const code = err && (err.code || err.name);
        return code === 'ECONNREFUSED' || code === 'ECONNRESET' || code === 'ETIMEDOUT' ||
               code === 'EHOSTUNREACH' || code === 'ENETUNREACH' || code === 'ERR_SOCKET_TIMEOUT';
    }

    static _shuffle(arr) {
        const a = arr.slice();
        for (let i = a.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [a[i], a[j]] = [a[j], a[i]];
        }
        return a;
    }
}

module.exports = { HdBootstrapBalancer };

