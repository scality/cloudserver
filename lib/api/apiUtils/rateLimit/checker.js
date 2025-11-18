const { config } = require('../../../Config');
const { getCounter, setCounter } = require('./cache');
const { evaluate, calculateInterval } = require('./gcra');
const { resolveRateLimit } = require('./limitResolver');
const constants = require('../../../../constants');

/**
 * Check if request should be rate limited
 *
 * Enhanced logging provides visibility into rate limiting decisions:
 * - trace: Cache hits, config resolution
 * - debug: Rate limit decisions, config details
 * - info: Rate limiting events (first limit, changes)
 * - error: Failures (fail open)
 *
 * @param {string} bucketName - Bucket name
 * @param {object} log - Logger instance
 * @param {function} callback - Callback(err, rateLimited)
 * @returns {undefined}
 */
function checkRateLimit(bucketName, log, callback) {
    // Skip if rate limiting is disabled
    if (!config.rateLimiting?.enabled) {
        return callback(null, false);
    }

    // Resolve configuration for this bucket (uses cache)
    return resolveRateLimit(bucketName, log, (err, limitConfig) => {
        if (err) {
            // Fail open: don't rate limit on errors
            log.error('Failed to resolve rate limit config, failing open', {
                bucketName,
                error: err.message,
                failOpen: true,
            });
            return callback(null, false);
        }

        // No rate limiting configured for this bucket
        if (!limitConfig || limitConfig.limit === 0) {
            return callback(null, false);
        }

        // Calculate interval for this limit
        const nodes = config.rateLimiting.nodes || 1;
        const workers = config.clusters || 1;
        const interval = calculateInterval(limitConfig.limit, nodes, workers);

        // Get burst capacity (default to 1 if not configured)
        const burstCapacity = config.rateLimiting.bucket?.defaultBurstCapacity ||
                              constants.rateLimitDefaultBurstCapacity;
        const bucketSize = burstCapacity * 1000;

        // Get counter (in-memory only, no sync with other workers)
        const counterKey = `bucket:${bucketName}:rps`;
        const emptyAt = getCounter(counterKey) || 0;
        const arrivedAt = Date.now();

        // Evaluate GCRA
        const result = evaluate(emptyAt, arrivedAt, interval, bucketSize);

        // Update counter if allowed
        if (result.allowed) {
            setCounter(counterKey, result.newEmptyAt);
        }

        // Enhanced logging based on decision
        const logLevel = result.allowed ? 'debug' : 'info';
        log[logLevel]('Rate limit check completed', {
            bucketName,
            rateLimited: !result.allowed,
            rateLimitSource: limitConfig.source,
            decision: result.allowed ? 'allowed' : 'denied',
            limit: limitConfig.limit,
            perWorkerRate: limitConfig.limit / nodes / workers,
            interval,
            nodes,
            workers,
            burstCapacity,
            emptyAt,
            arrivedAt,
            newEmptyAt: result.newEmptyAt,
        });

        return callback(null, !result.allowed);
    });
}

module.exports = {
    checkRateLimit,
};
