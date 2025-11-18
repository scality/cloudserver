const { config } = require('../../../Config');
const cache = require('./cache');
const { evaluate, calculateInterval } = require('./gcra');
const constants = require('../../../../constants');

/**
 * Get rate limit configuration from cache only (no metadata fetch)
 *
 * @param {string} bucketName - Bucket name
 * @returns {object|null|undefined} Rate limit config, null (no limit), or undefined (not cached)
 */
function getRateLimitFromCache(bucketName) {
    const cacheKey = `bucket:${bucketName}`;
    return cache.getCachedConfig(cacheKey);
}

/**
 * Extract rate limit configuration from bucket metadata and cache it
 *
 * Resolves in priority order:
 * 1. Per-bucket configuration (from bucket metadata)
 * 2. Global default configuration
 * 3. No rate limiting (null)
 *
 * @param {object} bucket - Bucket metadata object
 * @param {string} bucketName - Bucket name
 * @param {object} log - Logger instance
 * @returns {object|null} Rate limit config or null if no limit
 */
function extractAndCacheRateLimitConfig(bucket, bucketName, log) {
    const cacheKey = `bucket:${bucketName}`;
    const cacheTTL = config.rateLimiting.bucket?.configCacheTTL ||
                     constants.rateLimitDefaultConfigCacheTTL;

    // Try per-bucket config first
    const bucketConfig = bucket.getRateLimitConfiguration();
    if (bucketConfig) {
        const data = bucketConfig.getData();
        const limitConfig = {
            limit: data.RequestsPerSecond.Limit,
            source: 'bucket',
        };

        cache.setCachedConfig(cacheKey, limitConfig, cacheTTL);
        log.debug('Extracted per-bucket rate limit config', {
            bucketName,
            limit: limitConfig.limit,
        });

        return limitConfig;
    }

    // Fall back to global default config
    const globalLimit = config.rateLimiting.bucket?.defaultConfig?.limit;
    if (globalLimit !== undefined && globalLimit > 0) {
        const limitConfig = {
            limit: globalLimit,
            source: 'global',
        };

        cache.setCachedConfig(cacheKey, limitConfig, cacheTTL);
        log.debug('Using global default rate limit config', {
            bucketName,
            limit: limitConfig.limit,
        });

        return limitConfig;
    }

    // No rate limiting configured - cache null to avoid repeated lookups
    cache.setCachedConfig(cacheKey, null, cacheTTL);
    log.trace('No rate limit configured for bucket', { bucketName });

    return null;
}

/**
 * Check rate limit with pre-resolved configuration
 *
 * Uses GCRA algorithm to determine if request should be rate limited.
 * Updates counter if request is allowed.
 *
 * @param {string} bucketName - Bucket name
 * @param {object|null} limitConfig - Pre-resolved rate limit config
 * @param {object} log - Logger instance
 * @param {function} callback - Callback(err, rateLimited boolean)
 * @returns {undefined}
 */
function checkRateLimitWithConfig(bucketName, limitConfig, log, callback) {
    // No rate limiting configured
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
    const emptyAt = cache.getCounter(counterKey) || 0;
    const arrivedAt = Date.now();

    log.debug('Checking rate limit with GCRA', {
        bucketName,
        limit: limitConfig.limit,
        source: limitConfig.source,
        interval,
        emptyAt,
        arrivedAt,
    });

    // Evaluate GCRA
    const result = evaluate(emptyAt, arrivedAt, interval, bucketSize);

    // Update counter if allowed
    if (result.allowed) {
        cache.setCounter(counterKey, result.newEmptyAt);
        log.debug('Rate limit check: allowed', {
            bucketName,
            newEmptyAt: result.newEmptyAt,
        });
    } else {
        log.debug('Rate limit check: denied', {
            bucketName,
            limit: limitConfig.limit,
            allowAt: result.newEmptyAt,
            retryAfterMs: result.newEmptyAt - arrivedAt,
        });
    }

    return callback(null, !result.allowed);
}

module.exports = {
    getRateLimitFromCache,
    extractAndCacheRateLimitConfig,
    checkRateLimitWithConfig,
};
