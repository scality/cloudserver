const { config } = require('../../../Config');
const cache = require('./cache');
const constants = require('../../../../constants');
const { getTokenBucket } = require('./tokenBucket');

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
 * Check rate limit with pre-resolved configuration using token reservation
 *
 * Uses token bucket: Workers maintain local tokens granted by Redis.
 * Token consumption is pure in-memory (fast). Refills happen async in background.
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

    // Get or create token bucket for this bucket
    const tokenBucket = getTokenBucket(bucketName, limitConfig, log);

    // Try to consume a token (in-memory, no Redis)
    const allowed = tokenBucket.tryConsume();

    if (allowed) {
        log.trace('Rate limit check: allowed (token consumed)', {
            bucketName,
            tokensRemaining: tokenBucket.tokens,
        });
    } else {
        log.debug('Rate limit check: denied (no tokens available)', {
            bucketName,
            limit: limitConfig.limit,
            source: limitConfig.source,
        });
    }

    // Return inverse: callback expects "rateLimited" boolean
    // allowed=true → rateLimited=false
    // allowed=false → rateLimited=true
    return callback(null, !allowed);
}

module.exports = {
    getRateLimitFromCache,
    extractAndCacheRateLimitConfig,
    checkRateLimitWithConfig,
};
