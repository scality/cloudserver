const { config } = require('../../../Config');
const cache = require('./cache');
const metadata = require('../../../metadata/wrapper');
const constants = require('../../../../constants');

/**
 * Get cache TTL from config or use default
 * @returns {number} TTL in milliseconds
 */
function getCacheTTL() {
    return config.rateLimiting.bucket?.configCacheTTL ||
           constants.rateLimitDefaultConfigCacheTTL;
}

/**
 * Cache a rate limit config (or null)
 * @param {string} bucketName - Bucket name
 * @param {object|null} limitConfig - Config to cache
 */
function cacheConfig(bucketName, limitConfig) {
    const cacheKey = `bucket:${bucketName}`;
    cache.setCachedConfig(cacheKey, limitConfig, getCacheTTL());
}

/**
 * Resolve rate limit configuration for a bucket
 * Priority: Per-bucket config > Global default config > No limit
 *
 * Caches config (both presence and absence of limits) to avoid metadata lookups.
 *
 * @param {string} bucketName - Bucket name
 * @param {object} log - Logger instance
 * @param {function} callback - Callback(err, limitConfig)
 * @returns {undefined}
 */
function resolveRateLimit(bucketName, log, callback) {
    // Check cache first
    const cacheKey = `bucket:${bucketName}`;
    const cached = cache.getCachedConfig(cacheKey);
    if (cached !== undefined) {
        log.trace('Rate limit config cache hit', {
            bucketName,
            cached: cached !== null,
        });
        return callback(null, cached);
    }

    log.trace('Rate limit config cache miss', { bucketName });

    // Fetch bucket metadata
    return metadata.getBucket(bucketName, log, (err, bucket) => {
        if (err) {
            // If bucket doesn't exist, return no rate limit (null)
            // We only rate limit existing buckets
            if (err.NoSuchBucket || (err.is && err.is.NoSuchBucket())) {
                log.trace('Bucket does not exist, no rate limit applied', {
                    bucketName,
                });
                cacheConfig(bucketName, null);
                return callback(null, null);
            }

            // For other errors (metadata service issues), return the error
            log.debug('Failed to fetch bucket metadata for rate limit config', {
                bucketName,
                error: err,
            });
            return callback(err);
        }

        // Try per-bucket config first
        const bucketConfig = bucket.getRateLimitConfiguration();
        if (bucketConfig) {
            const data = bucketConfig.getData();
            const limitConfig = {
                limit: data.RequestsPerSecond.Limit,
                source: 'bucket',
            };

            cacheConfig(bucketName, limitConfig);
            log.debug('Resolved per-bucket rate limit config', {
                bucketName,
                limit: limitConfig.limit,
            });

            return callback(null, limitConfig);
        }

        // Fall back to global default config
        const globalLimit = config.rateLimiting.bucket?.defaultConfig?.limit;
        if (globalLimit !== undefined && globalLimit > 0) {
            const limitConfig = {
                limit: globalLimit,
                source: 'global',
            };

            cacheConfig(bucketName, limitConfig);
            log.debug('Using global default rate limit config', {
                bucketName,
                limit: limitConfig.limit,
            });

            return callback(null, limitConfig);
        }

        // No rate limiting configured - cache null to avoid repeated lookups
        cacheConfig(bucketName, null);
        log.trace('No rate limit configured for bucket', { bucketName });

        return callback(null, null);
    });
}

module.exports = {
    resolveRateLimit,
};
