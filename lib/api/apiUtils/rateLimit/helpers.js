const { config } = require('../../../Config');
const cache = require('./cache');
const { getTokenBucket } = require('./tokenBucket');
const { policies: { actionMaps: { actionMapBucketRateLimit } } } = require('arsenal');

const rateLimitApiActions = Object.keys(actionMapBucketRateLimit);

/**
 * Extract rate limit configuration from bucket metadata or global rate limit configuration.
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
function extractBucketRateLimitConfig(bucket, bucketName, log) {
    // Try per-bucket config first
    const bucketConfig = bucket.getRateLimitConfiguration();
    if (bucketConfig) {
        const data = bucketConfig.getData();
        const limitConfig = {
            limit: data.RequestsPerSecond.Limit,
            burstCapacity: config.rateLimiting.bucket.defaultBurstCapacity * 1000,
            source: 'bucket',
        };

        log.debug('Extracted per-bucket rate limit config', {
            bucketName,
            limit: limitConfig.limit,
            burstCapacity: config.rateLimiting.bucket.defaultBurstCapacity * 1000,
        });

        return limitConfig;
    }

    // Fall back to global default config
    const globalLimit = config.rateLimiting.bucket?.defaultConfig?.limit;
    if (globalLimit !== undefined && globalLimit > 0) {
        const limitConfig = {
            limit: globalLimit,
            burstCapacity: config.rateLimiting.bucket.defaultBurstCapacity * 1000,
            source: 'global',
        };

        log.debug('Using global default rate limit config', {
            bucketName,
            limit: limitConfig.limit,
        });

        return limitConfig;
    }

    // No rate limiting configured - cache null to avoid repeated lookups
    log.trace('No rate limit configured for bucket', { bucketName });
    return null;
}

function extractRateLimitConfigFromRequest(request) {
    const applyRateLimit = config.rateLimiting?.enabled
        && !rateLimitApiActions.includes(request.apiMethod) // Don't limit any rate limit admin actions
        && !request.isInternalServiceRequest;               // Don't limit any calls from internal services

    if (!applyRateLimit) {
        return { needsCheck: false };
    }

    const limitConfigs = {};
    let needsCheck = false;

    if (request.accountLimits) {
        limitConfigs.account = {
            ...request.accountLimits,
            source: 'account',
        };
        needsCheck = true;
    }

    if (request.bucketName) {
        const cachedConfig = cache.getCachedConfig(cache.namespace.bucket, request.bucketName);
        if (cachedConfig) {
            limitConfigs.bucket = cachedConfig;
            needsCheck = true;
        }

        if (!request.accountLimits) {
            const cachedOwner = cache.getCachedBucketOwner(request.bucketName);
            if (cachedOwner !== undefined) {
                const cachedConfig = cache.getCachedConfig(cache.namespace.account, cachedOwner);
                if (cachedConfig) {
                    limitConfigs.account = cachedConfig;
                    limitConfigs.bucketOwner = cachedOwner;
                    needsCheck = true;
                }
            }
        }
    }

    return { needsCheck, limitConfigs };
}

function checkRateLimitsForRequest(checks, log) {
    const buckets = [];
    for (const check of checks) {
        const bucket = getTokenBucket(check.resourceClass, check.resourceId, check.measure, check.config, log);
        if (!bucket.hasCapacity()) {
            log.debug('Rate limit check: denied (no tokens available)', {
                resourceClass: bucket.resourceClass,
                resourceId: bucket.resourceId,
                measure: bucket.measure,
                limit: bucket.limitConfig.limit,
                source: bucket.limitConfig.source,
            });

            return { allowed: false, rateLimitSource: `${check.resourceClass}:${check.source}`};
        }

        buckets.push(bucket);

        log.trace('Rate limit check: allowed (token consumed)', {
            resourceClass: bucket.resourceClass,
            resourceId: bucket.resourceId,
            measure: bucket.measure,
            source: bucket.limitConfig.source,
        });
    }

    buckets.forEach(bucket => bucket.tryConsume());
    return { allowed: true };
}

module.exports = {
    rateLimitApiActions,
    extractBucketRateLimitConfig,
    extractRateLimitConfigFromRequest,
    checkRateLimitsForRequest,
};
