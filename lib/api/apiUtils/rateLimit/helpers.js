const { config } = require('../../../Config');
const cache = require('./cache');
const { getTokenBucket } = require('./tokenBucket');
const {
    policies: {
        actionMaps: { actionMapBucketRateLimit },
    },
} = require('arsenal');

const rateLimitApiActions = Object.keys(actionMapBucketRateLimit);

function requestNeedsRateCheck(request) {
    // Is the feature enabled?
    if (!config.rateLimiting.enabled) {
        return false;
    }

    // Is the request an internal or rate limit admin operation?
    if (rateLimitApiActions.includes(request.apiMethod) || request.isInternalServiceRequest) {
        return false;
    }

    // Have we already checked both bucket and account configs?
    return !(request.rateLimitAccountAlreadyChecked && request.rateLimitBucketAlreadyChecked);
}

/**
 * Extract bucket rate limit configuration from bucket metadata or global rate limit configuration.
 *
 * Resolves in priority order:
 * 1. Per-bucket configuration (from bucket metadata)
 * 2. Global default configuration
 *
 * @param {object} bucket - Bucket metadata object
 * @param {object} log - Logger instance
 * @returns {object} Rate limit config
 */
function extractBucketRateLimitConfig(bucketMD, log) {
    // Try per-bucket config first
    const bucketConfig = bucketMD.getRateLimitConfiguration();
    if (bucketConfig) {
        const data = bucketConfig.getData();

        const merged = {
            RequestsPerSecond: {
                ...config.rateLimiting.bucket.defaultConfig.RequestsPerSecond,
                ...(data.RequestsPerSecond || {}),
                source: data.RequestsPerSecond !== undefined ? 'resource' : 'global',
            },
        };

        log.debug('Extracted per-bucket rate limit config', {
            bucketName: bucketMD.getName(),
            cfg: merged,
        });

        return merged;
    }

    log.debug('Using global default rate limit config', {
        bucketName: bucketMD.getName(),
        cfg: {
            ...config.rateLimiting.bucket.defaultConfig,
            source: 'global',
        },
    });

    return {
        RequestsPerSecond: {
            ...config.rateLimiting.bucket.defaultConfig.RequestsPerSecond,
            source: 'global',
        },
    };
}

/**
 * Extract account rate limit configuration from request or global rate limit configuration.
 *
 * Resolves in priority order:
 * 1. Per-account configuration (from request)
 * 2. Global default configuration
 *
 * @param {object} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - request object given by router
 * @param {object} log - Logger instance
 * @returns {object} Rate limit config
 */
function extractAccountRateLimitConfig(authInfo, request, log) {
    // Try per-account config first
    if (request.accountLimits) {
        const merged = {
            RequestsPerSecond: {
                ...config.rateLimiting.account.defaultConfig.RequestsPerSecond,
                ...(request.accountLimits.RequestsPerSecond || {}),
                source: request.accountLimits.RequestsPerSecond !== undefined ? 'resource' : 'global',
            },
        };

        log.debug('Extracted per-account rate limit config', {
            accountId: authInfo.getCanonicalID(),
            cfg: merged,
        });

        return merged;
    }

    const cfg = {
        RequestsPerSecond: {
            ...config.rateLimiting.account.defaultConfig.RequestsPerSecond,
            source: 'global',
        },
    };

    log.debug('Using global default rate limit config', {
        accountId: authInfo.getCanonicalID(),
        cfg,
    });

    return cfg;
}

function extractRateLimitConfigFromRequest(request, authInfo, bucketMD, log) {
    const limitConfig = {
        bucket: extractBucketRateLimitConfig(bucketMD, log),
        account: extractAccountRateLimitConfig(authInfo, request, log),
    };
    return limitConfig;
}

function getCachedRateLimitConfig(request) {
    const cachedConfig = {};
    const cachedBucketConfig = cache.getCachedConfig(cache.namespace.bucket, request.bucketName);
    if (cachedBucketConfig !== undefined) {
        cachedConfig.bucket = cachedBucketConfig;
    }

    const cachedOwner = cache.getCachedBucketOwner(request.bucketName);
    if (cachedOwner !== undefined) {
        const cachedAccountConfig = cache.getCachedConfig(cache.namespace.account, cachedOwner);
        if (cachedAccountConfig !== undefined) {
            cachedConfig.account = cachedAccountConfig;
            cachedConfig.bucketOwner = cachedOwner;
        }
    }

    return cachedConfig;
}

function buildRateChecksFromConfig(resourceClass, resourceId, limitConfig) {
    const checks = [];
    if (limitConfig?.RequestsPerSecond?.Limit > 0) {
        checks.push({
            resourceClass,
            resourceId,
            measure: 'rps',
            source: limitConfig.RequestsPerSecond.source,
            config: {
                limit: limitConfig.RequestsPerSecond.Limit,
                burstCapacity: limitConfig.RequestsPerSecond.BurstCapacity * 1000,
            },
        });
    }

    return checks;
}

function checkRateLimitsForRequest(checks, log) {
    const buckets = [];
    for (const check of checks) {
        const bucket = getTokenBucket(check.resourceClass, check.resourceId, check.measure, check.config, log);
        if (!bucket.hasCapacity()) {
            log.debug('Rate limit check: denied (no tokens available)', {
                resourceClass: check.resourceClass,
                resourceId: check.resourceId,
                measure: check.measure,
                limit: check.config.limit,
                source: check.source,
            });

            return { allowed: false, rateLimitSource: `${check.resourceClass}:${check.source}` };
        }

        buckets.push(bucket);

        log.trace('Rate limit check: allowed (token consumed)', {
            resourceClass: check.resourceClass,
            resourceId: check.resourceId,
            measure: check.measure,
            source: check.source,
        });
    }

    // buckets have already been checked for available capacity
    // no need to check return values
    buckets.forEach(bucket => bucket.tryConsume());
    return { allowed: true };
}

module.exports = {
    rateLimitApiActions,
    extractBucketRateLimitConfig,
    extractRateLimitConfigFromRequest,
    buildRateChecksFromConfig,
    checkRateLimitsForRequest,
    getCachedRateLimitConfig,
    requestNeedsRateCheck,
};
