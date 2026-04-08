const { expireCachedConfigs, expireCachedBucketOwners } = require('./cache');
const { rateLimitCleanupInterval } = require('../../../../constants');

let cleanupTimer = null;

function cleanupJob(log, options = {}) {
    const expiredConfigs = expireCachedConfigs();
    const expiredBucketOwners = expireCachedBucketOwners();
    if (expiredConfigs > 0 || expiredBucketOwners > 0) {
        log.debug('Rate limit cleanup completed', { expiredConfigs, expiredBucketOwners });
    }

    cleanupTimer = setTimeout(() => cleanupJob(log, options), rateLimitCleanupInterval);
    if (!options.skipUnref) {
        cleanupTimer.unref();
    }
}


/**
 * Start periodic cleanup of expired rate limit counters and cached configs
 *
 * Runs every 10 seconds to:
 * - Remove expired GCRA counters (where emptyAt <= now)
 * - Remove expired cached rate limit configs (where TTL expired)
 * - Remove expired cached bucket owners (where TTL expired)
 *
 * This prevents memory leaks from accumulating expired entries.
 */
function startCleanupJob(log, options = {}) {
    if (cleanupTimer) {
        log.warn('Rate limit cleanup job already running');
        return;
    }

    log.info('Starting rate limit cleanup job', {
        interval: rateLimitCleanupInterval,
    });

    cleanupTimer = setTimeout(() => cleanupJob(log, options), rateLimitCleanupInterval);

    // Prevent cleanup job from keeping process alive
    // Skip unref() in test environment to work with sinon fake timers
    if (!options.skipUnref) {
        cleanupTimer.unref();
    }
}

/**
 * Stop the periodic cleanup job
 * Used for graceful shutdown or testing
 */
function stopCleanupJob(log) {
    if (cleanupTimer !== null) {
        clearTimeout(cleanupTimer);
        cleanupTimer = null;
        if (log) {
            log.info('Stopped rate limit cleanup job');
        }
    }
}

module.exports = {
    startCleanupJob,
    stopCleanupJob,
};
