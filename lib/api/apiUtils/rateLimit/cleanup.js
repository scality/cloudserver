const { expireCachedConfigs, expireRequestTimestamps } = require('./cache');
const { rateLimitCleanupInterval } = require('../../../../constants');

let cleanupInterval = null;

/**
 * Start periodic cleanup of expired rate limit counters and cached configs
 *
 * Runs every 10 seconds to:
 * - Remove expired GCRA counters (where emptyAt <= now)
 * - Remove expired cached rate limit configs (where TTL expired)
 *
 * This prevents memory leaks from accumulating expired entries.
 */
function startCleanupJob(log, options = {}) {
    if (cleanupInterval) {
        log.warn('Rate limit cleanup job already running');
        return;
    }

    log.info('Starting rate limit cleanup job', {
        interval: rateLimitCleanupInterval,
    });

    cleanupInterval = setInterval(() => {
        const now = Date.now();
        const expiredConfigs = expireCachedConfigs(now);
        expireRequestTimestamps(now);

        if (expiredConfigs > 0) {
            log.debug('Rate limit cleanup completed', { expiredConfigs });
        }
    }, rateLimitCleanupInterval);

    // Prevent cleanup job from keeping process alive
    // Skip unref() in test environment to work with sinon fake timers
    if (!options.skipUnref) {
        cleanupInterval.unref();
    }
}

/**
 * Stop the periodic cleanup job
 * Used for graceful shutdown or testing
 */
function stopCleanupJob(log) {
    if (cleanupInterval) {
        clearInterval(cleanupInterval);
        cleanupInterval = null;
        if (log) {
            log.info('Stopped rate limit cleanup job');
        }
    }
}

module.exports = {
    startCleanupJob,
    stopCleanupJob,
};
