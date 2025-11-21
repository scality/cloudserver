const werelogs = require('werelogs');

const { getAllTokenBuckets, cleanupTokenBuckets } = require('./tokenBucket');

const logger = new werelogs.Logger('S3');

let refillTimer = null;

// Refill interval in milliseconds (how often to check and refill buckets)
const REFILL_INTERVAL_MS = 100;

// Cleanup interval for expired buckets (every 10 seconds)
const CLEANUP_INTERVAL_MS = 10000;
let cleanupCounter = 0;

/**
 * Background refill job for token buckets
 *
 * This job runs periodically (default: every 100ms) and:
 * 1. Iterates through all active token buckets
 * 2. Triggers async refill for buckets below threshold
 * 3. Periodically cleans up expired/idle token buckets
 *
 * The refills are asynchronous and non-blocking, keeping Redis
 * out of the hot request path.
 */
async function refillTokenBuckets() {
    const tokenBuckets = getAllTokenBuckets();

    if (tokenBuckets.size === 0) {
        return {
            checked: 0,
            refilled: 0,
        };
    }

    let checked = 0;
    let refilled = 0;

    // Trigger refill for all active buckets
    const refillPromises = [];

    for (const [bucketName, bucket] of tokenBuckets.entries()) {
        checked++;

        // Trigger async refill if needed (non-blocking)
        const promise = bucket.refillIfNeeded().then(() => {
            // Check if refill actually happened
            if (bucket.refillCount > 0) {
                refilled++;
            }
        }).catch(err => {
            logger.error('Token refill error', {
                bucketName,
                error: err.message,
                stack: err.stack,
            });
        });

        refillPromises.push(promise);
    }

    // Wait for all refills to complete
    await Promise.all(refillPromises);

    return {
        checked,
        refilled,
    };
}

/**
 * Main refill job loop
 * Runs periodically to proactively refill token buckets
 */
async function startRefillJob() {
    logger.info('Starting token refill job', {
        refillIntervalMs: REFILL_INTERVAL_MS,
        cleanupIntervalMs: CLEANUP_INTERVAL_MS,
    });

    const tick = async () => {
        try {
            const stats = await refillTokenBuckets();

            if (stats.refilled > 0) {
                logger.debug('Refill tick completed', stats);
            }

            // Periodic cleanup (every CLEANUP_INTERVAL_MS)
            cleanupCounter++;
            if (cleanupCounter * REFILL_INTERVAL_MS >= CLEANUP_INTERVAL_MS) {
                const removed = cleanupTokenBuckets();
                if (removed > 0) {
                    logger.debug('Cleaned up expired token buckets', {
                        removed,
                    });
                }
                cleanupCounter = 0;
            }
        } catch (err) {
            logger.error('Refill job error', {
                error: err.message,
                stack: err.stack,
            });
        }

        refillTimer = setTimeout(tick, REFILL_INTERVAL_MS);
    };

    // Start timer
    refillTimer = setTimeout(tick, REFILL_INTERVAL_MS);
}

/**
 * Stop the refill job
 * Used during graceful shutdown
 */
function stopRefillJob() {
    if (refillTimer) {
        clearTimeout(refillTimer);
        refillTimer = null;
        logger.info('Stopped token refill job');
    }
}

module.exports = {
    startRefillJob,
    stopRefillJob,
    refillTokenBuckets,
};
