const { getAllTokenBuckets, cleanupTokenBuckets } = require('./tokenBucket');

let refillTimer = null;
let refillJobRunning = false;

// Refill interval in milliseconds (how often to check and refill buckets)
const REFILL_INTERVAL_MS = process.env.REFILL_INTERVAL_MS
    ? parseInt(process.env.REFILL_INTERVAL_MS, 10)
    : 100;

// Cleanup interval for expired buckets (every 10 seconds)
const CLEANUP_INTERVAL_MS = process.env.CLEANUP_INTERVAL_MS
    ? parseInt(process.env.CLEANUP_INTERVAL_MS, 10)
    : 10000;

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
async function refillTokenBuckets(logger) {
    const tokenBuckets = getAllTokenBuckets();

    let checked = 0;
    let refilled = 0;

    // Trigger refill for all active buckets
    const refillPromises = [];

    for (const [bucketName, bucket] of tokenBuckets.entries()) {
        checked++;

        // Trigger async refill if needed (non-blocking)
        const promise = bucket.refillIfNeeded().then(bucketRefilled => {
            // Check if refill actually happened
            if (bucketRefilled) {
                refilled++;
            }
        }).catch(err => {
            logger.error('error refilling token bucket', {
                bucketName,
                method: 'rateLimit.refillTokenBuckets',
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
function startRefillJob(logger, options = {}) {
    if (refillJobRunning) {
        logger.warn('token refill job already running');
        return;
    }

    refillJobRunning = true;
    logger.info('starting token refill job', {
        refillIntervalMs: REFILL_INTERVAL_MS,
        cleanupIntervalMs: CLEANUP_INTERVAL_MS,
    });

    const arm = () => {
        refillTimer = setTimeout(tick, REFILL_INTERVAL_MS);
        // Prevent the refill job from keeping the process alive, as the
        // cleanup job already does. Skipped in tests using fake timers.
        if (!options.skipUnref) {
            refillTimer.unref();
        }
    };

    const tick = async () => {
        try {
            const stats = await refillTokenBuckets(logger);

            if (stats.refilled > 0) {
                logger.trace('refill tick completed', stats);
            }

            // Periodic cleanup (every CLEANUP_INTERVAL_MS)
            cleanupCounter++;
            if (cleanupCounter * REFILL_INTERVAL_MS >= CLEANUP_INTERVAL_MS) {
                const removed = cleanupTokenBuckets();
                if (removed > 0) {
                    logger.debug('cleaned up expired token buckets', {
                        removed,
                    });
                }
                cleanupCounter = 0;
            }
        } catch (err) {
            logger.error('refill job error', {
                method: 'rateLimit.refillJob',
                error: err.message,
                stack: err.stack,
            });
        }

        // A tick that was awaiting when stopRefillJob() ran must not re-arm:
        // it would replace the timer that stop just cleared and the job would
        // carry on for the rest of the process lifetime.
        if (!refillJobRunning) {
            return;
        }

        arm();
    };

    // Start timer
    arm();
}

/**
 * Stop the refill job
 * Used during graceful shutdown
 */
function stopRefillJob(logger) {
    const wasRunning = refillJobRunning;
    refillJobRunning = false;

    if (refillTimer) {
        clearTimeout(refillTimer);
        refillTimer = null;
    }

    if (wasRunning) {
        logger.info('stopped token refill job');
    }
}

module.exports = {
    startRefillJob,
    stopRefillJob,
    refillTokenBuckets,
};
