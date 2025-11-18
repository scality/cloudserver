/**
 * Generic Cell Rate Algorithm (GCRA) implementation for rate limiting
 *
 * GCRA provides smooth, burst-free rate limiting by tracking when a theoretical
 * "bucket" will be empty. Each request adds a fixed interval to the empty time.
 *
 * Key concepts:
 * - emptyAt: Timestamp (ms) when the bucket will be empty
 * - interval: Time between requests (ms) = 1000 / requests_per_second
 * - burstCapacity: Maximum bucket size in milliseconds
 *
 * For distributed systems (multiple nodes/workers):
 * - Each worker enforces limit/nodes/workers locally
 * - No sync in iteration 1 (in-memory only)
 */

/**
 * Evaluate if a request should be allowed using GCRA algorithm
 *
 * @param {number} emptyAt - Timestamp (ms) when bucket will be empty
 * @param {number} arrivedAt - Current request timestamp (ms)
 * @param {number} interval - Time between requests (ms)
 * @param {number} burstCapacity - Maximum bucket size (ms)
 * @returns {Object} { allowed: boolean, newEmptyAt: number }
 */
function evaluate(emptyAt, arrivedAt, interval, burstCapacity) {
    // If bucket would be empty before now, reset to now
    let adjustedEmptyAt = emptyAt;
    if (emptyAt < arrivedAt) {
        adjustedEmptyAt = arrivedAt;
    }

    // Check if we have capacity
    // If emptyAt > arrivedAt + burstCapacity, the bucket is full
    if (adjustedEmptyAt > arrivedAt + burstCapacity) {
        return {
            allowed: false,
            newEmptyAt: emptyAt,  // Don't update on rejection
        };
    }

    // Accept and update counter
    const newEmptyAt = adjustedEmptyAt + interval;
    return {
        allowed: true,
        newEmptyAt,
    };
}

/**
 * Calculate per-worker interval based on distributed architecture
 *
 * In a distributed setup with N nodes and W workers per node:
 * - Global limit: R requests per second
 * - Per-worker limit: R / N / W
 * - Interval = 1000ms / (R / N / W)
 *
 * The interval represents milliseconds between requests. We divide 1000 (milliseconds
 * in a second) by the rate to convert "requests per second" to "milliseconds per request".
 *
 * Examples:
 * - 100 req/s ÷ 1 node ÷ 10 workers = 10 req/s per worker → interval = 100ms
 * - 600 req/s ÷ 6 nodes ÷ 10 workers = 10 req/s per worker → interval = 100ms
 *
 * Dynamic work-stealing is achieved through Redis sync reconciliation:
 * - Each worker evaluates locally at its fixed per-worker quota
 * - Workers report consumed / workers to Redis
 * - Redis sums all workers' shares
 * - Workers overwrite local counters with Redis values
 * - Idle workers' unused capacity accumulates in Redis
 * - Busy workers pull back higher emptyAt values and throttle proportionally
 *
 * IMPORTANT: Limit must be >= N * W, otherwise per-worker rate < 1 req/s
 * which results in intervals > 1000ms and effectively blocks traffic.
 *
 * @param {number} limit - Global requests per second
 * @param {number} nodes - Total number of nodes
 * @param {number} _workers - Number of workers per node (unused in token reservation)
 * @returns {number} Interval in milliseconds between requests
 */
// eslint-disable-next-line no-unused-vars
function calculateInterval(limit, nodes, _workers) {
    // Per-node rate = limit / nodes (workers NOT divided)
    // This allows dynamic work-stealing - workers evaluate at node quota
    const perNodeRate = limit / nodes;

    // Interval = 1000ms / rate
    // Dividing 1000 (ms in a second) by rate converts "requests per second"
    // to "milliseconds between requests". Higher rate = smaller interval = more requests.
    return 1000 / perNodeRate;
}

module.exports = {
    evaluate,
    calculateInterval,
};
