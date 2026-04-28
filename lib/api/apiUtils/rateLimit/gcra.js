/**
 * Calculate per-node interval based on distributed architecture
 *
 * In a distributed setup with N nodes:
 * - Global limit: R requests per second
 * - Per-node limit: R / N
 * - Interval = 1000ms / (R / N)
 *
 * The interval represents milliseconds between requests. We divide 1000 (milliseconds
 * in a second) by the rate to convert "requests per second" to "milliseconds per request".
 *
 * Examples:
 * - 100 req/s ÷ 1 node = 100 req/s per node → interval = 100ms
 * - 600 req/s ÷ 6 nodes = 100 req/s per node → interval = 100ms
 *
 * Dynamic work-stealing is achieved through Redis sync reconciliation:
 * - Each worker evaluates locally using preallocated tokens
 * - Workers report processed requests to Redis
 * - Redis sums all workers' requests
 * - Idle workers' unused capacity accumulates in Redis
 * - Busy workers pull back higher emptyAt values and throttle proportionally
 *
 * IMPORTANT: Limit must be >= N, otherwise per-node rate < 1 req/s
 * which results in intervals > 1000ms and effectively blocks traffic.
 *
 * @param {number} limit - Global requests per second
 * @param {number} nodes - Total number of nodes
 * @returns {number} Interval in milliseconds between requests
 */

function calculateInterval(limit, nodes) {
    // Per-node rate = limit / nodes
    const perNodeRate = limit / nodes;

    // Interval = 1000ms / rate
    // Dividing 1000 (ms in a second) by rate converts "requests per second"
    // to "milliseconds between requests". Higher rate = smaller interval = more requests.
    return 1000 / perNodeRate;
}

module.exports = {
    calculateInterval,
};
