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
    calculateInterval,
};
