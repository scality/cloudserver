const assert = require('assert');

const { calculateInterval } = require('../../../../../lib/api/apiUtils/rateLimit/gcra');

describe('GCRA calculateInterval function', () => {
    it('should calculate interval for 1770 req/s across 177 nodes (ignoring workers)', () => {
        const limit = 1770;
        const nodes = 177;
        const workers = 10; // Ignored in new implementation

        // Per-NODE rate = 1770 / 177 = 10 req/s (workers NOT divided)
        // Interval = 1000 / 10 = 100ms
        // This allows dynamic work-stealing: busy workers can use full node quota
        const interval = calculateInterval(limit, nodes, workers);

        assert.strictEqual(interval, 100);
    });

    it('should calculate interval for 1000 req/s with single node', () => {
        const limit = 1000;
        const nodes = 1;
        const workers = 1; // Ignored

        // Per-NODE rate = 1000 / 1 = 1000 req/s (workers NOT divided)
        // Interval = 1000 / 1000 = 1ms
        const interval = calculateInterval(limit, nodes, workers);

        assert.strictEqual(interval, 1);
    });

    it('should calculate interval for 100 req/s with 10 workers on single node', () => {
        const limit = 100;
        const nodes = 1;
        const workers = 10; // Ignored in new implementation

        // Per-NODE rate = 100 / 1 = 100 req/s (workers NOT divided)
        // Interval = 1000 / 100 = 10ms
        // Each worker evaluates at node quota, Redis reconciliation shares capacity
        const interval = calculateInterval(limit, nodes, workers);

        assert.strictEqual(interval, 10);
    });

    it('should handle fractional intervals', () => {
        const limit = 3000;
        const nodes = 177;
        const workers = 10; // Ignored

        // Per-NODE rate = 3000 / 177 = 16.95 req/s (workers NOT divided)
        // Interval = 1000 / 16.95 = 58.99ms
        const interval = calculateInterval(limit, nodes, workers);

        assert.strictEqual(Math.floor(interval), 58);
    });

    it('should demonstrate dynamic work-stealing behavior', () => {
        const limit = 600;
        const nodes = 6;
        const workers = 10;

        // Per-NODE rate = 600 / 6 = 100 req/s
        // Interval = 1000 / 100 = 10ms per request
        //
        // Behavior:
        // - If 1 worker is busy, 9 idle: busy worker can use ~100 req/s
        // - If all 10 workers are busy: they share the 100 req/s (~10 req/s each)
        // - Redis reconciliation dynamically balances across active workers
        const interval = calculateInterval(limit, nodes, workers);

        assert.strictEqual(interval, 10);
        // Worker quota is NOT pre-divided: 100 req/s node quota available
        // Each worker can optimistically use up to node quota
        // Redis sync distributes capacity across active workers
    });
});
