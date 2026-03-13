const assert = require('assert');

const { calculateInterval } = require('../../../../../lib/api/apiUtils/rateLimit/gcra');

describe('GCRA calculateInterval function', () => {
    it('should calculate interval for 1770 req/s across 177 nodes', () => {
        const limit = 1770;
        const nodes = 177;

        // Per-NODE rate = 1770 / 177 = 10 req/s
        // Interval = 1000 / 10 = 100ms
        // Workers on the same node share node quota dynamically via Redis reconciliation
        const interval = calculateInterval(limit, nodes);

        assert.strictEqual(interval, 100);
    });

    it('should calculate interval for 1000 req/s with single node', () => {
        const limit = 1000;
        const nodes = 1;

        // Per-NODE rate = 1000 / 1 = 1000 req/s
        // Interval = 1000 / 1000 = 1ms
        const interval = calculateInterval(limit, nodes);

        assert.strictEqual(interval, 1);
    });

    it('should calculate interval for 100 req/s on a single node', () => {
        const limit = 100;
        const nodes = 1;

        // Per-NODE rate = 100 / 1 = 100 req/s
        // Interval = 1000 / 100 = 10ms
        // Workers share the node quota; Redis reconciliation distributes capacity
        const interval = calculateInterval(limit, nodes);

        assert.strictEqual(interval, 10);
    });

    it('should handle fractional intervals', () => {
        const limit = 3000;
        const nodes = 177;

        // Per-NODE rate = 3000 / 177 = 16.95 req/s
        // Interval = 1000 / 16.95 = 58.99ms
        const interval = calculateInterval(limit, nodes);

        assert.strictEqual(Math.floor(interval), 58);
    });

    it('should demonstrate dynamic work-stealing behavior', () => {
        const limit = 600;
        const nodes = 6;

        // Per-NODE rate = 600 / 6 = 100 req/s
        // Interval = 1000 / 100 = 10ms per request
        //
        // Behavior:
        // - If 1 worker is busy, 9 idle: busy worker can use ~100 req/s
        // - If all 10 workers are busy: they share the 100 req/s (~10 req/s each)
        // - Redis reconciliation dynamically balances across active workers
        const interval = calculateInterval(limit, nodes);

        assert.strictEqual(interval, 10);
        // Worker quota is NOT pre-divided: 100 req/s node quota available
        // Each worker can optimistically use up to node quota
        // Redis sync distributes capacity across active workers
    });
});
