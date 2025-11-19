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
<<<<<<< HEAD
=======

describe('GCRA integration scenarios', () => {
    it('should handle burst capacity allowing multiple requests', () => {
        const limit = 10;
        const nodes = 1;
        const workers = 10;
        const bucketSize = 2000; // 2 second burst

        // With new implementation: interval is based on NODE quota (10 req/s), not worker quota
        const interval = calculateInterval(limit, nodes, workers);
        assert.strictEqual(interval, 100); // 10 req/s per node (NOT divided by workers)

        let emptyAt = 0;
        const baseTime = 5000;

        // First request: emptyAt becomes 5000 + 100 = 5100
        let result = evaluate(emptyAt, baseTime, interval, bucketSize);
        assert.strictEqual(result.allowed, true);
        emptyAt = result.newEmptyAt;
        assert.strictEqual(emptyAt, 5100);

        // Second request: emptyAt becomes 5100 + 100 = 5200
        // Check: 5100 > 5001 + 2000 (7001)? No, allowed
        result = evaluate(emptyAt, baseTime + 1, interval, bucketSize);
        assert.strictEqual(result.allowed, true);
        emptyAt = result.newEmptyAt;
        assert.strictEqual(emptyAt, 5200);

        // Many more requests can succeed because interval is 100ms (not 1000ms)
        // With 2000ms burst capacity, we can have ~20 requests in quick succession
        for (let i = 0; i < 18; i++) {
            result = evaluate(emptyAt, baseTime + 2 + i, interval, bucketSize);
            assert.strictEqual(result.allowed, true, `Request ${i + 3} should be allowed`);
            emptyAt = result.newEmptyAt;
        }

        // After 20 requests: emptyAt ~= 5000 + (20 * 100) = 7000
        // With burst capacity of 2000ms, requests arriving up to 7000 - 2000 = 5000 + 2000 = 7000 are allowed
        // So we need to send more requests to exceed the burst capacity
        // Let's send requests without time passing to fill the bucket
        for (let i = 0; i < 5; i++) {
            result = evaluate(emptyAt, baseTime + 20, interval, bucketSize);
            if (!result.allowed) {
                break; // Found the limit
            }
            emptyAt = result.newEmptyAt;
        }

        // Now emptyAt should be > arrivedAt + bucketSize, so next request denied
        result = evaluate(emptyAt, baseTime + 20, interval, bucketSize);
        assert.strictEqual(result.allowed, false, 'Should eventually deny when burst capacity exceeded');
    });

});
>>>>>>> 32964604d (CLDSRV-783: Rename burstCapacity to bucketSize in GCRA)
