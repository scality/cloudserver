const assert = require('assert');

const {
    evaluate,
    calculateInterval,
} = require('../../../../../lib/api/apiUtils/rateLimit/gcra');

describe('GCRA evaluate function', () => {
    it('should allow request when bucket is empty', () => {
        const emptyAt = 1000;
        const arrivedAt = 2000; // After bucket is empty
        const interval = 100;
        const burstCapacity = 500;

        const result = evaluate(emptyAt, arrivedAt, interval, burstCapacity);

        assert.strictEqual(result.allowed, true);
        assert.strictEqual(result.newEmptyAt, arrivedAt + interval);
    });

    it('should allow request with available capacity', () => {
        const emptyAt = 2000;
        const arrivedAt = 1500;
        const interval = 100;
        const burstCapacity = 1000;

        const result = evaluate(emptyAt, arrivedAt, interval, burstCapacity);

        assert.strictEqual(result.allowed, true);
        assert.strictEqual(result.newEmptyAt, emptyAt + interval);
    });

    it('should deny request when bucket is full', () => {
        const arrivedAt = 1000;
        const burstCapacity = 500;
        const emptyAt = arrivedAt + burstCapacity + 100; // Over capacity
        const interval = 100;

        const result = evaluate(emptyAt, arrivedAt, interval, burstCapacity);

        assert.strictEqual(result.allowed, false);
        assert.strictEqual(result.newEmptyAt, emptyAt); // Unchanged on rejection
    });

    it('should allow request at exact burst capacity threshold', () => {
        const arrivedAt = 1000;
        const burstCapacity = 500;
        const emptyAt = arrivedAt + burstCapacity; // Exactly at threshold
        const interval = 100;

        const result = evaluate(emptyAt, arrivedAt, interval, burstCapacity);

        assert.strictEqual(result.allowed, true);
        assert.strictEqual(result.newEmptyAt, emptyAt + interval);
    });

    it('should handle zero burst capacity', () => {
        const emptyAt = 1000;
        const arrivedAt = 1000;
        const interval = 100;
        const burstCapacity = 0;

        const result = evaluate(emptyAt, arrivedAt, interval, burstCapacity);

        assert.strictEqual(result.allowed, true);
        assert.strictEqual(result.newEmptyAt, emptyAt + interval);
    });

    it('should deny when one interval ahead with zero burst capacity', () => {
        const arrivedAt = 1000;
        const emptyAt = 1100;
        const interval = 100;
        const burstCapacity = 0;

        const result = evaluate(emptyAt, arrivedAt, interval, burstCapacity);

        assert.strictEqual(result.allowed, false);
        assert.strictEqual(result.newEmptyAt, emptyAt);
    });
});

describe('GCRA calculateInterval function', () => {
    it('should calculate interval for 1770 req/s across 177 nodes with 10 workers', () => {
        const limit = 1770;
        const nodes = 177;
        const workers = 10;

        // Per-worker rate = 1770 / 177 / 10 = 1 req/s
        // Interval = 1000 / 1 = 1000ms
        const interval = calculateInterval(limit, nodes, workers);

        assert.strictEqual(interval, 1000);
    });

    it('should calculate interval for 1000 req/s with single node and worker', () => {
        const limit = 1000;
        const nodes = 1;
        const workers = 1;

        // Per-worker rate = 1000 / 1 / 1 = 1000 req/s
        // Interval = 1000 / 1000 = 1ms
        const interval = calculateInterval(limit, nodes, workers);

        assert.strictEqual(interval, 1);
    });

    it('should calculate interval for 100 req/s with 10 workers on single node', () => {
        const limit = 100;
        const nodes = 1;
        const workers = 10;

        // Per-worker rate = 100 / 1 / 10 = 10 req/s
        // Interval = 1000 / 10 = 100ms
        const interval = calculateInterval(limit, nodes, workers);

        assert.strictEqual(interval, 100);
    });

    it('should handle fractional intervals', () => {
        const limit = 3000;
        const nodes = 177;
        const workers = 10;

        // Per-worker rate = 3000 / 177 / 10 = 1.69 req/s
        // Interval = 1000 / 1.69 = 590.39ms
        const interval = calculateInterval(limit, nodes, workers);

        assert.strictEqual(Math.floor(interval), 590);
    });
});

describe('GCRA integration scenarios', () => {
    it('should handle burst capacity allowing multiple requests', () => {
        const limit = 10;
        const nodes = 1;
        const workers = 10;
        const burstCapacity = 2000; // 2 second burst

        const interval = calculateInterval(limit, nodes, workers);
        assert.strictEqual(interval, 1000); // 1 req/s per worker

        let emptyAt = 0;
        const baseTime = 5000;

        // First request: emptyAt becomes 5000 + 1000 = 6000
        let result = evaluate(emptyAt, baseTime, interval, burstCapacity);
        assert.strictEqual(result.allowed, true);
        emptyAt = result.newEmptyAt;

        // Second request: emptyAt becomes 6000 + 1000 = 7000
        // Check: 6000 > 5001 + 2000 (7001)? No, allowed
        result = evaluate(emptyAt, baseTime + 1, interval, burstCapacity);
        assert.strictEqual(result.allowed, true);
        emptyAt = result.newEmptyAt;

        // Third request: would make emptyAt = 8000
        // Check: 7000 > 5002 + 2000 (7002)? No, allowed
        result = evaluate(emptyAt, baseTime + 2, interval, burstCapacity);
        assert.strictEqual(result.allowed, true);
        emptyAt = result.newEmptyAt;

        // Fourth request: would make emptyAt = 9000
        // Check: 8000 > 5003 + 2000 (7003)? Yes, denied (exceeds burst)
        result = evaluate(emptyAt, baseTime + 3, interval, burstCapacity);
        assert.strictEqual(result.allowed, false);
    });

});
