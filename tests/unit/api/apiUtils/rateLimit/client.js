const assert = require('assert');

const { RateLimitClient } = require('../../../../../lib/api/apiUtils/rateLimit/client');

class RedisStub {
    constructor() {
        this.data = {};
        this.execErr = null;
    }

    pipeline() {
        return new PipelineStub(this.execErr);
    }

    setExecErr(err) {
        this.execErr = err;
    }
}

class PipelineStub {
    constructor(execErr) {
        this.ops = [];
        this.execErr = execErr;
    }

    updateCounter(key, cost) {
        this.ops.push([key, cost]);
    }

    exec(cb) {
        if (this.execErr) {
            cb(this.execErr);
        } else {
            cb(null, this.ops.map(v => [1, v[1]]));
        }
    }
}

describe('test RateLimitClient', () => {
    let client;

    before(() => {
        client = new RateLimitClient({});
    });

    beforeEach(() => {
        client.redis = new RedisStub();
    });

    it('should update a batch of counters', done => {
        const batch = [
            { key: 'foo', cost: 100 },
            { key: 'bar', cost: 200 },
            { key: 'qux', cost: 300 },
        ];

        client.updateLocalCounters(batch, (err, results) => {
            assert.ifError(err);
            assert.deepStrictEqual(results, [
                { key: 'foo', value: 100 },
                { key: 'bar', value: 200 },
                { key: 'qux', value: 300 },
            ]);
            done();
        });
    });

    it('should pass through errors', done => {
        const execErr = new Error('bad stuff');
        client.redis.setExecErr(execErr);
        const batch = [
            { key: 'foo', cost: 100 },
            { key: 'bar', cost: 200 },
            { key: 'qux', cost: 300 },
        ];

        client.updateLocalCounters(batch, (err, results) => {
            assert.strictEqual(err, execErr);
            assert.strictEqual(results, undefined);
            done();
        });
    });
});
