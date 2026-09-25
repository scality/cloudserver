const assert = require('assert');
const { RateLimitClient } = require('../../../../../lib/api/apiUtils/rateLimit/client');
const { config } = require('../../../../../lib/Config');
const { skipIfRateLimitDisabled } = require('./tooling');

const testBucket = 'rate-limit-test-bucket';

skipIfRateLimitDisabled('RateLimitClient', () => {
    let client;

    before(async () => {
        // Create a test client using the same config as the application
        client = new RateLimitClient(config.localCache);

        // Connect the client (since lazyConnect is true)
        await client.redis.connect();
    });

    after(async () => client.redis.quit().catch(() => {}));

    beforeEach(async () => {
        const keys = await client.redis.keys('ratelimit:*');
        if (keys.length > 0) {
            await client.redis.del(...keys);
        }
    });

    describe('isReady', () => {
        it('should return true when client is connected to redis', () => {
            assert.strictEqual(client.isReady(), true);
        });

        it('should return true when the client is waiting to connect or the first time', () => {
            const client = new RateLimitClient(config.localCache);
            assert.strictEqual(client.isReady(), true);
        });
    });

    describe('grantTokens', () => {
        it('should grant requested tokens when quota is available', done => {
            const requested = 5;
            const interval = 100; // 100ms per request = 10 req/s
            const burstCapacity = 1000; // 1000ms burst capacity

            client.grantTokens('bucket', testBucket, 'rps', requested, interval, burstCapacity, (err, granted) => {
                assert.ifError(err);
                assert.strictEqual(granted, requested);
                done();
            });
        });

        it('should grant tokens multiple times within burst capacity', done => {
            const requested = 2;
            const interval = 100; // 100ms per request
            const burstCapacity = 1000; // 1000ms burst capacity

            // First request
            client.grantTokens('bucket', testBucket, 'rps', requested, interval, burstCapacity, (err, granted1) => {
                assert.ifError(err);
                assert.strictEqual(granted1, requested);

                // Second request immediately after
                client.grantTokens('bucket', testBucket, 'rps', requested, interval, burstCapacity, (err, granted2) => {
                    assert.ifError(err);
                    assert.strictEqual(granted2, requested);
                    done();
                });
            });
        });

        it('should grant partial tokens when request exceeds available capacity', done => {
            const interval = 100; // 100ms per request
            const burstCapacity = 500; // 500ms burst capacity = max 5 tokens

            // Request more tokens than available in burst
            client.grantTokens('bucket', testBucket, 'rps', 10, interval, burstCapacity, (err, granted) => {
                assert.ifError(err);
                // Should grant partial tokens (5 tokens max with 500ms burst)
                assert(granted > 0, 'Should grant at least some tokens');
                assert(granted <= 5, 'Should not grant more than burst capacity allows');
                done();
            });
        });

        it('should deny tokens (return 0) when quota is exhausted', done => {
            const interval = 100; // 100ms per request
            const burstCapacity = 100; // 100ms burst capacity = max 1 token

            // First request consumes the burst capacity
            client.grantTokens('bucket', testBucket, 'rps', 1, interval, burstCapacity, (err, granted1) => {
                assert.ifError(err);
                assert.strictEqual(granted1, 1);

                // Second request immediately after should be denied
                client.grantTokens('bucket', testBucket, 'rps', 1, interval, burstCapacity, (err, granted2) => {
                    assert.ifError(err);
                    assert.strictEqual(granted2, 0, 'Should deny tokens when quota exhausted');
                    done();
                });
            });
        });
    });
});
