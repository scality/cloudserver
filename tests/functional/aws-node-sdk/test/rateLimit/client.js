const assert = require('assert');

const { config } = require('../../../../../lib/Config');
const { RateLimitClient } = require('../../../../../lib/api/apiUtils/rateLimit/client');


const counterKey = 'foo';

describe('Test RateLimitClient', () => {
    let client;

    before(done => {
        client = new RateLimitClient(config.localCache);
        client.redis.connect(done);
    });

    beforeEach(done => {
        client.redis.del(counterKey, err => done(err));
    });

    it('should set the value of an empty counter', done => {
        const batch = [{ key: counterKey, cost: 10000 }];
        client.updateLocalCounters(batch, (err, res) => {
            assert.ifError(err);
            assert.strictEqual(res.length, 1);
            assert.strictEqual(res[0].key, counterKey);
            done();
        });
    });

    it('should increment the value of an existing counter', done => {
        const batch = [{ key: counterKey, cost: 10000 }];
        client.updateLocalCounters(batch, (err, res) => {
            assert.ifError(err);
            const { value: existingValue } = res[0];
            client.updateLocalCounters(batch, (err, res) => {
                assert.ifError(err);
                const { value: newValue } = res[0];
                assert(newValue > existingValue, `${newValue} is not greater than ${existingValue}`);
                done();
            });
        });
    });
});
