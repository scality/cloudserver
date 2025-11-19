const assert = require('assert');

const { RateLimitClient } = require('../../../../../lib/api/apiUtils/rateLimit/client');

describe('test RateLimitClient', () => {
    let client;

    before(() => {
        client = new RateLimitClient({});
    });

    it('should instantiate client', () => {
        assert(client);
        assert(client.redis);
    });
});
