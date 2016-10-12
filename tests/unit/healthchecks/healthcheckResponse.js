import assert from 'assert';

import { isHealthy } from '../../../lib/routes';

describe('isHealthy function for setHealthCheckResponse', () => {
    it('should return true', () => {
        const returnBool = isHealthy();
        assert.strictEqual(returnBool, true);
    });
});
