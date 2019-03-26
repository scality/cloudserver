const assert = require('assert');

const { isHealthy } = require('../../../lib/utilities/healthcheckHandler');

describe('isHealthy function for setHealthCheckResponse', () => {
    test('should return true', () => {
        const returnBool = isHealthy();
        expect(returnBool).toBe(true);
    });
});
