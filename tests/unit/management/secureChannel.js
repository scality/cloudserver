const assert = require('assert');

const { getCapabilities } = require('../../../lib/utilities/reportHandler');

// Ensures that expected features are enabled even if they
// rely on optional dependencies (such as secureChannelOptimizedPath)
describe('report handler', () => {
    it('should report current capabilities', () => {
        const c = getCapabilities();
        assert.strictEqual(c.locationTypeDigitalOcean, true);
        assert.strictEqual(c.locationTypeS3Custom, true);
        assert.strictEqual(c.locationTypeSproxyd, true);
        assert.strictEqual(c.preferredReadLocation, true);
        assert.strictEqual(c.managedLifecycle, true);
        assert.strictEqual(c.secureChannelOptimizedPath, true);
    });
});
