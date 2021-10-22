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
        assert.strictEqual(c.locationTypeHyperdriveV2, true);
        assert.strictEqual(c.locationTypeLocal, true);
        assert.strictEqual(c.preferredReadLocation, true);
        assert.strictEqual(c.managedLifecycle, true);
        assert.strictEqual(c.secureChannelOptimizedPath, true);
        assert.strictEqual(c.s3cIngestLocation, true);
    });
    it('should allow configure local file system capability', () => {
        const OLD_ENV = process.env;

        process.env.LOCAL_VOLUME_CAPABILITY = 'true';
        assert.strictEqual(getCapabilities().locationTypeLocal, true);

        process.env.LOCAL_VOLUME_CAPABILITY = 'false';
        assert.strictEqual(getCapabilities().locationTypeLocal, false);

        process.env = OLD_ENV;
    });
});
