const assert = require('assert');

const { getCapabilities } = require('../../../lib/utilities/reportHandler');

// Ensures that expected features are enabled even if they
// rely on optional dependencies (such as secureChannelOptimizedPath)
describe('report handler', () => {
    test('should report current capabilities', () => {
        const c = getCapabilities();
        expect(c.locationTypeDigitalOcean).toBe(true);
        expect(c.locationTypeS3Custom).toBe(true);
        expect(c.locationTypeSproxyd).toBe(true);
        expect(c.preferredReadLocation).toBe(true);
        expect(c.managedLifecycle).toBe(true);
        expect(c.secureChannelOptimizedPath).toBe(true);
        expect(c.s3cIngestLocation).toBe(true);
    });
});
