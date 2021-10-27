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

    [
        { value: 'true',    result: true  },
        { value: 'TRUE',    result: true  },
        { value: 'tRuE',    result: true  },
        { value: '1',       result: true  },
        { value: 'false',   result: false },
        { value: 'FALSE',   result: false },
        { value: 'FaLsE',   result: false },
        { value: '0',       result: false },
        { value: 'foo',     result: false },
        { value: '',        result: true },
        { value: undefined, result: true },
    ].forEach(param =>
        it(`should allow set local file system capability ${param.value}`, () => {
            const OLD_ENV = process.env;

            if (param.value !== undefined) process.env.LOCAL_VOLUME_CAPABILITY = param.value;
            assert.strictEqual(getCapabilities().locationTypeLocal, param.result);

            process.env = OLD_ENV;
        })
    );
});
