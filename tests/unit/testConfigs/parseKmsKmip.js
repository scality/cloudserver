const assert = require('assert');

const { ConfigObject: Config } = require('../../../lib/Config');

function kmipCluster(retries) {
    return {
        providerName: 'thales',
        transport: [{ tls: { host: 'kmip1' } }, { tls: { host: 'kmip2' } }, { tls: { host: 'kmip3' } }],
        retries,
    };
}

describe('parseKmsKmip Function', () => {
    let configInstance;

    beforeEach(() => {
        configInstance = new Config();
    });

    it('should default cluster retries to the number of hosts - 1', () => {
        configInstance._parseKmsKmip({ kmip: kmipCluster(undefined) });
        assert.strictEqual(configInstance.kmip.retries, 2);
    });

    it('should use the configured cluster retries', () => {
        configInstance._parseKmsKmip({ kmip: kmipCluster(1) });
        assert.strictEqual(configInstance.kmip.retries, 1);
    });

    it('should use cluster retries configured to 0', () => {
        configInstance._parseKmsKmip({ kmip: kmipCluster(0) });
        assert.strictEqual(configInstance.kmip.retries, 0);
    });

    it('should reject cluster retries above the number of hosts - 1', () => {
        assert.throws(
            () => configInstance._parseKmsKmip({ kmip: kmipCluster(3) }),
            /KMIP Cluster retries must be lower or equal to the number of hosts - 1/,
        );
    });
});
