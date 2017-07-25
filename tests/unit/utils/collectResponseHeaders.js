const assert = require('assert');
const collectResponseHeaders =
    require('../../../lib/utilities/collectResponseHeaders');

describe('Middleware: Collect Response Headers', () => {
    it('should be able to set replication status when config is set', () => {
        const objectMD = { replicationInfo: { status: 'REPLICA' } };
        const headers = collectResponseHeaders(objectMD);
        assert.deepStrictEqual(headers['x-amz-replication-status'], 'REPLICA');
    });

    [
        { md: { replicationInfo: null }, test: 'when config is not set' },
        { md: {}, test: 'for older objects' },
    ].forEach(item => {
        it(`should skip replication header ${item.test}`, () => {
            const headers = collectResponseHeaders(item.md);
            assert.deepStrictEqual(headers['x-amz-replication-status'],
                undefined);
        });
    });
});
