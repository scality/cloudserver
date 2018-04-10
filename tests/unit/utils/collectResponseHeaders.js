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

    it('should return an undefined value when x-amz-website-redirect-location' +
       ' is empty', () => {
        const objectMD = { 'x-amz-website-redirect-location': '' };
        const headers = collectResponseHeaders(objectMD);
        assert.strictEqual(headers['x-amz-website-redirect-location'],
          undefined);
    });

    it('should return the (nonempty) value of WebsiteRedirectLocation', () => {
        const obj = { 'x-amz-website-redirect-location': 'google.com' };
        const headers = collectResponseHeaders(obj);
        assert.strictEqual(headers['x-amz-website-redirect-location'],
            'google.com');
    });
});
