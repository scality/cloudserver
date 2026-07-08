const assert = require('assert');
const collectResponseHeaders = require('../../../lib/utilities/collectResponseHeaders');

describe('Middleware: Collect Response Headers', () => {
    it('should be able to set replication status when config is set', () => {
        const objectMD = { replicationInfo: { status: 'REPLICA' } };
        const headers = collectResponseHeaders(objectMD);
        assert.deepStrictEqual(headers['x-amz-replication-status'], 'REPLICA');
    });

    it('should mark a replica with x-amz-meta-scal-replica', () => {
        const objectMD = {
            replicationInfo: { status: 'REPLICA', isReplica: true },
        };
        const headers = collectResponseHeaders(objectMD);
        assert.deepStrictEqual(headers['x-amz-replication-status'], 'REPLICA');
        assert.deepStrictEqual(headers['x-amz-meta-scal-replica'], 'true');
    });

    it('should default to REPLICA when isReplica is true and status is absent', () => {
        const objectMD = { replicationInfo: { isReplica: true } };
        const headers = collectResponseHeaders(objectMD);
        assert.deepStrictEqual(headers['x-amz-replication-status'], 'REPLICA');
        assert.deepStrictEqual(headers['x-amz-meta-scal-replica'], 'true');
    });

    ['PENDING', 'PROCESSING', 'FAILED'].forEach(status => {
        it(`should expose onward status ${status} on a cascaded replica`, () => {
            const objectMD = { replicationInfo: { status, isReplica: true } };
            const headers = collectResponseHeaders(objectMD);
            assert.deepStrictEqual(headers['x-amz-replication-status'], status);
            assert.deepStrictEqual(headers['x-amz-meta-scal-replica'], 'true');
        });
    });

    it('should report REPLICA once onward replication is COMPLETED', () => {
        const objectMD = {
            replicationInfo: { status: 'COMPLETED', isReplica: true },
        };
        const headers = collectResponseHeaders(objectMD);
        assert.deepStrictEqual(headers['x-amz-replication-status'], 'REPLICA');
        assert.deepStrictEqual(headers['x-amz-meta-scal-replica'], 'true');
    });

    it('should not mark x-amz-meta-scal-replica when not a replica', () => {
        const objectMD = {
            replicationInfo: { status: 'PENDING', isReplica: false },
        };
        const headers = collectResponseHeaders(objectMD);
        assert.deepStrictEqual(headers['x-amz-meta-scal-replica'], undefined);
    });

    it('should use replicationInfo.status when isReplica is false', () => {
        const objectMD = {
            replicationInfo: { status: 'PENDING', isReplica: false },
        };
        const headers = collectResponseHeaders(objectMD);
        assert.deepStrictEqual(headers['x-amz-replication-status'], 'PENDING');
    });

    it('should set the replication status of each site', () => {
        const objectMD = {
            replicationInfo: {
                status: 'COMPLETED',
                backends: [
                    { site: 'us-east-1', status: 'COMPLETED', dataStoreVersionId: '123' },
                    { site: 'us-west-2', status: 'COMPLETED', dataStoreVersionId: '' },
                ],
            },
        };
        const headers = collectResponseHeaders(objectMD);
        assert.deepStrictEqual(headers['x-amz-replication-status'], 'COMPLETED');
        assert.deepStrictEqual(headers['x-amz-meta-us-east-1-replication-status'], 'COMPLETED');
        assert.deepStrictEqual(headers['x-amz-meta-us-east-1-version-id'], '123');
        assert.deepStrictEqual(headers['x-amz-meta-us-west-2-replication-status'], 'COMPLETED');
        assert.deepStrictEqual(headers['x-amz-meta-us-west-2-version-id'], undefined);
    });

    [
        { md: { replicationInfo: null }, test: 'when config is not set' },
        { md: {}, test: 'for older objects' },
    ].forEach(item => {
        it(`should skip replication header ${item.test}`, () => {
            const headers = collectResponseHeaders(item.md);
            assert.deepStrictEqual(headers['x-amz-replication-status'], undefined);
        });
    });

    it('should add the Accept-Ranges header', () => {
        const headers = collectResponseHeaders({});
        assert.strictEqual(headers['Accept-Ranges'], 'bytes');
    });

    it('should return an undefined value when x-amz-website-redirect-location' + ' is empty', () => {
        const objectMD = { 'x-amz-website-redirect-location': '' };
        const headers = collectResponseHeaders(objectMD);
        assert.strictEqual(headers['x-amz-website-redirect-location'], undefined);
    });

    it('should return the (nonempty) value of WebsiteRedirectLocation', () => {
        const obj = { 'x-amz-website-redirect-location': 'google.com' };
        const headers = collectResponseHeaders(obj);
        assert.strictEqual(headers['x-amz-website-redirect-location'], 'google.com');
    });

    it('should not set flag when transition not in progress', () => {
        const obj = {};
        const headers = collectResponseHeaders(obj);
        assert.strictEqual(headers['x-amz-scal-transition-in-progress'], undefined);
        assert.strictEqual(headers['x-amz-meta-scal-s3-transition-in-progress'], undefined);
    });

    it('should set flag when transition in progress', () => {
        const obj = { 'x-amz-scal-transition-in-progress': 'true' };
        const headers = collectResponseHeaders(obj);
        assert.strictEqual(headers['x-amz-scal-transition-in-progress'], undefined);
        assert.strictEqual(headers['x-amz-meta-scal-s3-transition-in-progress'], true);
    });
});
