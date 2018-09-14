const assert = require('assert');

const getReplicationBackendDataLocator = require(
    '../../../lib/api/apiUtils/object/getReplicationBackendDataLocator');

const locCheckResult = {
    location: 'spoofbackend',
    key: 'spoofkey',
    locationType: 'spoof',
};
const repNoMatch = { backends: [{ site: 'nomatch' }] };
const repMatchPending = { backends:
    [{ site: 'spoofbackend', status: 'PENDING', dataVersionId: '' }] };
const repMatchFailed = { backends:
    [{ site: 'spoofbackend', status: 'FAILED', dataVersionId: '' }] };
const repMatch = { backends: [{
    site: 'spoofbackend',
    status: 'COMPLETED',
    dataStoreVersionId: 'spoofid' }],
};
const expDataLocator = [{
    key: locCheckResult.key,
    dataStoreName: locCheckResult.location,
    dataStoreType: locCheckResult.locationType,
    dataStoreVersionId: repMatch.backends[0].dataStoreVersionId,
}];


describe('Replication Backend Compare', () => {
    it('should return error if no match in replication backends', () => {
        const repBackendResult =
            getReplicationBackendDataLocator(locCheckResult, repNoMatch);
        assert(repBackendResult.error.InvalidLocationConstraint);
    });
    it('should return a status and reason if backend status is PENDING', () => {
        const repBackendResult =
            getReplicationBackendDataLocator(locCheckResult, repMatchPending);
        assert.strictEqual(repBackendResult.dataLocator, undefined);
        assert.strictEqual(repBackendResult.status, 'PENDING');
        assert.notStrictEqual(repBackendResult.reason, undefined);
    });
    it('should return a status and reason if backend status is FAILED', () => {
        const repBackendResult =
            getReplicationBackendDataLocator(locCheckResult, repMatchFailed);
        assert.strictEqual(repBackendResult.dataLocator, undefined);
        assert.strictEqual(repBackendResult.status, 'FAILED');
        assert.notStrictEqual(repBackendResult.reason, undefined);
    });
    it('should return dataLocator obj if backend matches and rep is COMPLETED',
    () => {
        const repBackendResult =
            getReplicationBackendDataLocator(locCheckResult, repMatch);
        assert.strictEqual(repBackendResult.status, 'COMPLETED');
        assert.deepStrictEqual(repBackendResult.dataLocator, expDataLocator);
    });
});
