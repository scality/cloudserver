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
    status: 'COMPLETE',
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
    it('should return error if backend status is PENDING', () => {
        const repBackendResult =
            getReplicationBackendDataLocator(locCheckResult, repMatchPending);
        assert(repBackendResult.error.NoSuchKey);
        assert.strictEqual(repBackendResult.status, 'PENDING');
    });
    it('should return error if backend status is FAILED', () => {
        const repBackendResult =
            getReplicationBackendDataLocator(locCheckResult, repMatchFailed);
        assert(repBackendResult.error.NoSuchKey);
        assert.strictEqual(repBackendResult.status, 'FAILED');
    });
    it('should return dataLocator obj if backend matches and rep is complete',
    () => {
        const repBackendResult =
            getReplicationBackendDataLocator(locCheckResult, repMatch);
        assert.deepStrictEqual(repBackendResult.dataLocator, expDataLocator);
    });
});
