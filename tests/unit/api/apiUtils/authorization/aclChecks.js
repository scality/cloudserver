const assert = require('assert');

const { isServiceAccount, getServiceAccountProperties } =
      require('../../../../../lib/api/apiUtils/authorization/permissionChecks');

describe('aclChecks', () => {
    it('should return whether a canonical ID is a service account', () => {
        assert.strictEqual(isServiceAccount('abcdefghijkl'), false);
        assert.strictEqual(isServiceAccount('abcdefghijkl/notaservice'), false);
        assert.strictEqual(isServiceAccount('abcdefghijkl/lifecycle'), true);
        assert.strictEqual(isServiceAccount('abcdefghijkl/md-ingestion'), true);
    });

    it('should return properties of a service account by canonical ID', () => {
        assert.strictEqual(
            getServiceAccountProperties('abcdefghijkl'), undefined);
        assert.strictEqual(
            getServiceAccountProperties('abcdefghijkl/notaservice'), undefined);
        assert.deepStrictEqual(
            getServiceAccountProperties('abcdefghijkl/lifecycle'), {});
        assert.deepStrictEqual(
            getServiceAccountProperties('abcdefghijkl/md-ingestion'), {
                canReplicate: true,
            });
    });
});
