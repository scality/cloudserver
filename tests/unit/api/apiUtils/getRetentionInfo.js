const assert = require('assert');

const BucketInfo = require('arsenal').models.BucketInfo;
const getRetentionInfo =
      require('../../../../lib/api/apiUtils/object/getRetentionInfo');

function _getRetentionInfo(objectLockConfig) {
    const bucketInfo = new BucketInfo(
        'testbucket', 'someCanonicalId', 'accountDisplayName',
        new Date().toJSON(),
        null, null, null, null, null, null, null, null, null,
        null, true, objectLockConfig);
    return getRetentionInfo(bucketInfo);
}

describe.only('getRetentionInfo helper', () => {
    it('should get retention info', () => {
        const objectLockConfig = {
            rule: {
                mode: 'COMPLIANCE',
                days: 1,
            },
        };
        const retentionInfo = _getRetentionInfo(objectLockConfig);
        const date = new Date();
        assert.deepStrictEqual(retentionInfo, {
            mode: 'COMPLIANCE',
            retainUntilDate: date.getDate() + 1,
        });
    });

    it('should not get retention info when no objectno object lock ' +
    'configuration is set', () => {
        const retentionInfo = _getRetentionInfo({});
        assert.deepStrictEqual(retentionInfo, undefined);
    });
});
