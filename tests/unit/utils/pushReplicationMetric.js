const assert = require('assert');
const { ObjectMD } = require('arsenal').models;

const { shouldPushMetric } =
    require('../../../lib/routes/utilities/pushReplicationMetric');

describe('shouldPushMetric', () => {
    it('should push metrics when putting a new replica version', () => {
        const prevObjectMD = undefined;
        const objectMD = new ObjectMD()
            .setReplicationStatus('REPLICA');
        const result = shouldPushMetric(prevObjectMD, objectMD);
        assert.strictEqual(result, true);
    });

    it('should push metrics when putting a new replica version with public ACL',
    () => {
        const prevObjectMD = undefined;
        const objectMD = new ObjectMD()
              .setReplicationStatus('REPLICA');
        const publicACL = objectMD.getAcl();
        publicACL.Canned = 'public-read';
        objectMD.setAcl(publicACL);
        const result = shouldPushMetric(prevObjectMD, objectMD);
        assert.strictEqual(result, true);
    });

    it('should not push metrics for non-replica operations', () => {
        const prevObjectMD = undefined;
        const objectMD = new ObjectMD()
            .setReplicationStatus('COMPLETED');
        const result = shouldPushMetric(prevObjectMD, objectMD);
        assert.strictEqual(result, false);
    });

    it('should not push metrics when updating replica in-place (ACL/Tagging)',
    () => {
        const prevObjectMD = new ObjectMD();
        const publicACL = prevObjectMD.getAcl();
        publicACL.Canned = 'public-read';
        const objectMD = new ObjectMD()
            .setReplicationStatus('REPLICA')
            .setTags({ 'object-tag-key': 'object-tag-value' })
            .setAcl(publicACL);
        const result = shouldPushMetric(prevObjectMD, objectMD);
        assert.strictEqual(result, false);
    });
});
