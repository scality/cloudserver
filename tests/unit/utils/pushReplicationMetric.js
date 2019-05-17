const assert = require('assert');
const { ObjectMD } = require('arsenal').models;

const { shouldPushMetric } =
    require('../../../lib/routes/utilities/pushReplicationMetric');

describe('shouldPushMetric', () => {
    it('should push metrics when putting a new replica version', () => {
        const prevObjectMD = new ObjectMD();
        const objectMD = new ObjectMD()
            .setReplicationStatus('REPLICA');
        const result = shouldPushMetric(prevObjectMD, objectMD);
        assert.strictEqual(result, true);
    });

    it('should not push metrics for non-replica operations', () => {
        const prevObjectMD = new ObjectMD();
        const objectMD = new ObjectMD()
            .setReplicationStatus('COMPLETED');
        const result = shouldPushMetric(prevObjectMD, objectMD);
        assert.strictEqual(result, false);
    });

    it('should not push metrics for replica operations with tagging', () => {
        const prevObjectMD = new ObjectMD();
        const objectMD = new ObjectMD()
            .setReplicationStatus('REPLICA')
            .setTags({ 'object-tag-key': 'object-tag-value' });
        const result = shouldPushMetric(prevObjectMD, objectMD);
        assert.strictEqual(result, false);
    });

    it('should not push metrics for replica operations when deleting tagging',
    () => {
        const prevObjectMD = new ObjectMD()
            .setTags({ 'object-tag-key': 'object-tag-value' });
        const objectMD = new ObjectMD().setReplicationStatus('REPLICA');
        const result = shouldPushMetric(prevObjectMD, objectMD);
        assert.strictEqual(result, false);
    });

    it('should not push metrics for replica operations with acl', () => {
        const prevObjectMD = new ObjectMD();
        const objectMD = new ObjectMD();
        const publicACL = objectMD.getAcl();
        publicACL.Canned = 'public-read';
        objectMD
            .setReplicationStatus('REPLICA')
            .setAcl(publicACL);
        const result = shouldPushMetric(prevObjectMD, objectMD);
        assert.strictEqual(result, false);
    });

    it('should not push metrics for replica operations when resetting acl',
    () => {
        const prevObjectMD = new ObjectMD();
        const publicACL = prevObjectMD.getAcl();
        publicACL.Canned = 'public-read';
        prevObjectMD
            .setReplicationStatus('REPLICA')
            .setAcl(publicACL);
        const objectMD = new ObjectMD();
        const privateACL = objectMD.getAcl();
        privateACL.Canned = 'private';
        objectMD
            .setReplicationStatus('REPLICA')
            .setAcl(privateACL);
        const result = shouldPushMetric(prevObjectMD, objectMD);
        assert.strictEqual(result, false);
    });
});
