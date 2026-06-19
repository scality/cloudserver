const assert = require('assert');
const { ObjectMD } = require('arsenal').models;

const { getMetricToPush } = require('../../../lib/routes/utilities/pushReplicationMetric');

describe('getMetricToPush', () => {
    it('should push metrics when putting a new replica version', () => {
        const prevObjectMD = new ObjectMD().setVersionId('1');
        const objectMD = new ObjectMD().setVersionId('2').setReplicationIsReplica(true);
        const result = getMetricToPush(prevObjectMD, objectMD);
        assert.strictEqual(result, 'replicateObject');
    });

    it('should not push metrics for non-replica operations', () => {
        const prevObjectMD = new ObjectMD();
        const objectMD = new ObjectMD().setReplicationStatus('COMPLETED');
        const result = getMetricToPush(prevObjectMD, objectMD);
        assert.strictEqual(result, null);
    });

    it('should push metrics for replica operations with tagging', () => {
        const prevObjectMD = new ObjectMD().setVersionId('1');
        const objectMD = new ObjectMD()
            .setVersionId('1')
            .setReplicationIsReplica(true)
            .setTags({ 'object-tag-key': 'object-tag-value' });
        const result = getMetricToPush(prevObjectMD, objectMD);
        assert.strictEqual(result, 'replicateTags');
    });

    it('should push metrics for replica operations when deleting tagging', () => {
        const prevObjectMD = new ObjectMD().setTags({ 'object-tag-key': 'object-tag-value' });
        const objectMD = new ObjectMD().setReplicationIsReplica(true);
        const result = getMetricToPush(prevObjectMD, objectMD);
        assert.strictEqual(result, 'replicateTags');
    });

    it('should not push metrics for replica operations with tagging ' + 'if tags are equal', () => {
        const prevObjectMD = new ObjectMD().setVersionId('1').setTags({ 'object-tag-key': 'object-tag-value' });
        const objectMD = new ObjectMD()
            .setVersionId('1')
            .setReplicationIsReplica(true)
            .setTags({ 'object-tag-key': 'object-tag-value' });
        const result = getMetricToPush(prevObjectMD, objectMD);
        assert.strictEqual(result, null);
    });

    it('should push metrics for replica operations with acl', () => {
        const prevObjectMD = new ObjectMD().setVersionId('1');
        const objectMD = new ObjectMD();
        const publicACL = objectMD.getAcl();
        publicACL.Canned = 'public-read';
        objectMD.setReplicationIsReplica(true).setAcl(publicACL).setVersionId('1');
        const result = getMetricToPush(prevObjectMD, objectMD);
        assert.strictEqual(result, 'replicateTags');
    });

    it('should push metrics for replica operations when resetting acl', () => {
        const prevObjectMD = new ObjectMD();
        const publicACL = prevObjectMD.getAcl();
        publicACL.Canned = 'public-read';
        prevObjectMD.setReplicationIsReplica(true).setAcl(publicACL).setVersionId('1');
        const objectMD = new ObjectMD();
        const privateACL = objectMD.getAcl();
        privateACL.Canned = 'private';
        objectMD.setReplicationIsReplica(true).setAcl(privateACL).setVersionId('1');
        const result = getMetricToPush(prevObjectMD, objectMD);
        assert.strictEqual(result, 'replicateTags');
    });

    it('should not push metrics for replica operations with acl ' + 'when they are equal', () => {
        const objectMD = new ObjectMD();
        const publicACL = objectMD.getAcl();
        publicACL.Canned = 'public-read';
        objectMD.setReplicationIsReplica(true).setAcl(publicACL).setVersionId('1');
        const prevObjectMD = new ObjectMD().setAcl(publicACL).setVersionId('1');
        const result = getMetricToPush(prevObjectMD, objectMD);
        assert.strictEqual(result, null);
    });
});
