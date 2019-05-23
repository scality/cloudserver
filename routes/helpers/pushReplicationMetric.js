const assert = require('assert');

const { ObjectMD } = require('arsenal').models;
const { pushMetric } = require('../../utapi/utilities');

function shouldPushMetric(prevObjectMD, newObjectMD) {
    // We only want to update metrics for a destination bucket.
    if (newObjectMD.getReplicationStatus() !== 'REPLICA') {
        return false;
    }
    try {
        // Replication of object tags and ACLs should not increment metrics.
        assert.deepStrictEqual(prevObjectMD.getAcl(), newObjectMD.getAcl());
        assert.deepStrictEqual(prevObjectMD.getTags(), newObjectMD.getTags());
    } catch (e) {
        return false;
    }
    return true;
}

function pushReplicationMetric(prevValue, newValue, bucket, key, log) {
    const prevObjectMD = new ObjectMD(prevValue);
    const newObjectMD = new ObjectMD(newValue);
    if (!shouldPushMetric(prevObjectMD, newObjectMD)) {
        return undefined;
    }
    return pushMetric('putData', log, {
        bucket,
        keys: [key],
        newByteLength: newObjectMD.getContentLength(),
        oldByteLength: null,
    });
}

module.exports = { pushReplicationMetric, shouldPushMetric };
