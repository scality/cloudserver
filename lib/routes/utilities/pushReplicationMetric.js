const { ObjectMD } = require('arsenal').models;
const { pushMetric } = require('../../utapi/utilities');

function shouldPushMetric(prevObjectMD, newObjectMD) {
    // We only want to update metrics for a destination bucket.
    if (newObjectMD.getReplicationStatus() !== 'REPLICA') {
        return false;
    }
    // The rule for a REPLICA is simple: push object metrics if it is
    // a new object version i.e. there is no existing object of the
    // same version, which happens when updating ACLs and/or tags
    // in-place, in which case we don't want to update object metrics.
    return !prevObjectMD;
}

function pushReplicationMetric(prevValue, newValue, bucket, key, log) {
    const prevObjectMD = prevValue ? new ObjectMD(prevValue) : null;
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
