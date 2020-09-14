const { ObjectMD } = require('arsenal').models;
const { pushMetric } = require('../../utapi/utilities');

function shouldPushMetric(prevObjectMD, newObjectMD) {
    // We only want to update metrics for a destination bucket.
    if (newObjectMD.getReplicationStatus() !== 'REPLICA') {
        return false;
    }
    // The rule for a REPLICA is: push object metrics if it is a new
    // object version i.e. there is no existing object of the same
    // version. It is the case when updating ACLs and/or tags
    // in-place, in which case we don't want to bump object metrics as
    // the object is still there with the same data.
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
