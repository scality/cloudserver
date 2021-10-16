const { ObjectMD } = require('arsenal').models;
const { pushMetric } = require('../../utapi/utilities');

function getMetricToPush(prevObjectMD, newObjectMD) {
    // We only want to update metrics for a destination bucket.
    if (newObjectMD.getReplicationStatus() !== 'REPLICA') {
        return null;
    }

    // If the versionIds match then we have a MD only op
    if (prevObjectMD.getVersionId() === newObjectMD.getVersionId()) {
        // Replication of object tags and ACLs should only increment
        // metrics if their value has changed.
        try {
            assert.deepStrictEqual(prevObjectMD.getAcl(), newObjectMD.getAcl());
            assert.deepStrictEqual(
                prevObjectMD.getTags(),
                newObjectMD.getTags()
            );
        } catch (e) {
            return 'replicateTags';
        }
        return null;
    }

    if (newObjectMD.getIsDeleteMarker()) {
        return 'replicateDelete';
    }

    return 'replicateObject';
}

function pushReplicationMetric(prevValue, newValue, bucket, key, log) {
    const prevObjectMD = new ObjectMD(prevValue);
    const newObjectMD = new ObjectMD(newValue);

    const metricType = getMetricToPush(prevObjectMD, newObjectMD);

    if (metricType === null) {
        return undefined;
    }

    const metricObj = {
        bucket,
        keys: [key],
        canonicalID: newObjectMD.getOwnerId(),
    };

    if (metricType === 'replicateObject') {
        metricObj.newByteLength = newObjectMD.getContentLength();
        metricObj.oldByteLength = null;
    }

    return pushMetric(metricType, log, metricObj);
}

module.exports = { pushReplicationMetric, getMetricToPush };
