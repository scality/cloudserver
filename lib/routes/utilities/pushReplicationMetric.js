const assert = require('assert');

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
        } catch {
            return 'replicateTags';
        }
        return null;
    }

    if (newObjectMD.getIsDeleteMarker()) {
        return 'replicateDelete';
    }

    return 'replicateObject';
}

module.exports = { getMetricToPush };
