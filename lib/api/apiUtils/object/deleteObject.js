/**
 * _bucketRequiresOplogUpdate - DELETE an object from a bucket
 * @param {BucketInfo} bucket - bucket object
 * @return {boolean} whether objects require oplog updates on deletion, or not
 */
function _bucketRequiresOplogUpdate(bucket) {
    // Default behavior is to require an oplog update
    if (!bucket || !bucket.getLifecycleConfiguration || !bucket.getNotificationConfiguration) {
        return true;
    }
    // If the bucket has lifecycle configuration or notification configuration
    // set, we also require an oplog update
    return bucket.getLifecycleConfiguration() || bucket.getNotificationConfiguration();
}

module.exports = {
    _bucketRequiresOplogUpdate,
};
