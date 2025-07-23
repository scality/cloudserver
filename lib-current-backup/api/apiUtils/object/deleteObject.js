/**
 * _bucketRequiresOplogUpdate - DELETE an object from a bucket
 * @param {ObjectMD} objMD - object metadata
 * @param {BucketInfo} bucket - bucket info
 * @return {boolean} whether objects require oplog updates on deletion, or not
 */
function _deleteRequiresOplogUpdate(objMD, bucket) {
    // If the object is archived, an oplog update is required
    if (objMD.archive) {
        return true;
    }
    // Default behavior is to require an oplog update
    if (!bucket || !bucket.getNotificationConfiguration) {
        return true;
    }
    // If the bucket has notification configuration set, we also require an oplog update
    return Boolean(bucket.getNotificationConfiguration());
}

module.exports = {
    _deleteRequiresOplogUpdate,
};
