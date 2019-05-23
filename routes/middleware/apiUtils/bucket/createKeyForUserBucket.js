function createKeyForUserBucket(canonicalID,
    splitter, bucketName) {
    return `${canonicalID}${splitter}${bucketName}`;
}

module.exports = createKeyForUserBucket;
