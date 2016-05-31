
export default function createKeyForUserBucket(canonicalID,
    splitter, bucketName) {
    return `${canonicalID}${splitter}${bucketName}`;
}
