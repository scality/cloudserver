/**
 * checkReadLocation - verify that a bucket's default read location exists
 * for a specified read data locator
 * @param {Config} config - Config object
 * @param {string} locationName - location constraint
 * @param {string} objectKey - object key
 * @param {string} bucketName - bucket name
 * @return {Object | null} return object containing location information
 * if location exists; otherwise, null
 */
function checkReadLocation(config, locationName, objectKey, bucketName) {
    const readLocation = config.getLocationConstraint(locationName);
    if (readLocation) {
        const bucketMatch = readLocation.details &&
            readLocation.details.bucketMatch;
        const backendKey = bucketMatch ? objectKey :
            `${bucketName}/${objectKey}`;
        return {
            location: locationName,
            key: backendKey,
            locationType: readLocation.type,
        };
    }
    return null;
}

module.exports = checkReadLocation;
