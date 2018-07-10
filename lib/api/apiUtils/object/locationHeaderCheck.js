const { errors } = require('arsenal');

const { config } = require('../../../Config');

/**
 * locationHeaderCheck - compares 'x-amz-location-constraint' header
 * to location constraints in config
 * @param {object} headers - request headers
 * @param {string} objectKey - key name of object
 * @param {string} bucketName - name of bucket
 * @return {undefined|Object} returns error, object, or undefined
 * @return {string} return.location - name of location constraint
 * @return {string} return.key - name of object at location constraint
 * @return {string} - return.locationType - type of location constraint
 */
function locationHeaderCheck(headers, objectKey, bucketName) {
    const location = headers['x-amz-location-constraint'];
    if (location) {
        const validLocation = config.locationConstraints[location];
        if (!validLocation) {
            return errors.InvalidLocationConstraint.customizeDescription(
                'Invalid location constraint specified in header');
        }
        const bucketMatch = validLocation.details.bucketMatch;
        const backendKey = bucketMatch ? objectKey :
            `${bucketName}/${objectKey}`;
        return {
            location,
            key: backendKey,
            locationType: validLocation.type,
        };
    }
    // no location header was passed
    return undefined;
}

module.exports = locationHeaderCheck;
