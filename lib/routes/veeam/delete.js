
const { s3routes, errors } = require('arsenal');
const metadata = require('../../metadata/wrapper');
const { responseXMLBody, responseNoBody } = s3routes.routesUtils;

/**
 * Deletes system.xml or capacity.xml files for a given bucket.
 *
 * @param {string} bucketName - bucket name
 * @param {string} objectKey - object key to delete
 * @param {object} bucketMd - bucket metadata from the db
 * @param {object} log - logger object
 * @param {function} callback - callback
 * @returns {undefined} -
 */
function deleteVeeamCapabilities(bucketName, objectKey, bucketMd, log, callback) {
    const isSystemXML = objectKey.endsWith('system.xml');
    const capabilityFieldName = isSystemXML ? 'SystemInfo' : 'CapacityInfo';

    // Ensure file exists in metadata before deletion
    if (!bucketMd._capabilities?.VeeamSOSApi
        || !bucketMd._capabilities?.VeeamSOSApi[capabilityFieldName]) {
        return callback(errors.NoSuchKey);
    }
    // eslint-disable-next-line no-param-reassign
    delete bucketMd._capabilities.VeeamSOSApi[capabilityFieldName];

    // Delete the whole veeam capacity if nothing is left
    if (Object.keys(bucketMd._capabilities.VeeamSOSApi).length === 0) {
        // eslint-disable-next-line no-param-reassign
        delete bucketMd._capabilities.VeeamSOSApi;
        // Delete all capacities if no capacity is left
        if (Object.keys(bucketMd._capabilities).length === 0) {
            // eslint-disable-next-line no-param-reassign
            delete bucketMd._capabilities;
        }
    }

    // Update the bucket metadata
    return metadata.updateBucket(bucketName, bucketMd, log, err => {
        if (err) {
            return callback(err);
        }
        return callback();
    });
}

/**
 * Deletes system.xml or capacity.xml files for a given bucket. handle
 * request context for custom routes.
 *
 * @param {object} request - request object
 * @param {object} response - response object
 * @param {object} bucketMd - bucket metadata from the db
 * @param {object} log - logger object
 * @returns {undefined} -
 */
function deleteVeeamFile(request, response, bucketMd, log) {
    if (!bucketMd) {
        return responseXMLBody(errors.NoSuchBucket, null, response, log);
    }
    return deleteVeeamCapabilities(request.bucketName, request.objectKey, bucketMd, log, err => {
        if (err) {
            return responseXMLBody(err, null, response, log);
        }
        return responseNoBody(null, null, response, 204, log);
    });
}

module.exports = {
    deleteVeeamFile,
    deleteVeeamCapabilities,
};
