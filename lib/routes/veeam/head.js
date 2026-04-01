const { errors } = require('arsenal');
const { getResponseHeader, buildVeeamFileData } = require('./utils');
const { responseXMLBody, responseContentHeaders } = require('arsenal/build/lib/s3routes/routesUtils');

/**
 * Returns system.xml or capacity.xml files metadata for a given bucket.
 *
 * @param {object} request - request object
 * @param {object} response - response object
 * @param {object} bucketMd - bucket metadata from the db
 * @param {object} log - logger object
 * @returns {undefined} -
 */
function headVeeamFile(request, response, bucketMd, log) {
    if (!bucketMd) {
        return responseXMLBody(errors.NoSuchBucket, null, response, log);
    }

    return buildVeeamFileData(request, bucketMd, log, 'headVeeamFile', (err, result) => {
        if (err) {
            return responseXMLBody(err, null, response, log);
        }

        return responseContentHeaders(
            null,
            {},
            getResponseHeader(request, result.bucketData, result.dataBuffer, result.modified, log),
            response,
            log,
        );
    });
}

module.exports = headVeeamFile;
