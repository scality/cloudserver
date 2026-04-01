const { errors } = require('arsenal');
const { respondWithData, buildHeadXML, buildVeeamFileData } = require('./utils');
const { responseXMLBody } = require('arsenal/build/lib/s3routes/routesUtils');

/**
 * Returns system.xml or capacity.xml files for a given bucket.
 *
 * @param {object} request - request object
 * @param {object} response - response object
 * @param {object} bucketMd - bucket metadata from the db
 * @param {object} log - logger object
 * @returns {undefined} -
 */
function getVeeamFile(request, response, bucketMd, log) {
    if (!bucketMd) {
        return responseXMLBody(errors.NoSuchBucket, null, response, log);
    }

    if ('tagging' in request.query) {
        return respondWithData(request, response, log, bucketMd,
            buildHeadXML('<Tagging><TagSet></TagSet></Tagging>'));
    }

    return buildVeeamFileData(request, bucketMd, log, 'getVeeamFile', (err, result) => {
        if (err) {
            return responseXMLBody(err, null, response, log);
        }

        return respondWithData(request, response, log, result.bucketData, result.xmlContent, result.modified);
    });
}

module.exports = getVeeamFile;
