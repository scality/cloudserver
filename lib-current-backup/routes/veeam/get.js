const xml2js = require('xml2js');
const { errors } = require('arsenal');
const metadata = require('../../metadata/wrapper');
const { respondWithData, buildHeadXML, getFileToBuild } = require('./utils');
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
    return metadata.getBucket(request.bucketName, log, (err, data) => {
        if (err) {
            return responseXMLBody(errors.InternalError, null, response, log);
        }
        const fileToBuild = getFileToBuild(request, data._capabilities?.VeeamSOSApi);
        if (fileToBuild.error) {
            return responseXMLBody(fileToBuild.error, null, response, log);
        }
        let modified = new Date().toISOString();
        // Extract the last modified date, but do not include it when computing
        // the file's ETag (md5)
        modified = fileToBuild.value.LastModified;
        delete fileToBuild.value.LastModified;

        const builder = new xml2js.Builder({
            headless: true,
        });
        return respondWithData(request, response, log, data,
            buildHeadXML(builder.buildObject(fileToBuild.value)), modified);
    });
}

module.exports = getVeeamFile;
