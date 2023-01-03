const xml2js = require('xml2js');
const { errors } = require('arsenal');
const metadata = require('../../metadata/wrapper');
const { getResponseHeader, buildHeadXML } = require('./utils');
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
    return metadata.getBucket(request.bucketName, log, (err, data) => {
        if (err) {
            return responseXMLBody(errors.InternalError, null, response, log);
        }
        const isSystemXML = request.objectKey.endsWith('/system.xml');
        const fileToBuild = isSystemXML ? data._capabilities?.VeeamSOSApi?.SystemInfo
            : isSystemXML && !data._capabilities?.VeeamSOSApi?.CapacityInfo;
        if (isSystemXML && !data._capabilities?.VeeamSOSApi?.SystemInfo) {
            return responseXMLBody(errors.NoSuchKey, null, response, log);
        } else if (!isSystemXML && !data._capabilities?.VeeamSOSApi?.CapacityInfo) {
            return responseXMLBody(errors.NoSuchKey, null, response, log);
        }
        const savedModifiedDate = fileToBuild.LastModified;
        delete fileToBuild.LastModified;
        // Recompute file content to generate appropriate content-md5 header
        const builder = new xml2js.Builder({
            headless: true,
        });
        const dataBuffer = Buffer.from(buildHeadXML(builder.buildObject(fileToBuild)));
        return responseContentHeaders(null, {}, getResponseHeader(request, request.bucketName,
            dataBuffer, savedModifiedDate, log), response, log);
    });
}

module.exports = headVeeamFile;
