const xml2js = require('xml2js');
const { errors } = require('arsenal');
const metadata = require('../../metadata/wrapper');
const { respondWithData, buildHeadXML } = require('./utils');
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
        return respondWithData(request, response, log, request.bucketName,
            buildHeadXML('<Tagging><TagSet></TagSet></Tagging>'));
    }
    return metadata.getBucket(request.bucketName, log, (err, data) => {
        if (err) {
            return responseXMLBody(errors.InternalError, null, response, log);
        }
        let modified = new Date().toISOString();
        const isSystemXML = request.objectKey.endsWith('/system.xml');
        let fileToBuild = null;
        if (isSystemXML) {
            if (!data._capabilities?.VeeamSOSApi?.SystemInfo) {
                return responseXMLBody(errors.NoSuchKey, null, response, log);
            }
            fileToBuild = {
                SystemInfo: data._capabilities?.VeeamSOSApi?.SystemInfo,
            };
            delete fileToBuild.SystemInfo?.LastModified;
        } else {
            if (!data._capabilities?.VeeamSOSApi?.CapacityInfo) {
                return responseXMLBody(errors.NoSuchKey, null, response, log);
            }
            fileToBuild = {
                CapacityInfo: data._capabilities?.VeeamSOSApi?.CapacityInfo,
            };
            delete fileToBuild.CapacityInfo?.LastModified;
        }
        modified = fileToBuild.LastModified;
        delete fileToBuild.LastModified;
        const builder = new xml2js.Builder({
            headless: true,
        });
        return respondWithData(request, response, log, request.bucketName,
            buildHeadXML(builder.buildObject(fileToBuild)), modified);
    });
}

module.exports = getVeeamFile;
